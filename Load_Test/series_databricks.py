"""
Series classes for Databricks Online Feature Store / Feature Serving load tests.

Two query modes are supported:
  1. REST API mode  — HTTP POST to a Feature Serving endpoint (BaseFeatureServingSeries)
  2. SQL mode       — SQL SELECT via Databricks SQL Connector to a SQL Warehouse (BaseSqlSeries)
"""

from series_base import BaseSeries
import random
import os
import string
import time
import urllib3
import requests


FEATURE_TABLE_PREFIX = "fs_load_test"

if os.getenv("DATABRICKS_TLS_NO_VERIFY", "").lower() in ("1", "true", "yes"):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

FIXED_TS = "2024-01-01T00:00:00.000Z"
FIXED_TS_SQL = "TIMESTAMP '2024-01-01 00:00:00'"


def _tls_no_verify():
    return os.getenv("DATABRICKS_TLS_NO_VERIFY", "").lower() in ("1", "true", "yes")


def _dbx_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _feature_table_name(catalog, schema, suffix="features"):
    return f"{catalog}.{schema}.{FEATURE_TABLE_PREFIX}_{suffix}"


def _online_table_name(catalog, schema, suffix="features"):
    override = os.getenv("DATABRICKS_ONLINE_TABLE_NAME", "")
    if override:
        return f"{catalog}.{schema}.{override}" if "." not in override else override
    return f"{catalog}.{schema}.{FEATURE_TABLE_PREFIX}_{suffix}_online"


def _feature_spec_name(catalog, schema, suffix="features"):
    return f"{catalog}.{schema}.{FEATURE_TABLE_PREFIX}_{suffix}_spec"


def _endpoint_name_for_series(series_suffix):
    random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"fs-load-test-{series_suffix}-{random_suffix}"


def column_names(num_columns, include_type=False):
    """Generate column name list, optionally with SQL type annotations."""
    cols = []
    for i in range(num_columns):
        if include_type:
            cols.append(f"col_{i:03d} DOUBLE")
        else:
            cols.append(f"col_{i:03d}")
    return ", ".join(cols)


def create_feature_table_sql(catalog, schema, num_columns, num_table_rows):
    """Returns SQL statements to create and populate the offline feature table."""
    table_name = _feature_table_name(catalog, schema)
    col_defs = column_names(num_columns, include_type=True)
    col_values = ", ".join(
        [f"CAST(rand() * 100 AS DOUBLE)" for _ in range(num_columns)]
    )

    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}",
        f"""CREATE OR REPLACE TABLE {table_name} (
            id INT NOT NULL,
            ts TIMESTAMP,
            {col_defs},
            CONSTRAINT pk PRIMARY KEY (id, ts)
        ) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""",
        f"ALTER TABLE {table_name} ALTER COLUMN id SET NOT NULL",
        f"""INSERT INTO {table_name}
        SELECT
            explode(sequence(0, {num_table_rows - 1})) as id,
            {FIXED_TS_SQL} as ts,
            {col_values}
        """,
    ]
    return statements


def create_online_store(client_config, capacity="CU_2"):
    """Create a Databricks Online Feature Store (Lakebase instance)."""
    from databricks.feature_engineering import FeatureEngineeringClient

    fe = FeatureEngineeringClient()
    online_store_name = client_config["online_store_name"]

    try:
        existing = fe.get_online_store(name=online_store_name)
        if existing and existing.state == "AVAILABLE":
            print(f"Online store '{online_store_name}' already exists (state={existing.state})")
            return existing
    except Exception:
        pass

    print(f"Creating online store '{online_store_name}' with capacity={capacity}")
    fe.create_online_store(name=online_store_name, capacity=capacity)

    for _ in range(60):
        time.sleep(10)
        store = fe.get_online_store(name=online_store_name)
        if store and store.state == "AVAILABLE":
            print(f"Online store '{online_store_name}' is AVAILABLE")
            return store
        print(f"  Waiting for online store... state={store.state if store else 'unknown'}")

    raise TimeoutError(f"Online store '{online_store_name}' did not become AVAILABLE within 10 minutes")


def publish_feature_table(client_config, publish_mode="TRIGGERED"):
    """Publish an offline feature table to the online store."""
    from databricks.feature_engineering import FeatureEngineeringClient

    fe = FeatureEngineeringClient()
    catalog = client_config["catalog"]
    schema = client_config["schema"]

    online_store = fe.get_online_store(name=client_config["online_store_name"])
    source_table = _feature_table_name(catalog, schema)
    online_table = _online_table_name(catalog, schema)

    print(f"Publishing {source_table} -> {online_table} (mode={publish_mode})")
    fe.publish_table(
        online_store=online_store,
        source_table_name=source_table,
        online_table_name=online_table,
        publish_mode=publish_mode,
    )
    print(f"Publish initiated for {online_table}")


def create_feature_spec(client_config, num_columns):
    """Create a FeatureSpec in Unity Catalog."""
    from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

    fe = FeatureEngineeringClient()
    catalog = client_config["catalog"]
    schema = client_config["schema"]

    feature_names = [f"col_{i:03d}" for i in range(num_columns)]
    spec_name = _feature_spec_name(catalog, schema)
    table_name = _feature_table_name(catalog, schema)

    features = [
        FeatureLookup(
            table_name=table_name,
            lookup_key=["id", "ts"],
            feature_names=feature_names,
        )
    ]

    try:
        fe.create_feature_spec(name=spec_name, features=features)
        print(f"Created FeatureSpec: {spec_name}")
    except Exception as e:
        if "already exists" in str(e):
            print(f"FeatureSpec {spec_name} already exists — skipping creation")
        else:
            raise


def create_serving_endpoint(client_config, workload_size="Small"):
    """Create a Feature Serving endpoint backed by the FeatureSpec."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

    host = client_config["host"]
    token = client_config["token"]
    catalog = client_config["catalog"]
    schema = client_config["schema"]
    endpoint_name = client_config["endpoint_name"]
    spec_name = _feature_spec_name(catalog, schema)

    w = WorkspaceClient(host=host, token=token)

    try:
        existing = w.serving_endpoints.get(name=endpoint_name)
        if existing:
            print(f"Endpoint '{endpoint_name}' already exists — updating config")
            w.serving_endpoints.update_config(
                name=endpoint_name,
                served_entities=[
                    ServedEntityInput(
                        entity_name=spec_name,
                        scale_to_zero_enabled=False,
                        workload_size=workload_size,
                    )
                ],
            )
            _wait_for_endpoint_ready(w, endpoint_name)
            return
    except Exception:
        pass

    print(f"Creating serving endpoint '{endpoint_name}' (workload_size={workload_size})")
    w.serving_endpoints.create_and_wait(
        name=endpoint_name,
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=spec_name,
                    scale_to_zero_enabled=False,
                    workload_size=workload_size,
                )
            ]
        ),
    )
    print(f"Serving endpoint '{endpoint_name}' is ready")


def _wait_for_endpoint_ready(workspace_client, endpoint_name, timeout_minutes=30):
    """Poll until a serving endpoint reaches READY state."""
    for _ in range(timeout_minutes * 6):
        time.sleep(10)
        ep = workspace_client.serving_endpoints.get(name=endpoint_name)
        if ep.state and ep.state.ready == "READY":
            print(f"Endpoint '{endpoint_name}' is READY")
            return
        print(f"  Waiting for endpoint... state={ep.state}")
    raise TimeoutError(f"Endpoint '{endpoint_name}' did not become READY within {timeout_minutes} minutes")


def delete_serving_endpoint(client_config):
    """Delete a serving endpoint."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(host=client_config["host"], token=client_config["token"])
    endpoint_name = client_config["endpoint_name"]
    try:
        w.serving_endpoints.delete(name=endpoint_name)
        print(f"Deleted serving endpoint '{endpoint_name}'")
    except Exception as e:
        print(f"Error deleting endpoint '{endpoint_name}': {e}")


def delete_online_table(client_config):
    """Delete an online table from the online store."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(host=client_config["host"], token=client_config["token"])
    catalog = client_config["catalog"]
    schema = client_config["schema"]
    online_table = _online_table_name(catalog, schema)
    try:
        w.online_tables.delete(name=online_table)
        print(f"Deleted online table '{online_table}'")
    except Exception as e:
        print(f"Error deleting online table '{online_table}': {e}")


def drop_feature_table_sql(catalog, schema):
    """Return SQL to drop the offline feature table."""
    table_name = _feature_table_name(catalog, schema)
    return f"DROP TABLE IF EXISTS {table_name}"


def _normalize_host(host):
    """Extract the base workspace URL."""
    from urllib.parse import urlparse
    parsed = urlparse(host.strip())
    scheme = parsed.scheme or "https"
    hostname = parsed.hostname or parsed.path.split("/")[0]
    port = f":{parsed.port}" if parsed.port else ""
    return f"{scheme}://{hostname}{port}"


def _build_query_url(host, endpoint_name):
    base = _normalize_host(host)
    return f"{base}/serving-endpoints/{endpoint_name}/invocations"


# ---------------------------------------------------------------------------
# REST API series classes
# ---------------------------------------------------------------------------


class BaseFeatureServingSeries(BaseSeries):
    """
    Base class for Databricks Feature Serving load test series (REST mode).

    Queries are HTTP POST requests to the serving endpoint, sending
    entity IDs as dataframe_records and receiving feature values.
    """

    def get_query_callback(self, session_or_config, params):
        host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
        token = os.getenv("DATABRICKS_TOKEN", "")
        endpoint_name = os.getenv("EXPERIMENT_PARAM_ENDPOINT_NAME", "")

        query_url = _build_query_url(host, endpoint_name)
        headers = _dbx_headers(token)
        num_table_rows = params.get("num_table_rows", 1000)
        batch_size = params.get("batch_size", 1)

        session = requests.Session()
        session.headers.update(headers)
        if _tls_no_verify():
            session.verify = False

        def execute_query():
            records = [
                {"id": random.randint(0, num_table_rows - 1), "ts": FIXED_TS}
                for _ in range(batch_size)
            ]
            payload = {"dataframe_records": records}
            resp = session.post(query_url, json=payload, timeout=30)
            resp.raise_for_status()
            return resp.json()

        for _ in range(5):
            try:
                execute_query()
            except Exception:
                pass

        return execute_query


class QpsSeries(BaseFeatureServingSeries):
    """REST API QPS scaling series. Assumes infrastructure is pre-provisioned."""

    can_skip_re_warmup = True

    def setup_series(self, session_or_config, params):
        endpoint_name = session_or_config.get("endpoint_name", "")
        if not endpoint_name:
            raise ValueError(
                "DATABRICKS_SERVING_ENDPOINT must be set in .env. "
                "Run the setup notebook first to create the infrastructure."
            )
        os.environ["EXPERIMENT_PARAM_ENDPOINT_NAME"] = endpoint_name
        print(f"Using pre-provisioned serving endpoint: {endpoint_name}")

    def teardown_series(self, session_or_config, params):
        pass


class CapacitySeries(BaseFeatureServingSeries):
    """Online store capacity scaling series via REST API."""

    can_skip_re_warmup = False

    def setup_series(self, session_or_config, params):
        endpoint_name = session_or_config.get("endpoint_name", "")
        if not endpoint_name:
            raise ValueError(
                "DATABRICKS_SERVING_ENDPOINT must be set in .env. "
                "CapacitySeries requires infrastructure changes between experiments."
            )
        os.environ["EXPERIMENT_PARAM_ENDPOINT_NAME"] = endpoint_name
        print(f"Using pre-provisioned serving endpoint: {endpoint_name}")
        print(
            "WARNING: CapacitySeries cannot change online store capacity from a local machine. "
            "All experiments will run against the same pre-provisioned capacity."
        )

    def teardown_series(self, session_or_config, params):
        pass


class EndpointSizeSeries(BaseFeatureServingSeries):
    """Endpoint workload size scaling series via REST API."""

    can_skip_re_warmup = False

    def setup_series(self, session_or_config, params):
        endpoint_name = session_or_config.get("endpoint_name", "")
        if not endpoint_name:
            raise ValueError(
                "DATABRICKS_SERVING_ENDPOINT must be set in .env. "
                "EndpointSizeSeries requires infrastructure changes between experiments."
            )
        os.environ["EXPERIMENT_PARAM_ENDPOINT_NAME"] = endpoint_name
        print(f"Using pre-provisioned serving endpoint: {endpoint_name}")
        print(
            "WARNING: EndpointSizeSeries cannot change endpoint workload_size from a local machine. "
            "All experiments will run against the same pre-provisioned workload size."
        )

    def teardown_series(self, session_or_config, params):
        pass


class TableWidthSeries(BaseFeatureServingSeries):
    """Table width scaling series via REST API."""

    can_skip_re_warmup = False

    def setup_series(self, session_or_config, params):
        endpoint_name = session_or_config.get("endpoint_name", "")
        if not endpoint_name:
            raise ValueError(
                "DATABRICKS_SERVING_ENDPOINT must be set in .env. "
                "Run the setup notebook first to create the infrastructure."
            )
        os.environ["EXPERIMENT_PARAM_ENDPOINT_NAME"] = endpoint_name
        print(f"Using pre-provisioned serving endpoint: {endpoint_name}")

    def teardown_series(self, session_or_config, params):
        pass


class BatchSizeSeries(QpsSeries):
    """Batch size scaling series via REST API."""

    can_skip_re_warmup = True


# ---------------------------------------------------------------------------
# SQL-based series classes
# ---------------------------------------------------------------------------


class BaseSqlSeries(BaseSeries):
    """
    Base class for SQL-based load test series.

    Each Locust worker opens a connection to a Databricks SQL Warehouse
    and executes SELECT queries against the online feature table.
    """

    def get_query_callback(self, session_or_config, params):
        from databricks import sql as dbx_sql

        host = os.getenv("DATABRICKS_HOST", "").rstrip("/").replace("https://", "")
        token = os.getenv("DATABRICKS_TOKEN", "")
        http_path = os.getenv("DATABRICKS_SQL_WAREHOUSE_HTTP_PATH", "")
        catalog = os.getenv("DATABRICKS_CATALOG", "")
        schema = os.getenv("DATABRICKS_SCHEMA", "")

        if not http_path:
            raise ValueError(
                "DATABRICKS_SQL_WAREHOUSE_HTTP_PATH must be set in .env for SQL-based series"
            )

        online_table = _online_table_name(catalog, schema)
        num_table_rows = params.get("num_table_rows", 1000)
        batch_size = params.get("batch_size", 1)
        num_columns = params.get("num_columns", 10)
        col_list = column_names(num_columns)

        connect_kwargs = dict(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
        )
        if _tls_no_verify():
            connect_kwargs["_tls_no_verify"] = True
        connection = dbx_sql.connect(**connect_kwargs)
        cursor = connection.cursor()

        def execute_query():
            ids = [random.randint(0, num_table_rows - 1) for _ in range(batch_size)]
            where_clause = " OR ".join(
                [f"(id = {id_val} AND ts = {FIXED_TS_SQL})" for id_val in ids]
            )
            query = f"SELECT id, {col_list} FROM {online_table} WHERE {where_clause}"
            cursor.execute(query)
            return cursor.fetchall()

        for _ in range(10):
            try:
                execute_query()
            except Exception:
                pass

        return execute_query


class SqlQpsSeries(BaseSqlSeries):
    """SQL-based QPS scaling series."""

    can_skip_re_warmup = True

    def setup_series(self, session_or_config, params):
        http_path = os.getenv("DATABRICKS_SQL_WAREHOUSE_HTTP_PATH", "")
        if not http_path:
            raise ValueError(
                "DATABRICKS_SQL_WAREHOUSE_HTTP_PATH must be set in .env for SQL series."
            )
        print(f"Using pre-provisioned SQL Warehouse: {http_path}")

    def teardown_series(self, session_or_config, params):
        pass


class SqlCapacitySeries(BaseSqlSeries):
    """SQL-based online store capacity scaling series."""

    can_skip_re_warmup = False

    def setup_series(self, session_or_config, params):
        http_path = os.getenv("DATABRICKS_SQL_WAREHOUSE_HTTP_PATH", "")
        if not http_path:
            raise ValueError(
                "DATABRICKS_SQL_WAREHOUSE_HTTP_PATH must be set in .env for SQL series."
            )
        print(f"Using pre-provisioned SQL Warehouse: {http_path}")
        print(
            "WARNING: SqlCapacitySeries cannot change online store capacity from a local machine."
        )

    def teardown_series(self, session_or_config, params):
        pass


class SqlTableWidthSeries(BaseSqlSeries):
    """SQL-based table width scaling series."""

    can_skip_re_warmup = False

    def setup_series(self, session_or_config, params):
        http_path = os.getenv("DATABRICKS_SQL_WAREHOUSE_HTTP_PATH", "")
        if not http_path:
            raise ValueError(
                "DATABRICKS_SQL_WAREHOUSE_HTTP_PATH must be set in .env for SQL series."
            )
        print(f"Using pre-provisioned SQL Warehouse: {http_path}")

    def teardown_series(self, session_or_config, params):
        pass


class SqlBatchSizeSeries(SqlQpsSeries):
    """SQL-based batch size scaling series."""

    can_skip_re_warmup = True
