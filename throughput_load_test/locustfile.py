"""
Unified Locust load test harness for Feature Store performance testing.

Supports two platforms via EXPERIMENT_PARAM_PLATFORM env var:
  - "snowflake": Snowflake Feature Store Postgres (REST query/ingest + SQL)
  - "databricks": Databricks Online Feature Store (REST serving + SQL Warehouse)

Each platform supports two query modes:
  - REST: HTTP POST to the feature store's REST API
  - SQL:  Direct SQL SELECT queries against the online/dynamic table
"""

from locust import User, task, constant_throughput
from dotenv import load_dotenv
import os
import time
import random
import sys
import importlib
import requests
from datetime import datetime, timezone

load_dotenv(override=True)

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0] if sys.argv else "."))

if script_dir and script_dir not in sys.path:
    sys.path.insert(0, script_dir)


def get_wait_time():
    users_multiplier = float(os.getenv("EXPERIMENT_PARAM_USERS_MULTIPLIER"))
    return constant_throughput(users_multiplier)


class LoadTestUser(User):
    """
    Unified Locust User class for testing Feature Store performance.

    Platform behavior is determined by EXPERIMENT_PARAM_PLATFORM:
      - "databricks": Delegates entirely to series.get_query_callback()
      - "snowflake": Uses built-in REST/SQL execution, or series callback if provided

    Snowflake REST mode supports three task types (EXPERIMENT_PARAM_TASK_TYPE):
      - "query":  POST to the Query API endpoint
      - "ingest": POST to the Ingest API endpoint
      - "mixed":  Alternates between query and ingest
    """

    wait_time = get_wait_time()

    def on_start(self):
        self.platform = os.getenv("EXPERIMENT_PARAM_PLATFORM", "snowflake").lower()

        if self.platform == "databricks":
            self._init_databricks()
        else:
            self._init_snowflake()

    def _init_databricks(self):
        """Initialize Databricks mode — delegates to series callback."""
        series_class_name = os.getenv("EXPERIMENT_PARAM_SERIES_CLASS")

        script_dir = os.path.dirname(os.path.abspath(__file__))
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)

        series_module = importlib.import_module("series_databricks")
        series_class = getattr(series_module, series_class_name)
        self._series_instance = series_class()

        params = {}
        for env_key, env_value in os.environ.items():
            if env_key.startswith("EXPERIMENT_PARAM_"):
                param_name = env_key[len("EXPERIMENT_PARAM_"):].lower()
                try:
                    params[param_name] = int(env_value)
                except ValueError:
                    params[param_name] = env_value

        self.query_callback = self._series_instance.get_query_callback(None, params)

        is_sql = isinstance(self._series_instance, series_module.BaseSqlSeries)
        mode = "SQL" if is_sql else "REST API"
        print(f"Databricks user initialized (series={series_class_name}, mode={mode})")

    def _init_snowflake(self):
        """Initialize Snowflake mode — REST/SQL with optional series callback."""
        self.query_mode = os.getenv("EXPERIMENT_PARAM_QUERY_MODE", "REST").upper()
        self.task_type = os.getenv("EXPERIMENT_PARAM_TASK_TYPE", "query")
        self.num_entity_keys = int(os.getenv("EXPERIMENT_PARAM_NUM_ENTITY_KEYS", "1000"))
        self.batch_size = int(os.getenv("EXPERIMENT_PARAM_BATCH_SIZE", "1"))
        self.num_columns = int(os.getenv("EXPERIMENT_PARAM_NUM_COLUMNS", "10"))

        self._build_feature_names()

        if self.query_mode == "SQL":
            self._init_sql_mode()
        else:
            self._init_rest_mode()

        series_class_name = os.getenv("EXPERIMENT_PARAM_SERIES_CLASS")
        if series_class_name:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            if script_dir not in sys.path:
                sys.path.insert(0, script_dir)
            series_module = importlib.import_module("series_snowflake")
            series_class = getattr(series_module, series_class_name)
            series_instance = series_class()

            params = {}
            for env_key, env_value in os.environ.items():
                if env_key.startswith("EXPERIMENT_PARAM_"):
                    param_name = env_key[len("EXPERIMENT_PARAM_"):].lower()
                    try:
                        params[param_name] = int(env_value)
                    except ValueError:
                        params[param_name] = env_value

            if self.query_mode == "SQL":
                series_instance.init_worker_session(self.sf_session)
                self.query_callback = series_instance.get_query_callback(self.sf_session, params)
            else:
                series_instance.init_worker_session(self.http_session)
                self.query_callback = series_instance.get_query_callback(self.http_session, params)
        else:
            self.query_callback = None

        print(
            f"Snowflake user started: mode={self.query_mode}, task_type={self.task_type}, "
            f"batch_size={self.batch_size}, entity_keys={self.num_entity_keys}"
        )

    def _init_rest_mode(self):
        """Initialize HTTP session for Snowflake REST API queries."""
        query_url = os.getenv("QUERY_URL")
        ingest_url = os.getenv("INGEST_URL")
        pat = os.getenv("SNOWFLAKE_PAT")

        if not pat:
            raise ValueError("SNOWFLAKE_PAT environment variable is required")
        if not query_url:
            raise ValueError("QUERY_URL environment variable is required")

        self.query_endpoint = f"{query_url.rstrip('/')}/api/v1/query"
        self.ingest_endpoint = f"{ingest_url.rstrip('/')}/api/v1/ingest" if ingest_url else None

        self.http_session = requests.Session()
        self.http_session.headers.update({
            "Authorization": f'Snowflake Token="{pat}"',
            "Content-Type": "application/json",
        })

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=0,
        )
        self.http_session.mount("https://", adapter)
        self.http_session.mount("http://", adapter)

        self.feature_view_name = os.getenv("EXPERIMENT_PARAM_FEATURE_VIEW_NAME", "load_test_fv").upper()
        self.feature_view_version = os.getenv("EXPERIMENT_PARAM_FEATURE_VIEW_VERSION", "V1").upper()
        self.stream_source_name = os.getenv("EXPERIMENT_PARAM_STREAM_SOURCE_NAME", "load_test_stream").upper()

    def _init_sql_mode(self):
        """Initialize Snowpark session for SQL queries against Dynamic Table."""
        from snowflake.snowpark.session import Session

        warehouse_name = os.getenv("EXPERIMENT_PARAM_WAREHOUSE_NAME")
        connection_params = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
            "host": os.getenv("SNOWFLAKE_HOST"),
            "warehouse": warehouse_name,
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }

        missing = [k for k, v in connection_params.items() if not v]
        if missing:
            raise ValueError(f"Missing Snowflake parameters for SQL mode: {missing}")

        self.sf_session = Session.builder.configs(connection_params).create()
        self.sf_session.sql(f"USE WAREHOUSE {warehouse_name}").collect()

        self.dt_name = os.getenv("EXPERIMENT_PARAM_DT_NAME", "fs_pg_load_test_db_tmp.src.dt")

    def _build_feature_names(self):
        self.feature_names = [f"COL_{i:03d}" for i in range(self.num_columns)]

    def on_stop(self):
        if hasattr(self, "http_session"):
            self.http_session.close()
        if hasattr(self, "sf_session"):
            self.sf_session.close()

    def _build_query_payload(self):
        request_rows = []
        for _ in range(self.batch_size):
            entity_id = random.randint(1, self.num_entity_keys)
            request_rows.append({"entity": {"ID": entity_id}})

        return {
            "name": self.feature_view_name,
            "version": self.feature_view_version,
            "object_type": "feature_view",
            "request_rows": request_rows,
            "features": self.feature_names,
        }

    def _build_ingest_payload(self):
        records = []
        for _ in range(self.batch_size):
            entity_id = random.randint(1, self.num_entity_keys)
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            record = {
                "ID": entity_id,
                "EVENT_TIME": ts,
            }
            for i in range(self.num_columns):
                record[f"COL_{i:03d}"] = round(random.uniform(0, 100), 4)
            records.append(record)

        return {
            "records": {
                self.stream_source_name: records,
            }
        }

    def _execute_rest_query(self):
        payload = self._build_query_payload()
        resp = self.http_session.post(self.query_endpoint, json=payload)
        if resp.status_code >= 400:
            print(f"Query failed: HTTP {resp.status_code} {resp.reason}")
            print(f"  URL: {self.query_endpoint}")
            print(f"  Response body: {resp.text[:500]}")
            resp.raise_for_status()
        return resp

    def _execute_rest_ingest(self):
        if not self.ingest_endpoint:
            raise ValueError("INGEST_URL not configured for ingest task")
        payload = self._build_ingest_payload()
        resp = self.http_session.post(self.ingest_endpoint, json=payload)
        if resp.status_code >= 400:
            print(f"Ingest failed: HTTP {resp.status_code} {resp.reason}")
            print(f"  URL: {self.ingest_endpoint}")
            print(f"  Response body: {resp.text[:500]}")
            resp.raise_for_status()
        return resp

    def _execute_sql_query(self):
        """Execute a SQL SELECT against the Dynamic Table."""
        ids = [random.randint(1, self.num_entity_keys) for _ in range(self.batch_size)]
        column_list = ", ".join(self.feature_names)
        where_clause = " OR ".join([f"ID = {id}" for id in ids])
        query = f"SELECT ID, {column_list} FROM {self.dt_name} WHERE {where_clause}"
        return self.sf_session.sql(query).collect()

    @task
    def run_task(self):
        start_time = time.time()
        task_name = self.task_type if self.platform == "snowflake" else "feature_lookup"

        try:
            if self.platform == "databricks":
                self.query_callback()
                is_sql = hasattr(self, "_series_instance") and \
                    "Sql" in self._series_instance.__class__.__name__
                req_type = "sql_query" if is_sql else "feature_lookup"
                self.environment.events.request.fire(
                    request_type=req_type,
                    name="execute_query" if is_sql else "query_endpoint",
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=1,
                    exception=None,
                )
                return

            # Snowflake path
            if self.query_callback:
                self.query_callback()
                self.environment.events.request.fire(
                    request_type="sql" if self.query_mode == "SQL" else "http",
                    name=f"{task_name}_execute",
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=1,
                    exception=None,
                )
                return

            if self.query_mode == "SQL":
                self._execute_sql_query()
                self.environment.events.request.fire(
                    request_type="sql",
                    name="sql_query",
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=1,
                    exception=None,
                )
            else:
                if self.task_type == "query":
                    resp = self._execute_rest_query()
                elif self.task_type == "ingest":
                    resp = self._execute_rest_ingest()
                elif self.task_type == "mixed":
                    if random.random() < 0.5:
                        resp = self._execute_rest_query()
                        task_name = "mixed_query"
                    else:
                        resp = self._execute_rest_ingest()
                        task_name = "mixed_ingest"
                else:
                    raise ValueError(f"Unknown task_type: {self.task_type}")

                self.environment.events.request.fire(
                    request_type="http",
                    name=task_name,
                    response_time=int((time.time() - start_time) * 1000),
                    response_length=len(resp.content) if resp else 0,
                    exception=None,
                )

        except Exception as e:
            req_type = "sql" if (self.platform == "snowflake" and self.query_mode == "SQL") else "http"
            if self.platform == "databricks":
                is_sql = hasattr(self, "_series_instance") and \
                    "Sql" in self._series_instance.__class__.__name__
                req_type = "sql_query" if is_sql else "feature_lookup"

            self.environment.events.request.fire(
                request_type=req_type,
                name=task_name,
                response_time=int((time.time() - start_time) * 1000),
                response_length=0,
                exception=e,
            )
            raise
