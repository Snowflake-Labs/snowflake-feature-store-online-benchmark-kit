"""
Series classes for Snowflake Feature Store Postgres load test experiments.

Each series class manages the Feature Store lifecycle (online service, entities,
feature views) and provides HTTP-based query/ingest callbacks for Locust workers.
"""

from series_base import BaseSeries
import random
import os
import time
import string
import requests
from datetime import datetime, timezone

from snowflake.ml.feature_store import (
    FeatureStore,
    FeatureView,
    Entity,
    Feature,
    CreationMode,
    OnlineConfig,
    OnlineStoreType,
    StreamSource,
    StreamConfig,
    online_service,
)
from snowflake.snowpark.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DoubleType,
    TimestampType,
    TimestampTimeZone,
)

DATABASE_NAME = "fs_pg_load_test_db_tmp"
SOURCE_SCHEMA = "src"
FS_SCHEMA = "feature_store"
PRODUCER_ROLE = "FS_LOAD_TEST_PRODUCER"
CONSUMER_ROLE = "FS_LOAD_TEST_CONSUMER"


class BaseFSSeries(BaseSeries):
    """
    Base class for Feature Store Postgres series.

    Handles the full lifecycle:
      - Create database, schemas, warehouse
      - Initialize FeatureStore with Postgres online service (REST mode)
      - OR create Dynamic Table for direct SQL queries (SQL mode)
      - Register entity and populate source data
      - Create batch FeatureView with OnlineStoreType.POSTGRES
      - Extract query/ingest URLs and set them as env vars
    """

    can_skip_re_warmup = True

    def setup_series(self, session_or_config, params):
        query_mode = params.get("query_mode", "REST").upper()
        self._query_mode = query_mode

        self._setup_infrastructure(session_or_config, params)

        if query_mode == "SQL":
            self._populate_source_data(session_or_config, params)
            self._create_dynamic_table(session_or_config, params)
            self._wait_for_dynamic_table(session_or_config, params)
            self._setup_ingestion_task(session_or_config, params)
            os.environ["EXPERIMENT_PARAM_WAREHOUSE_NAME"] = self._warehouse_name
        else:
            self._init_feature_store(session_or_config, params)
            self._create_online_service(session_or_config, params)
            self._register_entity(session_or_config, params)
            self._populate_source_data(session_or_config, params)
            self._create_feature_view(session_or_config, params)
            self._wait_for_online_data(session_or_config, params)

    def teardown_series(self, session_or_config, params):
        query_mode = getattr(self, "_query_mode", "REST")

        if query_mode == "SQL":
            self._drop_ingestion_task(session_or_config, params)
            try:
                session_or_config.sql(
                    f"DROP DYNAMIC TABLE IF EXISTS {DATABASE_NAME}.{SOURCE_SCHEMA}.dt"
                ).collect()
                print("Dropped dynamic table")
            except Exception as e:
                print(f"Error dropping dynamic table: {e}")
            try:
                session_or_config.sql(
                    f"DROP TABLE IF EXISTS {DATABASE_NAME}.{SOURCE_SCHEMA}.source_data"
                ).collect()
                print("Cleaned up source data")
            except Exception as e:
                print(f"Error cleaning up source data: {e}")
        else:
            try:
                if hasattr(self, "_fs"):
                    try:
                        fv = self._fs.get_feature_view(
                            self._feature_view_name, self._feature_view_version
                        )
                        self._fs.delete_feature_view(fv)
                        print(f"Deleted feature view {self._feature_view_name}/{self._feature_view_version}")
                    except Exception as e:
                        print(f"Could not delete feature view: {e}")

                    try:
                        self._fs.delete_entity("LOAD_TEST_USER")
                    except Exception:
                        pass

            except Exception as e:
                print(f"Error during feature store teardown: {e}")

            try:
                session_or_config.sql(f"DROP TABLE IF EXISTS {DATABASE_NAME}.{SOURCE_SCHEMA}.source_data").collect()
                print("Cleaned up source data")
            except Exception as e:
                print(f"Error cleaning up source data: {e}")

        try:
            if hasattr(self, "_warehouse_name"):
                session_or_config.sql(f"DROP WAREHOUSE IF EXISTS {self._warehouse_name}").collect()
                print(f"Dropped warehouse {self._warehouse_name}")
        except Exception as e:
            print(f"Error dropping warehouse: {e}")

    def get_query_callback(self, session_or_config, params):
        """Default: no custom callback, locustfile uses built-in query/ingest."""
        return None

    def _create_dynamic_table(self, session, params):
        """Create a Dynamic Table from source_data for SQL-mode queries."""
        num_columns = params.get("num_columns", 10)
        col_list = ", ".join(["ID", "TS"] + [f"COL_{i:03d}" for i in range(num_columns)])

        session.sql(f"USE SCHEMA {DATABASE_NAME}.{SOURCE_SCHEMA}").collect()
        session.sql(
            f"""
            CREATE OR REPLACE DYNAMIC TABLE dt
                TARGET_LAG = '1 minute'
                REFRESH_MODE = 'incremental'
                WAREHOUSE = {self._warehouse_name}
            AS SELECT {col_list} FROM source_data
        """
        ).collect()
        session.sql("ALTER DYNAMIC TABLE dt REFRESH").collect()
        print("Created and refreshed dynamic table dt")

        os.environ["EXPERIMENT_PARAM_DT_NAME"] = f"{DATABASE_NAME}.{SOURCE_SCHEMA}.dt"
        num_entity_keys = params.get("num_entity_keys", params.get("num_table_rows", 1000))
        os.environ["EXPERIMENT_PARAM_NUM_ENTITY_KEYS"] = str(num_entity_keys)

    def _wait_for_dynamic_table(self, session, params):
        """Wait for the Dynamic Table to have data."""
        print("Waiting for dynamic table to populate...")
        for attempt in range(30):
            try:
                count_row = session.sql(
                    f"SELECT COUNT(*) AS cnt FROM {DATABASE_NAME}.{SOURCE_SCHEMA}.dt"
                ).collect()
                count = count_row[0]["CNT"]
                if count > 0:
                    print(f"  Dynamic table ready ({count} rows)")
                    return
                print(f"  [{attempt}] Dynamic table empty, waiting...")
            except Exception as e:
                print(f"  [{attempt}] DT check error: {e}")
            time.sleep(10)
        print("WARNING: Dynamic table did not populate within timeout")

    def _setup_ingestion_task(self, session, params):
        """Create a scheduled task for background ingestion (SQL mode)."""
        ingest_keys_per_minute = params.get("ingest_keys_per_minute", 0)
        if ingest_keys_per_minute <= 0:
            print("No ingestion task configured (ingest_keys_per_minute=0)")
            return

        num_columns = params.get("num_columns", 10)
        num_table_rows = params.get("num_table_rows", 1000)
        col_values = ", ".join(
            [f"uniform(0::float, 100::float, random())" for _ in range(num_columns)]
        )
        rows_per_execution = max(1, ingest_keys_per_minute // 6)

        session.sql(f"USE SCHEMA {DATABASE_NAME}.{SOURCE_SCHEMA}").collect()
        session.sql(
            f"""
            CREATE OR REPLACE TASK data_population_task
                WAREHOUSE = {self._warehouse_name}
                SCHEDULE = '10 seconds'
            AS
            INSERT INTO {DATABASE_NAME}.{SOURCE_SCHEMA}.source_data
            SELECT
                uniform(1::int, {num_table_rows}::int, random()) AS ID,
                current_timestamp() AS TS,
                {col_values}
            FROM TABLE(generator(rowcount => {rows_per_execution}))
        """
        ).collect()
        session.sql("ALTER TASK data_population_task RESUME").collect()
        print(f"Created ingestion task: {ingest_keys_per_minute} keys/min ({rows_per_execution} rows every 10s)")

    def _drop_ingestion_task(self, session, params):
        """Stop and drop the ingestion task."""
        try:
            session.sql(f"USE SCHEMA {DATABASE_NAME}.{SOURCE_SCHEMA}").collect()
            session.sql(
                "SELECT SYSTEM$USER_TASK_CANCEL_ONGOING_EXECUTIONS('data_population_task')"
            ).collect()
        except Exception:
            pass
        try:
            session.sql("DROP TASK IF EXISTS data_population_task").collect()
            print("Dropped ingestion task")
        except Exception as e:
            print(f"Error dropping ingestion task: {e}")

    def _setup_infrastructure(self, session, params):
        random_suffix = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=8)
        )
        self._warehouse_name = f"FS_PG_LOAD_TEST_WH_{random_suffix}"

        session.sql("USE ROLE ACCOUNTADMIN").collect()
        print("Using role ACCOUNTADMIN")

        session.sql(
            f"""
            CREATE OR REPLACE WAREHOUSE {self._warehouse_name}
            WITH WAREHOUSE_SIZE = 'X-SMALL'
                 AUTO_SUSPEND = 60
                 MIN_CLUSTER_COUNT = 1
                 MAX_CLUSTER_COUNT = 1
        """
        ).collect()
        print(f"Created warehouse {self._warehouse_name}")

        session.sql(f"USE WAREHOUSE {self._warehouse_name}").collect()

        session.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}").collect()
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SOURCE_SCHEMA}").collect()
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{FS_SCHEMA}").collect()
        session.sql(f"USE DATABASE {DATABASE_NAME}").collect()
        session.sql(f"USE SCHEMA {DATABASE_NAME}.{FS_SCHEMA}").collect()

        session.sql(f"DROP TABLE IF EXISTS {DATABASE_NAME}.{SOURCE_SCHEMA}.source_data").collect()

        for role in [PRODUCER_ROLE, CONSUMER_ROLE]:
            session.sql(f"CREATE ROLE IF NOT EXISTS {role}").collect()
            session.sql(f"GRANT ROLE {role} TO ROLE ACCOUNTADMIN").collect()

        print(f"Infrastructure ready: {DATABASE_NAME}")

    def _init_feature_store(self, session, params):
        self._fs = FeatureStore(
            session=session,
            database=DATABASE_NAME,
            name=FS_SCHEMA,
            default_warehouse=self._warehouse_name,
            creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
        )
        print("Feature Store initialized")

    def _create_online_service(self, session, params):
        try:
            create_result = self._fs.create_online_service(PRODUCER_ROLE, CONSUMER_ROLE)
            print(f"Online service create result: {create_result}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print("Online service already exists, reusing")
            else:
                print(f"Online service creation failed: {e}")
                print("Attempting nuclear cleanup and retry...")
                self._nuclear_cleanup_and_retry(session, params)
                return

        self._wait_for_online_service_running(session)

    def _wait_for_online_service_running(self, session):
        print("Waiting for online service to reach RUNNING...")
        for i in range(60):
            status = self._fs.get_online_service_status()
            ep_names = [ep.name for ep in status.endpoints]
            print(f"  [{i}] Status: {status.status} | Endpoints: {ep_names}")
            if status.status == "RUNNING" and online_service.endpoint_url(status, "query"):
                query_url = online_service.endpoint_url(status, "query")
                ingest_url = online_service.endpoint_url(status, "ingest")
                print(f"Online service RUNNING")
                print(f"  Query URL: {query_url}")
                print(f"  Ingest URL: {ingest_url}")

                os.environ["QUERY_URL"] = query_url
                os.environ["INGEST_URL"] = ingest_url
                return
            time.sleep(30)

        raise RuntimeError("Online service did not reach RUNNING within 30 minutes")

    def _nuclear_cleanup_and_retry(self, session, params):
        """
        Last-resort recovery: drop the online service, drop the entire
        database, recreate everything, and retry create_online_service once.
        """
        session.sql("USE ROLE ACCOUNTADMIN").collect()

        try:
            self._fs.drop_online_service()
            print("  SDK drop_online_service succeeded")
        except Exception as e:
            print(f"  SDK drop_online_service failed: {e}")

        try:
            session.sql(f"DROP DATABASE IF EXISTS {DATABASE_NAME}").collect()
            print(f"  Dropped database {DATABASE_NAME}")
        except Exception as e:
            print(f"  Could not drop database: {e}")

        time.sleep(5)

        session.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}").collect()
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SOURCE_SCHEMA}").collect()
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{FS_SCHEMA}").collect()
        session.sql(f"USE DATABASE {DATABASE_NAME}").collect()
        session.sql(f"USE SCHEMA {DATABASE_NAME}.{FS_SCHEMA}").collect()
        session.sql(f"USE WAREHOUSE {self._warehouse_name}").collect()
        print(f"  Recreated database {DATABASE_NAME}")

        self._fs = FeatureStore(
            session=session,
            database=DATABASE_NAME,
            name=FS_SCHEMA,
            default_warehouse=self._warehouse_name,
            creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
        )
        print("  Feature Store reinitialized")

        create_result = self._fs.create_online_service(PRODUCER_ROLE, CONSUMER_ROLE)
        print(f"  Online service create result (retry): {create_result}")

        self._wait_for_online_service_running(session)

    def _register_entity(self, session, params):
        entity = Entity(
            name="LOAD_TEST_USER",
            join_keys=["ID"],
            desc="Load test entity keyed by integer ID",
        )
        self._fs.register_entity(entity)
        self._entity = entity
        print("Entity LOAD_TEST_USER registered")

    def _populate_source_data(self, session, params):
        num_columns = params.get("num_columns", 10)
        num_table_rows = params.get("num_table_rows", 1000)

        col_defs = ", ".join(
            [f"COL_{i:03d} FLOAT" for i in range(num_columns)]
        )
        col_values = ", ".join(
            [f"uniform(0::float, 100::float, random())" for _ in range(num_columns)]
        )

        session.sql(f"USE SCHEMA {DATABASE_NAME}.{SOURCE_SCHEMA}").collect()
        session.sql(
            f"CREATE OR REPLACE TABLE source_data (ID INT, TS TIMESTAMP_NTZ, {col_defs})"
        ).collect()
        session.sql(
            f"""
            INSERT INTO source_data
            SELECT
                seq4() + 1 AS ID,
                current_timestamp() AS TS,
                {col_values}
            FROM TABLE(generator(rowcount => {num_table_rows}))
        """
        ).collect()
        print(f"Populated source_data: {num_table_rows} rows, {num_columns} columns")

    def _create_feature_view(self, session, params):
        self._feature_view_name = params.get("feature_view_name", "load_test_fv")
        self._feature_view_version = params.get("feature_view_version", "V1")

        os.environ["EXPERIMENT_PARAM_FEATURE_VIEW_NAME"] = self._feature_view_name
        os.environ["EXPERIMENT_PARAM_FEATURE_VIEW_VERSION"] = self._feature_view_version

        source_df = session.table(f"{DATABASE_NAME}.{SOURCE_SCHEMA}.source_data")

        fv = FeatureView(
            name=self._feature_view_name,
            entities=[self._entity],
            feature_df=source_df,
            timestamp_col="TS",
            refresh_freq="1 minute",
            online_config=OnlineConfig(
                enable=True,
                target_lag="10s",
                store_type=OnlineStoreType.POSTGRES,
            ),
            desc="Load test batch feature view with Postgres online store",
        )

        self._registered_fv = self._fs.register_feature_view(
            fv, self._feature_view_version, overwrite=True
        )
        print(
            f"Registered feature view: {self._feature_view_name}/{self._feature_view_version}"
        )

    def _wait_for_online_data(self, session, params):
        """Wait for offline backfill and online materialization to complete."""
        print("Waiting for offline backfill...")
        for attempt in range(40):
            try:
                count = self._fs.read_feature_view(
                    self._registered_fv, store_type="offline"
                ).count()
                print(f"  Offline rows: {count}")
                if count > 0:
                    print("Offline backfill complete")
                    break
            except Exception as e:
                print(f"  Backfill check error: {e}")
            time.sleep(15)
        else:
            print("WARNING: Backfill did not complete within timeout")

        print("Waiting for online materialization...")
        for attempt in range(60):
            try:
                online_df = self._fs.read_feature_view(
                    self._registered_fv,
                    keys=[[1]],
                    store_type="online",
                )
                online_count = online_df.count()
                if online_count > 0:
                    print(f"  Online store ready (got {online_count} rows for key=1)")
                    break
                print(f"  [{attempt}] Online store not ready yet (0 rows)")
            except Exception as e:
                err = str(e)
                if "not found" in err.lower():
                    print(f"  [{attempt}] Feature view not yet in online store")
                else:
                    print(f"  [{attempt}] Online check error: {e}")
            time.sleep(15)
        else:
            print("WARNING: Online materialization did not complete within 15 minutes")

        num_entity_keys = params.get("num_entity_keys", params.get("num_table_rows", 1000))
        os.environ["EXPERIMENT_PARAM_NUM_ENTITY_KEYS"] = str(num_entity_keys)


class QueryQpsSeries(BaseFSSeries):
    """QPS scaling series for the Query API."""

    can_skip_re_warmup = True


class QueryBatchSizeSeries(BaseFSSeries):
    """Batch size scaling series for the Query API."""

    can_skip_re_warmup = True


class QueryFeatureWidthSeries(BaseFSSeries):
    """
    Feature width scaling series for the Query API.
    Requires recreating the source data and feature view per experiment.
    """

    can_skip_re_warmup = False

    def setup_series(self, session_or_config, params):
        query_mode = params.get("query_mode", "REST").upper()
        self._query_mode = query_mode

        self._setup_infrastructure(session_or_config, params)

        if query_mode == "SQL":
            os.environ["EXPERIMENT_PARAM_WAREHOUSE_NAME"] = self._warehouse_name
        else:
            self._init_feature_store(session_or_config, params)
            self._create_online_service(session_or_config, params)
            self._register_entity(session_or_config, params)

    def setup_experiment(self, session_or_config, params):
        self._populate_source_data(session_or_config, params)
        if self._query_mode == "SQL":
            self._create_dynamic_table(session_or_config, params)
            self._wait_for_dynamic_table(session_or_config, params)
        else:
            self._create_feature_view(session_or_config, params)
            self._wait_for_online_data(session_or_config, params)

    def teardown_experiment(self, session_or_config, params):
        if self._query_mode == "SQL":
            try:
                session_or_config.sql(
                    f"DROP DYNAMIC TABLE IF EXISTS {DATABASE_NAME}.{SOURCE_SCHEMA}.dt"
                ).collect()
                print("Dropped dynamic table for experiment")
            except Exception as e:
                print(f"Error dropping dynamic table: {e}")
        else:
            try:
                if hasattr(self, "_registered_fv"):
                    self._fs.delete_feature_view(self._registered_fv)
                    print(f"Deleted feature view for experiment")
            except Exception as e:
                print(f"Error deleting feature view: {e}")


class IngestQpsSeries(BaseFSSeries):
    """
    QPS scaling series for the Ingest API.

    REST mode: Creates a StreamSource and stream feature view.
    SQL mode: Uses a scheduled Task for background ingestion.
    """

    can_skip_re_warmup = True

    def setup_series(self, session_or_config, params):
        query_mode = params.get("query_mode", "REST").upper()
        self._query_mode = query_mode

        if query_mode == "SQL":
            self._setup_infrastructure(session_or_config, params)
            self._populate_source_data(session_or_config, params)
            self._create_dynamic_table(session_or_config, params)
            self._wait_for_dynamic_table(session_or_config, params)
            self._setup_ingestion_task(session_or_config, params)
            os.environ["EXPERIMENT_PARAM_WAREHOUSE_NAME"] = self._warehouse_name
            os.environ["EXPERIMENT_PARAM_TASK_TYPE"] = "query"
        else:
            self._setup_infrastructure(session_or_config, params)
            self._init_feature_store(session_or_config, params)
            self._create_online_service(session_or_config, params)
            self._register_entity(session_or_config, params)
            self._populate_source_data(session_or_config, params)
            self._create_stream_source(session_or_config, params)
            self._create_stream_feature_view(session_or_config, params)
            os.environ["EXPERIMENT_PARAM_TASK_TYPE"] = "ingest"

        num_entity_keys = params.get("num_entity_keys", params.get("num_table_rows", 1000))
        os.environ["EXPERIMENT_PARAM_NUM_ENTITY_KEYS"] = str(num_entity_keys)

    def _create_stream_source(self, session, params):
        num_columns = params.get("num_columns", 10)
        self._stream_source_name = params.get("stream_source_name", "load_test_stream")

        fields = [
            StructField("ID", IntegerType()),
            StructField("EVENT_TIME", TimestampType(TimestampTimeZone.NTZ)),
        ]
        for i in range(num_columns):
            fields.append(StructField(f"COL_{i:03d}", DoubleType()))

        self._stream_source = StreamSource(
            name=self._stream_source_name,
            schema=StructType(fields),
            desc="Load test stream source for ingest experiments",
        )
        self._fs.register_stream_source(self._stream_source)
        os.environ["EXPERIMENT_PARAM_STREAM_SOURCE_NAME"] = self._stream_source_name
        print(f"Registered stream source: {self._stream_source_name}")

    def _create_stream_feature_view(self, session, params):
        self._feature_view_name = params.get("feature_view_name", "load_test_stream_fv")
        self._feature_view_version = params.get("feature_view_version", "V1")

        os.environ["EXPERIMENT_PARAM_FEATURE_VIEW_NAME"] = self._feature_view_name
        os.environ["EXPERIMENT_PARAM_FEATURE_VIEW_VERSION"] = self._feature_view_version

        num_columns = params.get("num_columns", 10)
        col_names = ", ".join(["ID", "TS AS EVENT_TIME"] + [f"COL_{i:03d}" for i in range(num_columns)])
        backfill_df = session.sql(
            f"SELECT {col_names} FROM {DATABASE_NAME}.{SOURCE_SCHEMA}.source_data"
        )

        stream_cfg = StreamConfig(
            stream_source=self._stream_source,
            backfill_df=backfill_df,
        )

        stream_fv = FeatureView(
            name=self._feature_view_name,
            entities=[self._entity],
            stream_config=stream_cfg,
            timestamp_col="EVENT_TIME",
            online_config=OnlineConfig(
                enable=True,
                store_type=OnlineStoreType.POSTGRES,
            ),
            desc="Load test stream feature view for ingest experiments",
        )

        self._registered_fv = self._fs.register_feature_view(
            stream_fv, self._feature_view_version, overwrite=True
        )
        print(f"Registered stream feature view: {self._feature_view_name}/{self._feature_view_version}")

        print("Waiting for stream feature view backfill (30s)...")
        time.sleep(30)

    def teardown_series(self, session_or_config, params):
        if self._query_mode == "SQL":
            self._drop_ingestion_task(session_or_config, params)
            try:
                session_or_config.sql(
                    f"DROP DYNAMIC TABLE IF EXISTS {DATABASE_NAME}.{SOURCE_SCHEMA}.dt"
                ).collect()
            except Exception as e:
                print(f"Error dropping dynamic table: {e}")
            try:
                session_or_config.sql(
                    f"DROP TABLE IF EXISTS {DATABASE_NAME}.{SOURCE_SCHEMA}.source_data"
                ).collect()
                print("Cleaned up source data")
            except Exception as e:
                print(f"Error cleaning up source data: {e}")
        else:
            try:
                if hasattr(self, "_fs"):
                    try:
                        fv = self._fs.get_feature_view(
                            self._feature_view_name, self._feature_view_version
                        )
                        self._fs.delete_feature_view(fv)
                    except Exception as e:
                        print(f"Could not delete stream feature view: {e}")
                    try:
                        self._fs.delete_stream_source(self._stream_source_name)
                    except Exception as e:
                        print(f"Could not delete stream source: {e}")
                    try:
                        self._fs.delete_entity("LOAD_TEST_USER")
                    except Exception:
                        pass
            except Exception as e:
                print(f"Error during feature store teardown: {e}")

            try:
                session_or_config.sql(f"DROP TABLE IF EXISTS {DATABASE_NAME}.{SOURCE_SCHEMA}.source_data").collect()
                print("Cleaned up source data")
            except Exception as e:
                print(f"Error cleaning up source data: {e}")

        try:
            if hasattr(self, "_warehouse_name"):
                session_or_config.sql(f"DROP WAREHOUSE IF EXISTS {self._warehouse_name}").collect()
        except Exception as e:
            print(f"Error dropping warehouse: {e}")


class MixedWorkloadSeries(BaseFSSeries):
    """
    Mixed query + ingest workload series.

    REST mode: Sets up both a batch feature view and a stream source.
    SQL mode: Creates a DT for reads with a scheduled ingestion task.
    """

    can_skip_re_warmup = True

    def setup_series(self, session_or_config, params):
        query_mode = params.get("query_mode", "REST").upper()
        self._query_mode = query_mode

        if query_mode == "SQL":
            self._setup_infrastructure(session_or_config, params)
            self._populate_source_data(session_or_config, params)
            self._create_dynamic_table(session_or_config, params)
            self._wait_for_dynamic_table(session_or_config, params)
            self._setup_ingestion_task(session_or_config, params)
            os.environ["EXPERIMENT_PARAM_WAREHOUSE_NAME"] = self._warehouse_name
            os.environ["EXPERIMENT_PARAM_TASK_TYPE"] = "query"
        else:
            self._setup_infrastructure(session_or_config, params)
            self._init_feature_store(session_or_config, params)
            self._create_online_service(session_or_config, params)
            self._register_entity(session_or_config, params)
            self._populate_source_data(session_or_config, params)
            self._create_feature_view(session_or_config, params)
            self._wait_for_online_data(session_or_config, params)
            self._create_stream_source_for_mixed(session_or_config, params)
            os.environ["EXPERIMENT_PARAM_TASK_TYPE"] = "mixed"

    def _create_stream_source_for_mixed(self, session, params):
        num_columns = params.get("num_columns", 10)
        self._stream_source_name = params.get("stream_source_name", "load_test_mixed_stream")

        fields = [
            StructField("ID", IntegerType()),
            StructField("EVENT_TIME", TimestampType(TimestampTimeZone.NTZ)),
        ]
        for i in range(num_columns):
            fields.append(StructField(f"COL_{i:03d}", DoubleType()))

        stream_source = StreamSource(
            name=self._stream_source_name,
            schema=StructType(fields),
            desc="Load test stream source for mixed workload",
        )
        self._fs.register_stream_source(stream_source)
        os.environ["EXPERIMENT_PARAM_STREAM_SOURCE_NAME"] = self._stream_source_name
        print(f"Registered stream source for mixed workload: {self._stream_source_name}")

    def teardown_series(self, session_or_config, params):
        if self._query_mode == "SQL":
            self._drop_ingestion_task(session_or_config, params)
        else:
            try:
                if hasattr(self, "_fs"):
                    try:
                        self._fs.delete_stream_source(self._stream_source_name)
                    except Exception as e:
                        print(f"Could not delete stream source: {e}")
            except Exception:
                pass
        super().teardown_series(session_or_config, params)
