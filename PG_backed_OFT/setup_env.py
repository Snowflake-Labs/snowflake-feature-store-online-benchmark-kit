# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
setup_env.py — One-time idempotent provisioning for the Postgres Online Service latency benchmark.

Run this ONCE from your laptop before submitting the headless job:

    export SNOWFLAKE_PAT='<your-pat>'
    python setup_env.py

What it provisions:
  - Compute pool  : FS_LAT_POOL (HIGHMEM_X64_S, 1 node)
  - Network rule  : FS_LAT_PYPI_RULE (PyPI egress for wheel deps)
  - EAI           : FS_LAT_PYPI_EAI
  - Stage         : JOB_PAYLOAD (for submit_notebook payload)
  - Stage         : BENCHMARK_RESULTS_STAGE (for raw latency JSON dumps)
  - Source table  : BENCHMARK_USER_FEATURES_SOURCE (100 K rows, 5 float cols)
  - Results table : BENCHMARK_RESULTS
  - Entity        : user (reuses existing if present)
  - Feature View  : BENCHMARK_USER_FEATURES V1 (Postgres online, target_lag=10s)
  - Waits for offline backfill, then fires one sanity online lookup.
"""

import os
import sys
import time

from snowflake.snowpark import Session
from snowflake.ml.feature_store import (
    FeatureStore,
    FeatureView,
    Entity,
    CreationMode,
    OnlineConfig,
    OnlineStoreType,
    StoreType,
)
from snowflake.ml.feature_store import online_service

DATABASE  = "RTFS_DEMO_DB"
SCHEMA    = "OFT_DEMO"
WAREHOUSE = "DEMO_WH"
POOL_NAME = "FS_LAT_POOL"
PRODUCER_ROLE = "DEMO_PRODUCER_ROLE"
CONSUMER_ROLE = "DEMO_CONSUMER_ROLE"

FV_NAME        = "BENCHMARK_USER_FEATURES"
FV_VERSION     = "V1"
SOURCE_TABLE   = "BENCHMARK_USER_FEATURES_SOURCE"
RESULTS_TABLE  = "BENCHMARK_RESULTS"
N_ROWS         = 100_000


def _sql(session, stmt, *, silent=False):
    if not silent:
        print(f"  SQL: {stmt[:120].strip()}")
    return session.sql(stmt).collect()


def build_session():
    pat = os.environ.get("SNOWFLAKE_PAT")
    if not pat:
        print("ERROR: SNOWFLAKE_PAT environment variable is not set.", file=sys.stderr)
        sys.exit(1)
    session = Session.builder.config("connection_name", "vnextqa6").getOrCreate()
    _sql(session, f"USE WAREHOUSE {WAREHOUSE}", silent=True)
    _sql(session, f"CREATE DATABASE IF NOT EXISTS {DATABASE}", silent=True)
    _sql(session, f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{SCHEMA}", silent=True)
    _sql(session, f"USE DATABASE {DATABASE}", silent=True)
    _sql(session, f"USE SCHEMA {DATABASE}.{SCHEMA}", silent=True)
    print(f"Session ready | account={session.get_current_account()} role={session.get_current_role()}")
    return session


def provision_compute_pool(session):
    print("\n[1/7] Compute pool")
    _sql(session, f"""
        CREATE COMPUTE POOL IF NOT EXISTS {POOL_NAME}
          MIN_NODES = 1
          MAX_NODES = 1
          INSTANCE_FAMILY = HIGHMEM_X64_S
          AUTO_RESUME = TRUE
          AUTO_SUSPEND_SECS = 3600
    """)
    print(f"  Pool {POOL_NAME}: OK")


def provision_eai(session):
    print("\n[2/7] Network rule + EAI (PyPI egress for wheel deps)")
    _sql(session, f"""
        CREATE OR REPLACE NETWORK RULE FS_LAT_PYPI_RULE
          MODE = EGRESS
          TYPE = HOST_PORT
          VALUE_LIST = ('pypi.org', 'files.pythonhosted.org', 'pypi.python.org')
    """)
    _sql(session, f"""
        CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION FS_LAT_PYPI_EAI
          ALLOWED_NETWORK_RULES = (FS_LAT_PYPI_RULE)
          ENABLED = TRUE
    """)
    print("  EAI FS_LAT_PYPI_EAI: OK")


def provision_stages(session):
    print("\n[3/7] Stages")
    _sql(session, f"CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.JOB_PAYLOAD")
    _sql(session, f"CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.BENCHMARK_RESULTS_STAGE")
    print("  Stages: OK")


def provision_source_table(session):
    print(f"\n[4/7] Source table ({N_ROWS:,} rows)")
    _sql(session, f"""
        CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.{SOURCE_TABLE} AS
        SELECT
            'user_' || TO_VARCHAR(SEQ4())                    AS USER_ID,
            UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())            AS F1,
            UNIFORM(0::FLOAT, 100::FLOAT, RANDOM())          AS F2,
            UNIFORM(-1::FLOAT, 1::FLOAT, RANDOM())           AS F3,
            UNIFORM(0::FLOAT, 1000::FLOAT, RANDOM())         AS F4,
            UNIFORM(0::FLOAT, 10::FLOAT, RANDOM())           AS F5,
            DATEADD('second', -SEQ4(), CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)  AS EVENT_TIME
        FROM TABLE(GENERATOR(ROWCOUNT => {N_ROWS}))
    """)
    count = session.sql(f"SELECT COUNT(*) AS CNT FROM {DATABASE}.{SCHEMA}.{SOURCE_TABLE}").collect()[0]["CNT"]
    print(f"  {SOURCE_TABLE}: {count:,} rows")


def provision_results_table(session):
    print("\n[5/7] Results table")
    _sql(session, f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{RESULTS_TABLE} (
            RUN_ID     VARCHAR,
            TS         TIMESTAMP_NTZ,
            ENV        VARCHAR,
            N          INTEGER,
            P50_MS     FLOAT,
            P90_MS     FLOAT,
            P99_MS     FLOAT,
            MEAN_MS    FLOAT,
            STDEV_MS   FLOAT,
            MIN_MS     FLOAT,
            MAX_MS     FLOAT
        )
    """)
    print(f"  {RESULTS_TABLE}: OK")


def provision_feature_view(session):
    print("\n[6/7] Feature View + Online Service")
    fs = FeatureStore(
        session=session,
        database=DATABASE,
        name=SCHEMA,
        default_warehouse=WAREHOUSE,
        creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
    )

    status = fs.get_online_service_status()
    if status.status == "RUNNING":
        print("  Online Service: already RUNNING")
    else:
        print(f"  Online Service status: {status.status} — creating ...")
        try:
            fs.create_online_service(PRODUCER_ROLE, CONSUMER_ROLE)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                print("  Online Service: already exists, waiting for RUNNING ...")
            else:
                raise
        print("  Polling for RUNNING (may take 5–10 min) ...")
        deadline = time.time() + 15 * 60
        while time.time() < deadline:
            status = fs.get_online_service_status()
            print(f"    status: {status.status}")
            if status.status == "RUNNING":
                break
            time.sleep(30)
        else:
            print("ERROR: Online Service did not reach RUNNING in 15 min.", file=sys.stderr)
            sys.exit(1)
        print("  Online Service: RUNNING")

    user_entity = Entity(name="user", join_keys=["USER_ID"], desc="A unique user")
    try:
        fs.register_entity(user_entity)
        print("  Entity user: registered")
    except Exception:
        print("  Entity user: already exists (skipped)")

    source_df = session.table(f"{DATABASE}.{SCHEMA}.{SOURCE_TABLE}")

    fv = FeatureView(
        name=FV_NAME,
        entities=[user_entity],
        feature_df=source_df,
        timestamp_col="EVENT_TIME",
        refresh_freq="1m",
        online_config=OnlineConfig(
            enable=True,
            target_lag="10s",
            store_type=OnlineStoreType.POSTGRES,
        ),
        desc="Benchmark FV: 5 float features per user, Postgres online store",
    )

    existing = [r["NAME"] for r in fs.list_feature_views().collect()]
    if FV_NAME.upper() in [n.upper() for n in existing]:
        print(f"  FV {FV_NAME}: already exists — fetching")
        registered_fv = fs.get_feature_view(FV_NAME, FV_VERSION)
    else:
        print(f"  Registering FV {FV_NAME} ...")
        registered_fv = fs.register_feature_view(
            feature_view=fv,
            version=FV_VERSION,
            block=True,
        )
        print(f"  FV {FV_NAME}/{FV_VERSION}: registered")

    print("  Waiting for offline backfill ...")
    deadline = time.time() + 10 * 60
    while time.time() < deadline:
        cnt = fs.read_feature_view(registered_fv, store_type=StoreType.OFFLINE).count()
        print(f"    offline rows: {cnt:,}")
        if cnt > 0:
            print("  Offline backfill complete.")
            break
        time.sleep(15)
    else:
        print("WARNING: offline backfill timed out — online data may not be ready yet.", file=sys.stderr)

    return fs, registered_fv


def sanity_online_check(fs, registered_fv):
    print("\n[7/7] Sanity online lookup")
    status = fs.get_online_service_status()
    if status.status != "RUNNING":
        print(f"  Online Service status: {status.status} — skipping sanity check.")
        return

    query_url = next(
        (ep.url for ep in status.endpoints if ep.name == "query"), None
    )
    print(f"  Query URL: {query_url}")

    fv_live = fs.get_feature_view(FV_NAME, FV_VERSION)
    fv_live._online_config = OnlineConfig(
        enable=True,
        target_lag="10s",
        store_type=OnlineStoreType.POSTGRES,
    )
    fv_live._postgres_online_query_url = query_url

    deadline = time.time() + 5 * 60
    while time.time() < deadline:
        try:
            result = fs.read_feature_view(
                fv_live,
                keys=[["user_0"], ["user_1"], ["user_2"]],
                store_type=StoreType.ONLINE,
            )
            if result.count() > 0:
                print("  Online lookup: OK")
                result.show()
                return
        except Exception as exc:
            print(f"  Waiting for online data ... ({exc})")
        time.sleep(15)
    print("WARNING: online sanity check timed out.", file=sys.stderr)


def main():
    session = build_session()
    provision_compute_pool(session)
    provision_eai(session)
    provision_stages(session)
    provision_source_table(session)
    provision_results_table(session)
    fs, registered_fv = provision_feature_view(session)
    sanity_online_check(fs, registered_fv)

    print("\n=== setup_env.py complete ===")
    print("Next step:  python submit_job_sdk.py  (or submit_job_rest.py)")
    session.close()


if __name__ == "__main__":
    main()
