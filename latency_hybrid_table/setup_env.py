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
setup_env.py — One-time idempotent provisioning for SPCS job-based benchmarks.

Run this ONCE from your laptop before submitting jobs:

    export SNOWFLAKE_CONNECTION_NAME=gkfs
    python latency_hybrid_table/setup_env.py

What it provisions:
  - Compute pool  : FS_BENCH_JOB_POOL (CPU_X64_SL, 1 node)
  - Network rules : PyPI egress + *.snowflakecomputing.app egress
  - EAIs          : FS_BENCH_JOB_EAI (PyPI) + FS_BENCH_JOB_SVC_EAI (REST endpoint)
  - Stages        : FS_BENCH_JOB_PAYLOAD, FS_BENCH_JOB_RESULTS
  - Results table : FS_BENCH_JOB_RESULTS_TBL
  - Sanity check  : Verifies CUSTOMER_FEATURES/v1 online table is queryable
"""

import os
import sys

from snowflake.snowpark import Session
from snowflake.ml.feature_store import (
    FeatureStore,
    CreationMode,
    StoreType,
)

DATABASE  = "FS_BENCHMARK_DB"
SCHEMA    = "FS_BENCHMARK_SCHEMA"
FS_SCHEMA = "FS_BENCHMARK_FS"
WAREHOUSE = "FS_BENCHMARK_WH"

POOL_NAME = "FS_BENCH_JOB_POOL"
NR_PYPI   = "FS_BENCH_JOB_NR_PYPI"
EAI_PYPI  = "FS_BENCH_JOB_EAI"
NR_SVC    = "FS_BENCH_JOB_NR_SVC"
EAI_SVC   = "FS_BENCH_JOB_SVC_EAI"

FV_NAME = "CUSTOMER_FEATURES"
FV_VER  = "v1"


def _sql(session, stmt, *, silent=False):
    if not silent:
        print(f"  SQL: {stmt[:120].strip()}")
    return session.sql(stmt).collect()


def build_session():
    conn_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "gkfs")
    session = Session.builder.config("connection_name", conn_name).getOrCreate()
    _sql(session, f"USE WAREHOUSE {WAREHOUSE}", silent=True)
    _sql(session, f"USE DATABASE {DATABASE}", silent=True)
    _sql(session, f"USE SCHEMA {DATABASE}.{SCHEMA}", silent=True)
    print(f"Session ready | account={session.get_current_account()} role={session.get_current_role()}")
    return session


def provision_compute_pool(session):
    print("\n[1/6] Compute pool")
    _sql(session, f"""
        CREATE COMPUTE POOL IF NOT EXISTS {POOL_NAME}
          MIN_NODES = 1
          MAX_NODES = 1
          INSTANCE_FAMILY = CPU_X64_SL
          AUTO_RESUME = TRUE
          AUTO_SUSPEND_SECS = 1800
    """)
    print(f"  Pool {POOL_NAME} (CPU_X64_SL): OK")


def provision_eais(session):
    print("\n[2/6] Network rules + EAIs")

    _sql(session, f"""
        CREATE OR REPLACE NETWORK RULE {DATABASE}.{SCHEMA}.{NR_PYPI}
          MODE = EGRESS
          TYPE = HOST_PORT
          VALUE_LIST = ('pypi.org', 'files.pythonhosted.org', 'pypi.python.org')
    """)
    _sql(session, f"""
        CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {EAI_PYPI}
          ALLOWED_NETWORK_RULES = ({DATABASE}.{SCHEMA}.{NR_PYPI})
          ENABLED = TRUE
    """)
    print(f"  EAI {EAI_PYPI} (PyPI): OK")

    _sql(session, f"""
        CREATE OR REPLACE NETWORK RULE {DATABASE}.{SCHEMA}.{NR_SVC}
          MODE = EGRESS
          TYPE = HOST_PORT
          VALUE_LIST = ('*.snowflakecomputing.app')
    """)
    _sql(session, f"""
        CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {EAI_SVC}
          ALLOWED_NETWORK_RULES = ({DATABASE}.{SCHEMA}.{NR_SVC})
          ENABLED = TRUE
    """)
    print(f"  EAI {EAI_SVC} (*.snowflakecomputing.app): OK")


def provision_stages(session):
    print("\n[3/6] Stages")
    _sql(session, f"CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.FS_BENCH_JOB_PAYLOAD")
    _sql(session, f"CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.FS_BENCH_JOB_RESULTS")
    print("  Stages: OK")


def provision_results_table(session):
    print("\n[4/6] Results table")
    _sql(session, f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.FS_BENCH_JOB_RESULTS_TBL (
            RUN_ID            VARCHAR,
            TS                TIMESTAMP_NTZ,
            ENV               VARCHAR,
            N_THREADS         INTEGER,
            WARMUP_SECONDS    INTEGER,
            MEASURE_SECONDS   INTEGER,
            TOTAL_REQUESTS    INTEGER,
            QPS               FLOAT,
            P50_MS            FLOAT,
            P90_MS            FLOAT,
            P95_MS            FLOAT,
            P99_MS            FLOAT,
            MEAN_MS           FLOAT,
            STDEV_MS          FLOAT,
            MIN_MS            FLOAT,
            MAX_MS            FLOAT
        )
    """)
    print("  FS_BENCH_JOB_RESULTS_TBL: OK")


def verify_feature_view(session):
    print("\n[5/6] Verify feature view exists")
    fs = FeatureStore(
        session=session,
        database=DATABASE,
        name=FS_SCHEMA,
        default_warehouse=WAREHOUSE,
        creation_mode=CreationMode.FAIL_IF_NOT_EXIST,
    )
    fv = fs.get_feature_view(FV_NAME, FV_VER)
    print(f"  FV: {fv.name}/{fv.version} status={fv.status}")
    return fs, fv


def sanity_online_check(fs, fv):
    print("\n[6/6] Sanity online lookup")
    result = fs.read_feature_view(
        fv,
        keys=[["CUST_0000000000"]],
        store_type=StoreType.ONLINE,
    ).collect()
    if len(result) > 0:
        print(f"  Online lookup: OK ({len(result)} row, {len(result[0].as_dict())} cols)")
    else:
        print("  WARNING: Online lookup returned 0 rows — data may not be synced yet.")


def main():
    session = build_session()
    provision_compute_pool(session)
    provision_eais(session)
    provision_stages(session)
    provision_results_table(session)
    fs, fv = verify_feature_view(session)
    sanity_online_check(fs, fv)

    print("\n=== setup_env.py complete ===")
    print("Next steps:")
    print("  python latency_hybrid_table/submit_job_sdk.py --wait --logs         # SDK benchmark")
    print("  python latency_hybrid_table/submit_job_direct_sql.py --wait --logs  # Direct SQL benchmark")
    session.close()


if __name__ == "__main__":
    main()
