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
submit_job_rest.py — Submit the REST-based latency benchmark as a headless ML Job on SPCS.

Runs direct HTTP/2 calls via `httpx` to the Postgres Online Service `/api/v1/query`
endpoint using a persistent connection. Bypasses the Snowpark SDK entirely.

Usage:
    export SNOWFLAKE_PAT='<your-pat>'
    export SNOWFLAKE_USER='<your-username>'          # e.g. GHAZALEH
    python submit_job_rest.py [--wait] [--logs]
"""

import argparse
import os
import pathlib
import sys

from snowflake.snowpark import Session
from snowflake.ml.jobs import submit_directory

DATABASE  = "RTFS_DEMO_DB"
SCHEMA    = "OFT_DEMO"
WAREHOUSE = "DEMO_WH"
POOL_NAME = "FS_LAT_POOL"
STAGE     = f"@{DATABASE}.{SCHEMA}.JOB_PAYLOAD"
EAI       = "FS_LAT_PYPI_EAI"
EAI_SVC   = "FS_LAT_ONLINE_SVC_EAI"


def _require_env(name):
    val = os.environ.get(name)
    if not val:
        print(f"ERROR: env var {name} is not set.", file=sys.stderr)
        sys.exit(1)
    return val


def main():
    parser = argparse.ArgumentParser(description="Submit Postgres REST latency benchmark as a headless ML Job")
    parser.add_argument("--wait", action="store_true", help="Block until the job finishes")
    parser.add_argument("--logs", action="store_true", help="Print job logs (implies --wait)")
    args = parser.parse_args()
    if args.logs:
        args.wait = True

    pat  = _require_env("SNOWFLAKE_PAT")
    user = _require_env("SNOWFLAKE_USER")

    session = Session.builder.config("connection_name", "vnextqa6").getOrCreate()
    account = session.get_current_account().strip('"')
    role    = session.get_current_role().strip('"')
    host    = session._conn._conn.host
    print(f"Session ready | account={account} role={role} host={host}")

    payload_dir = str(pathlib.Path(__file__).parent / "payload")
    entrypoint  = "run_benchmark_rest.py"

    print(f"Payload dir : {payload_dir}")
    print(f"Entrypoint  : {entrypoint}")
    print(f"Stage       : {STAGE}")
    print(f"Pool        : {POOL_NAME}")
    print(f"EAI         : {EAI}, {EAI_SVC}")
    print()

    job = submit_directory(
        dir_path=payload_dir,
        compute_pool=POOL_NAME,
        entrypoint=entrypoint,
        stage_name=STAGE,
        external_access_integrations=[EAI, EAI_SVC],
        session=session,
        database=DATABASE,
        schema=SCHEMA,
        env_vars={
            "SNOWFLAKE_PAT":       pat,
            "SNOWFLAKE_ACCOUNT":   account,
            "SNOWFLAKE_USER":      user,
            "SNOWFLAKE_ROLE":      role,
            "SNOWFLAKE_WAREHOUSE": WAREHOUSE,
            "SNOWFLAKE_DATABASE":  DATABASE,
            "SNOWFLAKE_SCHEMA":    SCHEMA,
        },
    )

    print(f"Job submitted  | id={job.id}")
    print(f"Monitor in Snowsight → Jobs, or run:")
    print(f"  python submit_job_rest.py --logs")

    if args.wait:
        print("\nWaiting for job to complete ...")
        job.wait()
        print(f"\nJob status: {job.status}")

    if args.logs:
        print("\n--- Job Logs ---")
        print(job.get_logs())

    session.close()


if __name__ == "__main__":
    main()
