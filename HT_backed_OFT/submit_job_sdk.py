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
submit_job_sdk.py — Submit SDK benchmark as a headless ML Job on SPCS.

Usage:
    export SNOWFLAKE_CONNECTION_NAME=gkfs
    export SNOWFLAKE_PAT='<your-pat>'
    export SNOWFLAKE_USER='GHAZALEH'
    python HT_backed_OFT/submit_job_sdk.py [--wait] [--logs]
"""

import argparse
import os
import pathlib
import sys

from snowflake.snowpark import Session
from snowflake.ml.jobs import submit_directory

DATABASE  = "FS_BENCHMARK_DB"
SCHEMA    = "FS_BENCHMARK_SCHEMA"
WAREHOUSE = "FS_BENCHMARK_WH"
POOL_NAME = "FS_BENCH_JOB_POOL"
STAGE     = f"@{DATABASE}.{SCHEMA}.FS_BENCH_JOB_PAYLOAD"
EAI       = "FS_BENCH_JOB_EAI"


def _require_env(name):
    val = os.environ.get(name)
    if not val:
        print(f"ERROR: env var {name} is not set.", file=sys.stderr)
        sys.exit(1)
    return val


def main():
    parser = argparse.ArgumentParser(description="Submit SDK latency benchmark as SPCS job")
    parser.add_argument("--wait", action="store_true", help="Block until the job finishes")
    parser.add_argument("--logs", action="store_true", help="Print job logs (implies --wait)")
    args = parser.parse_args()
    if args.logs:
        args.wait = True

    pat  = _require_env("SNOWFLAKE_PAT")
    user = _require_env("SNOWFLAKE_USER")

    conn_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "gkfs")
    session = Session.builder.config("connection_name", conn_name).getOrCreate()
    account = session.get_current_account().strip('"')
    role    = session.get_current_role().strip('"')
    print(f"Session ready | account={account} role={role}")

    payload_dir = str(pathlib.Path(__file__).parent / "payload")
    entrypoint  = "run_benchmark_sdk.py"

    print(f"Payload dir : {payload_dir}")
    print(f"Entrypoint  : {entrypoint}")
    print(f"Stage       : {STAGE}")
    print(f"Pool        : {POOL_NAME}")
    print(f"EAI         : {EAI}")
    print()

    job = submit_directory(
        dir_path=payload_dir,
        compute_pool=POOL_NAME,
        entrypoint=entrypoint,
        stage_name=STAGE,
        external_access_integrations=[EAI],
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
    print(f"Monitor in Snowsight -> Jobs, or run:")
    print(f"  python HT_backed_OFT/submit_job_sdk.py --logs")

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
