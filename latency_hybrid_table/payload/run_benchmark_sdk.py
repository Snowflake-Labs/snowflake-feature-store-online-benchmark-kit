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
run_benchmark_sdk.py — 8-thread SDK benchmark entrypoint for SPCS ML Job.

Submitted via snowflake.ml.jobs.submit_directory.
Runs inside SPCS container with env vars injected by submit_job.py.

Config: 8 threads, 600s warmup, 300s measurement.
Uses multi-cursor on shared session (SPCS auth limitation).
"""

import subprocess, sys

subprocess.run(
    [sys.executable, "-m", "pip", "install", "--quiet",
     "snowflake-ml-python==1.37.0"],
    check=True,
)

import importlib.metadata
print(f"snowflake-ml-python=={importlib.metadata.version('snowflake-ml-python')}")

import os
import time
import json
import uuid
import random
import statistics
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.ml.feature_store import (
    FeatureStore,
    CreationMode,
    StoreType,
)

DATABASE   = os.environ.get("SNOWFLAKE_DATABASE",  "FS_BENCHMARK_DB")
SCHEMA     = os.environ.get("SNOWFLAKE_SCHEMA",    "FS_BENCHMARK_SCHEMA")
FS_SCHEMA  = "FS_BENCHMARK_FS"
WAREHOUSE  = os.environ.get("SNOWFLAKE_WAREHOUSE", "FS_BENCHMARK_WH")

FV_NAME    = "CUSTOMER_FEATURES"
FV_VER     = "v1"
N_KEYS     = 100_000
N_THREADS  = 8
WARMUP_SECONDS  = 600
MEASURE_SECONDS = 300

ENV_TAG = f"SPCS_SDK_{N_THREADS}T"

_spcs_host = os.environ.get("SNOWFLAKE_HOST")

try:
    session = get_active_session()
    session.sql(f"USE WAREHOUSE {WAREHOUSE}").collect()
    session.sql(f"USE DATABASE {DATABASE}").collect()
    session.sql(f"USE SCHEMA {DATABASE}.{SCHEMA}").collect()
    print(f"Auth: active SPCS session (reused from launcher)  host={_spcs_host}")
except Exception:
    print(f"Auth: PAT fallback (no active session)  host={_spcs_host or '(derived from account)'}")
    for _var in ("SNOWFLAKE_PAT", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"):
        if not os.environ.get(_var):
            print(f"FATAL: env var {_var} is not set", file=sys.stderr)
            sys.exit(1)
    _conn_params = {
        "account":       os.environ["SNOWFLAKE_ACCOUNT"],
        "user":          os.environ["SNOWFLAKE_USER"],
        "authenticator": "PROGRAMMATIC_ACCESS_TOKEN",
        "token":         os.environ["SNOWFLAKE_PAT"],
        "role":          os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse":     WAREHOUSE,
        "database":      DATABASE,
        "schema":        SCHEMA,
    }
    if _spcs_host:
        _conn_params["host"] = _spcs_host
    session = Session.builder.configs(_conn_params).create()

print(f"Session: account={session.get_current_account()} role={session.get_current_role()}")

fs = FeatureStore(
    session=session,
    database=DATABASE,
    name=FS_SCHEMA,
    default_warehouse=WAREHOUSE,
    creation_mode=CreationMode.FAIL_IF_NOT_EXIST,
)

fv = fs.get_feature_view(FV_NAME, FV_VER)
print(f"FV: {fv.name}/{fv.version} status={fv.status}")

print("\nSanity check: verifying read_feature_view returns data...")
_sanity = fs.read_feature_view(
    fv,
    keys=[["CUST_0000000000"]],
    store_type=StoreType.ONLINE,
).collect()
assert len(_sanity) > 0, "Sanity check failed: read_feature_view returned 0 rows"
print(f"Sanity check passed: {len(_sanity)} row, {len(_sanity[0].as_dict())} cols")

_perf = time.perf_counter


def worker_fn(thread_id, phase, duration_s):
    latencies = []
    count = 0
    start = time.time()
    deadline = start + duration_s

    while time.time() < deadline:
        cid = f"CUST_{random.randint(0, N_KEYS - 1):010d}"
        t0 = _perf()
        fs.read_feature_view(
            feature_view=fv,
            keys=[[cid]],
            store_type=StoreType.ONLINE,
        ).collect()
        elapsed_ms = (_perf() - t0) * 1000.0

        if phase == "measure":
            latencies.append(elapsed_ms)
        count += 1

        if count % 500 == 0 and thread_id == 0:
            elapsed = time.time() - start
            remaining = duration_s - elapsed
            print(f"  [{phase}] thread-0: {count} reads, {elapsed:.0f}s elapsed, {remaining:.0f}s remaining")

    return latencies


print(f"\n{'='*60}")
print(f"  SDK Benchmark — {N_THREADS} threads")
print(f"  Warmup: {WARMUP_SECONDS}s, Measurement: {MEASURE_SECONDS}s")
print(f"{'='*60}")

print(f"\nWarming up ({WARMUP_SECONDS}s, {N_THREADS} threads) ...")
wu_start = _perf()
with ThreadPoolExecutor(max_workers=N_THREADS) as pool:
    wu_futures = [pool.submit(worker_fn, i, "warmup", WARMUP_SECONDS) for i in range(N_THREADS)]
    for fut in as_completed(wu_futures):
        fut.result()
wu_elapsed = _perf() - wu_start
print(f"Warm-up complete in {wu_elapsed:.1f}s")

print(f"\nMeasuring ({MEASURE_SECONDS}s, {N_THREADS} threads) ...")
m_start = _perf()
all_latencies = []
with ThreadPoolExecutor(max_workers=N_THREADS) as pool:
    m_futures = [pool.submit(worker_fn, i, "measure", MEASURE_SECONDS) for i in range(N_THREADS)]
    for fut in as_completed(m_futures):
        all_latencies.extend(fut.result())
wall_s = _perf() - m_start

all_latencies.sort()
_n = len(all_latencies)

def _pct(s, p):
    return s[min(int(p / 100 * _n), _n - 1)]

_p50  = _pct(all_latencies, 50)
_p90  = _pct(all_latencies, 90)
_p95  = _pct(all_latencies, 95)
_p99  = _pct(all_latencies, 99)
_mean = statistics.mean(all_latencies)
_std  = statistics.stdev(all_latencies) if _n > 1 else 0.0
_min  = all_latencies[0]
_max  = all_latencies[-1]
_qps  = _n / wall_s

print(f"\nMeasurement complete: {_n:,} reads in {wall_s:.2f}s ({_qps:.1f} QPS)")
print("=" * 52)
print(f"  Threads    : {N_THREADS:>8}")
print(f"  N          : {_n:>8,}")
print(f"  QPS        : {_qps:>8.1f}")
print(f"  Min   (ms) : {_min:>8.2f}")
print(f"  P50   (ms) : {_p50:>8.2f}")
print(f"  P90   (ms) : {_p90:>8.2f}")
print(f"  P95   (ms) : {_p95:>8.2f}")
print(f"  P99   (ms) : {_p99:>8.2f}")
print(f"  Mean  (ms) : {_mean:>8.2f}")
print(f"  Stdev (ms) : {_std:>8.2f}")
print(f"  Max   (ms) : {_max:>8.2f}")
print("=" * 52)

_run_id = str(uuid.uuid4())
_ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

session.sql(f"""
    INSERT INTO {DATABASE}.{SCHEMA}.FS_BENCH_JOB_RESULTS_TBL
        (RUN_ID, TS, ENV, N_THREADS, WARMUP_SECONDS, MEASURE_SECONDS,
         TOTAL_REQUESTS, QPS, P50_MS, P90_MS, P95_MS, P99_MS,
         MEAN_MS, STDEV_MS, MIN_MS, MAX_MS)
    VALUES
        ('{_run_id}', '{_ts_str}'::TIMESTAMP_NTZ, '{ENV_TAG}',
         {N_THREADS}, {WARMUP_SECONDS}, {MEASURE_SECONDS},
         {_n}, {_qps}, {_p50}, {_p90}, {_p95}, {_p99},
         {_mean}, {_std}, {_min}, {_max})
""").collect()
print(f"Results persisted: RUN_ID={_run_id}")

_payload = {
    "run_id": _run_id, "ts": _ts_str, "env": ENV_TAG,
    "n_threads": N_THREADS, "warmup_seconds": WARMUP_SECONDS,
    "measure_seconds": MEASURE_SECONDS,
    "total_requests": _n, "qps": _qps,
    "p50_ms": _p50, "p90_ms": _p90, "p95_ms": _p95, "p99_ms": _p99,
    "mean_ms": _mean, "stdev_ms": _std, "min_ms": _min, "max_ms": _max,
    "latencies_ms": all_latencies,
}
_json_path = f"/tmp/benchmark_sdk_{_run_id}.json"
with open(_json_path, "w") as _fh:
    json.dump(_payload, _fh)

session.sql(f"PUT file://{_json_path} @{DATABASE}.{SCHEMA}.FS_BENCH_JOB_RESULTS AUTO_COMPRESS=TRUE").collect()
print(f"Raw latency array uploaded to @FS_BENCH_JOB_RESULTS/benchmark_sdk_{_run_id}.json.gz")

session.sql(f"""
    SELECT RUN_ID, TS, ENV, N_THREADS, TOTAL_REQUESTS, QPS, P50_MS, P95_MS, P99_MS
    FROM {DATABASE}.{SCHEMA}.FS_BENCH_JOB_RESULTS_TBL
    ORDER BY TS DESC LIMIT 10
""").show()

session.close()
