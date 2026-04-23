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
run_benchmark_sdk.py — SDK-based headless entrypoint for SPCS ML Job.

Runs `fs.read_feature_view(fv, keys=[[key]], store_type=StoreType.ONLINE)`
against the Postgres Online Service. Submitted via snowflake.ml.jobs.submit_directory.

The SDK uses an internal HTTP/2 REST client against `/api/v1/query` for Postgres
online reads and returns a pandas DataFrame directly (no `.collect()` / SQL
round-trip). Latency profile is equivalent to the manual REST benchmark.

ENV tag in results: SPCS
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
import statistics
from datetime import datetime, timezone

from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.ml.feature_store import (
    FeatureStore,
    CreationMode,
    OnlineConfig,
    OnlineStoreType,
    StoreType,
)

DATABASE  = os.environ.get("SNOWFLAKE_DATABASE",  "RTFS_DEMO_DB")
SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA",    "OFT_DEMO")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "DEMO_WH")
FV_NAME   = "BENCHMARK_USER_FEATURES"
FV_VER    = "V1"
N_WARMUP  = 100
N_MEASURE = 5000
N_KEYS    = 1000

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
    name=SCHEMA,
    default_warehouse=WAREHOUSE,
    creation_mode=CreationMode.FAIL_IF_NOT_EXIST,
)

fv = fs.get_feature_view(FV_NAME, FV_VER)

svc_status = fs.get_online_service_status()
if svc_status.status != "RUNNING":
    print(f"FATAL: Online Service is {svc_status.status} — aborting", file=sys.stderr)
    sys.exit(1)

_query_url = next(ep.url for ep in svc_status.endpoints if ep.name == "query")
print(f"Query URL: {_query_url}")

fv._online_config = OnlineConfig(
    enable=True,
    target_lag="10s",
    store_type=OnlineStoreType.POSTGRES,
)
fv._postgres_online_query_url = _query_url
print(f"FV: {fv.name}/{fv.version}  status={fv.status}")

keys = [[f"user_{i}"] for i in range(N_KEYS)]

_read   = fs.read_feature_view
_ONLINE = StoreType.ONLINE

print(f"Warming up ({N_WARMUP} throwaway reads) ...")
_t_wu = time.perf_counter()
for _i in range(N_WARMUP):
    _read(fv, keys=[keys[_i % N_KEYS]], store_type=_ONLINE)
print(f"Warm-up complete in {(time.perf_counter() - _t_wu)*1e3:.1f} ms")

_perf = time.perf_counter
_lat  = [0.0] * N_MEASURE

_loop_start = _perf()
for _i in range(N_MEASURE):
    _k  = [keys[_i % N_KEYS]]
    _t0 = _perf()
    _read(fv, keys=_k, store_type=_ONLINE)
    _lat[_i] = _perf() - _t0
_wall_s = _perf() - _loop_start

print(f"Measurement complete: {N_MEASURE} reads in {_wall_s:.2f}s  ({N_MEASURE/_wall_s:.1f} QPS)")

_lat_ms = [v * 1e3 for v in _lat]
_sorted = sorted(_lat_ms)
_n      = len(_sorted)

def _pct(s, p):
    return s[min(int(p / 100 * _n), _n - 1)]

_p50  = _pct(_sorted, 50)
_p90  = _pct(_sorted, 90)
_p99  = _pct(_sorted, 99)
_mean = statistics.mean(_lat_ms)
_std  = statistics.stdev(_lat_ms)
_min  = _sorted[0]
_max  = _sorted[-1]
_qps  = N_MEASURE / _wall_s

print("=" * 52)
print(f"  N          : {_n:>8,}")
print(f"  QPS        : {_qps:>8.1f}")
print(f"  Min   (ms) : {_min:>8.2f}")
print(f"  P50   (ms) : {_p50:>8.2f}")
print(f"  P90   (ms) : {_p90:>8.2f}")
print(f"  P99   (ms) : {_p99:>8.2f}")
print(f"  Mean  (ms) : {_mean:>8.2f}")
print(f"  Stdev (ms) : {_std:>8.2f}")
print(f"  Max   (ms) : {_max:>8.2f}")
print("=" * 52)

_run_id  = str(uuid.uuid4())
_ts_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
_env_tag = "SPCS"

session.sql(f"""
    INSERT INTO {DATABASE}.{SCHEMA}.BENCHMARK_RESULTS
        (RUN_ID, TS, ENV, N, P50_MS, P90_MS, P99_MS, MEAN_MS, STDEV_MS, MIN_MS, MAX_MS)
    VALUES
        ('{_run_id}', '{_ts_str}'::TIMESTAMP_NTZ, '{_env_tag}',
         {_n}, {_p50}, {_p90}, {_p99}, {_mean}, {_std}, {_min}, {_max})
""").collect()
print(f"Results persisted: RUN_ID={_run_id}")

_payload = {
    "run_id": _run_id, "ts": _ts_str, "env": _env_tag, "n": _n,
    "p50_ms": _p50, "p90_ms": _p90, "p99_ms": _p99,
    "mean_ms": _mean, "stdev_ms": _std, "min_ms": _min, "max_ms": _max,
    "latencies_ms": _lat_ms,
}
_json_path = f"/tmp/benchmark_{_run_id}.json"
with open(_json_path, "w") as _fh:
    json.dump(_payload, _fh)

session.sql(f"PUT file://{_json_path} @{DATABASE}.{SCHEMA}.BENCHMARK_RESULTS_STAGE AUTO_COMPRESS=TRUE").collect()
print(f"Raw latency array uploaded to @BENCHMARK_RESULTS_STAGE/benchmark_{_run_id}.json.gz")

session.sql(f"""
    SELECT RUN_ID, TS, ENV, N, P50_MS, P90_MS, P99_MS, MEAN_MS, MIN_MS, MAX_MS
    FROM {DATABASE}.{SCHEMA}.BENCHMARK_RESULTS
    ORDER BY TS DESC LIMIT 10
""").show()

session.close()
