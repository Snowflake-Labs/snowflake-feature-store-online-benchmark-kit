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
run_benchmark_rest.py — REST-based headless entrypoint for SPCS ML Job.

Direct HTTP/2 calls via `httpx` to the Postgres Online Service `/api/v1/query` endpoint.
Best-practice variant: persistent HTTP/2 connection, zero SDK overhead in the hot loop.
Auth for Online Service uses SNOWFLAKE_PAT env var.
Snowpark session uses get_active_session() (SPCS launcher session).
ENV tag in results: SPCS_HTTP2
"""

import subprocess, sys

subprocess.run(
    [sys.executable, "-m", "pip", "install", "--quiet",
     "snowflake-ml-python==1.37.0"],
    check=True,
)

import importlib.metadata
print(f"snowflake-ml-python=={importlib.metadata.version('snowflake-ml-python')}")
subprocess.run(
    [sys.executable, "-m", "pip", "install", "--quiet", "httpx[http2]"],
    check=True,
)

import os
import time
import json
import uuid
import statistics
import urllib.parse
from datetime import datetime, timezone

import httpx
from snowflake.snowpark.context import get_active_session
from snowflake.ml.feature_store import FeatureStore, CreationMode, StoreType

DATABASE  = os.environ.get("SNOWFLAKE_DATABASE",  "RTFS_DEMO_DB")
SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA",    "OFT_DEMO")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "DEMO_WH")
FV_NAME   = "BENCHMARK_USER_FEATURES"
FV_VER    = "V1"
N_WARMUP  = 100
N_MEASURE = 5000
N_KEYS    = 1000

pat = os.environ.get("SNOWFLAKE_PAT", "").strip()
if not pat:
    print("FATAL: SNOWFLAKE_PAT is not set", file=sys.stderr)
    sys.exit(1)

session = get_active_session()
session.sql(f"USE WAREHOUSE {WAREHOUSE}").collect()
session.sql(f"USE DATABASE {DATABASE}").collect()
session.sql(f"USE SCHEMA {DATABASE}.{SCHEMA}").collect()
print(f"Session: account={session.get_current_account()} role={session.get_current_role()}")

fs = FeatureStore(
    session=session,
    database=DATABASE,
    name=SCHEMA,
    default_warehouse=WAREHOUSE,
    creation_mode=CreationMode.FAIL_IF_NOT_EXIST,
)

fv = fs.get_feature_view(FV_NAME, FV_VER)
_join_keys = [str(k) for e in fv.entities for k in e.join_keys]
print(f"Feature view: name={fv.name!r} version={fv.version!r} join_keys={_join_keys}")

JOIN_KEY = _join_keys[0] if _join_keys else "USER_ID"

svc_status = fs.get_online_service_status()
if svc_status.status != "RUNNING":
    print(f"FATAL: Online Service is {svc_status.status} — aborting", file=sys.stderr)
    sys.exit(1)

_base_url  = next(ep.url for ep in svc_status.endpoints if ep.name == "query")
_query_url = urllib.parse.urljoin(_base_url.rstrip("/") + "/", "api/v1/query")
print(f"Query URL (HTTP/2): {_query_url}")

_headers = {
    "Authorization": f'Snowflake Token="{pat}"',
    "Content-Type":  "application/json",
    "Accept":        "application/json",
}

_FV_BODY_BASE = {
    "name":             str(fv.name),
    "version":          str(fv.version),
    "object_type":      "feature_view",
    "metadata_options": {"include_names": True, "include_data_types": True},
}
keys = [f"user_{i}" for i in range(N_KEYS)]
print(f"FV body base: {json.dumps(_FV_BODY_BASE)}")
print(f"PAT prefix  : {pat[:12]}...")

_perf = time.perf_counter

with httpx.Client(http2=True, headers=_headers, timeout=30.0) as _client:

    _probe_body = {**_FV_BODY_BASE, "request_rows": [{"entity": {JOIN_KEY: keys[0]}}]}
    print(f"DEBUG probe URL  : {_query_url}")
    print(f"DEBUG probe body : {json.dumps(_probe_body)}")
    _probe = _client.post(_query_url, json=_probe_body)
    print(f"DEBUG probe status: {_probe.status_code}")
    print(f"DEBUG probe resp  : {_probe.text[:500]}")
    _probe.raise_for_status()

    print(f"Warming up ({N_WARMUP} throwaway reads, HTTP/2) ...")
    _t_wu = _perf()
    for _i in range(N_WARMUP):
        _body = {**_FV_BODY_BASE, "request_rows": [{"entity": {JOIN_KEY: keys[_i % N_KEYS]}}]}
        _resp = _client.post(_query_url, json=_body)
        _resp.raise_for_status()
    print(f"Warm-up complete in {(_perf() - _t_wu)*1e3:.1f} ms")

    _lat  = [0.0] * N_MEASURE
    _loop_start = _perf()
    for _i in range(N_MEASURE):
        _k    = keys[_i % N_KEYS]
        _t0   = _perf()
        _resp = _client.post(_query_url, json={**_FV_BODY_BASE, "request_rows": [{"entity": {JOIN_KEY: _k}}]})
        _resp.raise_for_status()
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
_env_tag = "SPCS_HTTP2"

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
