# Benchmark Results & Findings

> **Disclaimer:** Actual results may vary based on your workload, configuration, and region; comparisons are for illustration only and do not guarantee performance in any specific environment.

Latency results from benchmarking Snowflake ML Feature Store online serving
across two online store backends, using SPCS job-based execution for the
lowest and most consistent numbers.

---

## 1. Hybrid Table-Backed Online Feature Tables (Generally Available)

### Test Configuration

| Parameter | Value |
|-----------|-------|
| Data size | 100,000 rows |
| Features per row | 10 (`FEATURE_01` through `FEATURE_10`) |
| Join key | Single key (`CUSTOMER_ID`) |
| Warehouse | `FS_BENCHMARK_WH` (XS, `AUTO_SUSPEND=600`) |
| FeatureView | `CUSTOMER_FEATURES/v1` with `OnlineConfig(enable=True)` |
| Online table | `CUSTOMER_FEATURES$v1$ONLINE` (Hybrid Table) |
| Compute pool | `FS_BENCH_JOB_POOL` (CPU_X64_SL, 1 node) |
| Threads | 8 (multi-cursor on shared SPCS session) |
| Warmup | 600 seconds (discarded) |
| Measurement | 300 seconds |
| snowflake-ml-python | 1.37.0 |
| Execution method | `snowflake.ml.jobs.submit_directory()` headless SPCS job |

### Retrieval Methods

**SDK** — `read_feature_view()`: The official Feature Store API. Issues the online
read and returns a DataFrame in one call:

```python
fs = FeatureStore(session=session, database=DB, name=SCHEMA,
                  default_warehouse=WH, creation_mode=CreationMode.FAIL_IF_NOT_EXIST)
fv = fs.get_feature_view(name="CUSTOMER_FEATURES", version="v1")
result = fs.read_feature_view(fv, keys=[[customer_id]], store_type=StoreType.ONLINE).collect()
```

**Direct SQL** — `cursor.execute()`: Bypasses the Snowpark SDK entirely. Issues the
same query through the low-level `snowflake-connector-python` cursor:

```python
cursor.execute(
    'SELECT "CUSTOMER_ID","FEATURE_01",...,"FEATURE_10","UPDATED_AT" '
    'FROM FS_BENCHMARK_DB.FS_BENCHMARK_FS."CUSTOMER_FEATURES$v1$ONLINE" '
    'WHERE "CUSTOMER_ID" = ?',
    (customer_id,)
)
rows = cursor.fetchall()
```

> **Note:** Inside SPCS, the connection uses `qmark` paramstyle (`?` placeholders)
> instead of `pyformat` (`%s`).

### Results

#### Direct SQL — 8 Threads, SL Compute Pool

| Metric | Value |
|--------|-------|
| **QPS** | **169.2** |
| Min (ms) | 28.60 |
| **P50 (ms)** | **43.78** |
| P90 (ms) | 61.85 |
| P95 (ms) | 70.04 |
| P99 (ms) | 90.33 |
| Mean (ms) | 47.27 |
| Stdev (ms) | 13.80 |
| Max (ms) | 826.17 |
| Total requests | 50,771 |

#### SDK — 8 Threads, SL Compute Pool

| Metric | Value |
|--------|-------|
| **QPS** | **159.2** |
| Min (ms) | 28.78 |
| **P50 (ms)** | **47.34** |
| P90 (ms) | 64.74 |
| P95 (ms) | 72.79 |
| P99 (ms) | 93.04 |
| Mean (ms) | 50.25 |
| Stdev (ms) | 15.43 |
| Max (ms) | 779.90 |
| Total requests | 47,763 |

#### Side-by-Side Comparison

| Metric | Direct SQL | SDK | Difference |
|--------|-----------|-----|------------|
| p50 (ms) | 43.78 | 47.34 | +3.56ms (+8%) |
| p95 (ms) | 70.04 | 72.79 | +2.75ms (+4%) |
| p99 (ms) | 90.33 | 93.04 | +2.71ms (+3%) |
| QPS | 169.2 | 159.2 | -5.9% |
| Total requests | 50,771 | 47,763 | -5.9% |

### Key Findings

**1. SDK overhead is negligible on SPCS with SL compute pool.**
The SDK adds only ~3.5ms to p50 latency (47.34ms vs 43.78ms). On the SL compute
pool running as a headless SPCS job, the Snowpark DataFrame construction and SQL
compilation overhead is effectively eliminated. Production applications can use
`read_feature_view()` without meaningful latency penalty.

**2. Sub-50ms p50 latency is achievable.**
Both methods achieve sub-50ms median latency for single-point lookups against 100K
rows with 10 features. The Hybrid Table point-lookup execution time floors at
approximately 29ms (observed min).

**3. P99 stays under 100ms.**
Both Direct SQL (90.33ms) and SDK (93.04ms) maintain sub-100ms p99, suitable for
real-time serving SLAs requiring consistent tail latency.

**4. 600s warmup validates Hybrid Table best practice.**
With 600 seconds of warmup on 100K rows, the Hybrid Table caches are fully primed.
The resulting latency distribution is tight (stdev ~14-15ms) with no warmup-related
outliers.

**5. 8 threads on SL pool deliver ~160-170 QPS.**
A single SL compute pool node with 8 concurrent threads achieves 159-169 QPS on an
XS warehouse, without any additional scaling.

**6. Warehouse size does not matter for point lookups.**
XS warehouses deliver the same point-lookup latency as larger sizes. Hybrid Table
lookups are handled by the Unistore execution engine, not the warehouse compute
layer. Set `AUTO_SUSPEND = 600` to prevent cache loss between runs.

**7. Headless SPCS jobs eliminate notebook overhead.**
Running benchmarks as headless jobs via `submit_directory()` eliminates the
Snowsight UI and notebook runtime overhead, producing the cleanest and most
reproducible results.

### Reproducing These Results

```bash
pip install snowflake-ml-python==1.37.0

export SNOWFLAKE_CONNECTION_NAME=<your-connection>
export SNOWFLAKE_PAT='<your-programmatic-access-token>'
export SNOWFLAKE_USER='<your-username>'

python latency_hybrid_table/setup_env.py
python latency_hybrid_table/submit_job_direct_sql.py --wait --logs   # Direct SQL
python latency_hybrid_table/submit_job_sdk.py --wait --logs        # SDK
```

Results: `FS_BENCHMARK_DB.FS_BENCHMARK_SCHEMA.FS_BENCH_JOB_RESULTS_TBL`
Raw JSON: `@FS_BENCHMARK_DB.FS_BENCHMARK_SCHEMA.FS_BENCH_JOB_RESULTS`

---

## 2. Postgres-Backed Online Feature Tables (Private Preview)

### Test Configuration

| Parameter | Value |
|-----------|-------|
| Account | Snowflake account with Postgres Online Service Private Preview enabled |
| Data size | 100,000 rows |
| Features per row | 5 float columns (`F1` through `F5`) |
| Join key | Single key (`USER_ID`, format: `user_0` to `user_999`) |
| Warehouse | `DEMO_WH` |
| FeatureView | `BENCHMARK_USER_FEATURES/V1` with Postgres online store |
| Online store | Postgres Online Service (`OnlineStoreType.POSTGRES`, `target_lag="10s"`) |
| Compute pool | `FS_LAT_POOL` (HIGHMEM_X64_S, 1 node) |
| Threads | 1 (single-threaded) |
| Warmup | 100 reads (count-based, discarded) |
| Measurement | 5,000 reads (count-based) |
| Key pool | 1,000 rotating keys |
| snowflake-ml-python | 1.37.0 |
| Execution method | `snowflake.ml.jobs.submit_directory()` headless SPCS job |

### Retrieval Methods

**SDK** (`run_benchmark_sdk.py`, ENV tag: `SPCS`):

The official Feature Store API:

```python
fs.read_feature_view(fv, keys=[[key]], store_type=StoreType.ONLINE)
```

For Postgres online reads, the SDK internally uses an HTTP/2 REST client
(`OnlineServiceHttpClient` backed by `httpx.Client(http2=True)`) against
the Online Service `/api/v1/query` endpoint, and returns a pandas DataFrame
directly — no warehouse SQL round-trip. The latency profile is effectively
identical to the manual REST benchmark.

**REST — Direct HTTP/2** (`run_benchmark_rest.py`, ENV tag: `SPCS_HTTP2`):

Opens **one persistent `httpx` HTTP/2 connection** before the measurement loop and
reuses it for all 5,000 reads:

```python
with httpx.Client(http2=True, headers=_headers, timeout=30.0) as client:
    for i in range(N_MEASURE):
        key = keys[i % N_KEYS]
        t0 = time.perf_counter()
        resp = client.post(query_url, json={**FV_BODY_BASE,
                    "request_rows": [{"entity": {JOIN_KEY: key}}]})
        resp.raise_for_status()
        latencies[i] = time.perf_counter() - t0
```

Why it's fast:
1. **HTTP/2 persistent connection**: TCP + TLS negotiation happens exactly once during
   warmup. Every subsequent request reuses the open connection.
2. **Pre-built request body**: Static fields are constructed once; the hot path only
   merges the per-request key.

This manual REST benchmark serves as a baseline confirming what the SDK achieves
under the hood. Production applications can use the SDK's `read_feature_view()` API
directly and see equivalent latency.

Online Service request format:

```json
{
  "name": "BENCHMARK_USER_FEATURES",
  "version": "V1",
  "object_type": "feature_view",
  "metadata_options": {"include_names": true, "include_data_types": true},
  "request_rows": [{"entity": {"USER_ID": "user_42"}}]
}
```

Endpoint: `{online_service_base_url}/api/v1/query`
Auth: `Authorization: Snowflake Token="{pat}"`
HTTP client: `httpx` with `httpx[http2]` (Python `h2` HTTP/2 framing library).

### Results

| Metric | SDK | REST (Direct HTTP/2) |
|--------|-----|---------------------|
| Min (ms) | 8.9 | 8.6 |
| **P50 (ms)** | **10.9** | **10.5** |
| P90 (ms) | 12.5 | 12.3 |
| **P99 (ms)** | **16.3** | **18.6** |
| Mean (ms) | 11.3 | 11.1 |

Both runs: N=5,000 reads, 100 warmup reads, 1,000 rotating keys, HIGHMEM_X64_S node.

**The SDK and Direct REST produce effectively identical latency.** This is
expected: the Snowflake ML Python SDK uses an internal HTTP/2 REST client for
Postgres online reads under the hood, and returns a pandas DataFrame directly
from the Online Service `/api/v1/query` response. The manual REST benchmark
exists to confirm and demonstrate the underlying transport.

### Key Findings

**8. SDK and REST achieve equivalent low-latency on Postgres OFT.**
The SDK (p50=10.9ms, p99=16.3ms) and Direct REST (p50=10.5ms, p99=18.6ms) are
effectively indistinguishable. The SDK uses an HTTP/2 REST client internally for
Postgres online reads, so there is no meaningful overhead from calling
`read_feature_view()` instead of hitting the endpoint directly.

**9. Sub-20ms p99 is achievable with either method.**
Both approaches achieve sub-20ms p99 — well within real-time serving SLAs. The
tight distribution indicates stable, predictable latency.

**10. The SDK is the recommended API for Postgres online reads.**
Because the SDK already uses the optimal HTTP/2 REST transport under the hood,
applications should prefer `read_feature_view()` for its cleaner surface, automatic
endpoint discovery, and integration with the rest of the Feature Store API. Drop
down to the manual REST benchmark only when you need a baseline measurement.

**11. Headless mode eliminates 50-200ms of notebook jitter on P99.**
Interactive notebooks (Snowsight, Jupyter) introduce kernel round-trip overhead
(~5-30ms), output rendering, and GIL jitter from background threads. These add
50-200ms of artificial latency on P99, making it impossible to distinguish real
backend latency from notebook overhead. Headless SPCS jobs eliminate all of this.

### Infrastructure Requirements (Postgres REST endpoint access)

Both the SDK and REST benchmarks hit the Online Service HTTP endpoint. From an
SPCS container, two pieces of infrastructure are required:

**1. External Access Integration for the Online Service domain:**

```sql
CREATE OR REPLACE NETWORK RULE <db>.<schema>.FS_LAT_ONLINE_SVC_RULE
    TYPE = HOST_PORT MODE = EGRESS
    VALUE_LIST = ('*.snowflakecomputing.app');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION FS_LAT_ONLINE_SVC_EAI
    ALLOWED_NETWORK_RULES = (<db>.<schema>.FS_LAT_ONLINE_SVC_RULE)
    ENABLED = TRUE;
```

**2. User-level network policy for PAT auth from SPCS:**

SPCS containers exit via a public IP not in the corporate VPN allowlist. The Online
Service only accepts PAT tokens (session tokens are rejected with 403). A user-level
policy overrides the account-level policy:

```sql
CREATE OR REPLACE NETWORK POLICY <user>_SPCS_POLICY
    ALLOWED_IP_LIST = ('0.0.0.0/0')
    COMMENT = 'Allow all IPs for SPCS demo user';

ALTER USER <username> SET NETWORK_POLICY = <user>_SPCS_POLICY;
```

### Reproducing These Results

```bash
export SNOWFLAKE_PAT='<your-programmatic-access-token>'
export SNOWFLAKE_USER='<your-username>'

# One-time setup (compute pool, EAI, source data, Feature View, online store)
python latency_postgres/setup_env.py

# SDK benchmark
python latency_postgres/submit_job_sdk.py --logs

# REST (Direct HTTP/2) benchmark
python latency_postgres/submit_job_rest.py --logs
```

Results: `RTFS_DEMO_DB.OFT_DEMO.BENCHMARK_RESULTS`
Raw JSON: `@RTFS_DEMO_DB.OFT_DEMO.BENCHMARK_RESULTS_STAGE`

```sql
SELECT ENV, ROUND(AVG(P50_MS), 2) AS p50_ms, ROUND(AVG(P90_MS), 2) AS p90_ms,
       ROUND(AVG(P99_MS), 2) AS p99_ms, ROUND(AVG(MEAN_MS), 2) AS mean_ms, COUNT(*) AS runs
FROM RTFS_DEMO_DB.OFT_DEMO.BENCHMARK_RESULTS
GROUP BY ENV ORDER BY p50_ms;
```

---

## 3. Cross-Backend Comparison

| Aspect | Hybrid Table (Generally Available) | Postgres Online Service (Private Preview) |
|--------|-------------------|-------------------------------|
| **Best p50** | 43.78ms (Direct SQL, 8T) | 10.5ms (Direct REST, 1T) |
| **Best p99** | 90.33ms (Direct SQL, 8T) | 18.6ms (Direct REST, 1T) |
| **SDK p50** | 47.34ms (8T, SL pool) | 10.9ms (1T) |
| **SDK overhead** | ~3.5ms (negligible) | ~0ms (SDK uses HTTP/2 REST transport internally) |
| **Optimal retrieval** | SDK or Direct SQL (both work well) | SDK or Direct REST (equivalent latency) |
| **Throughput** | ~170 QPS (8 threads) | ~475 QPS (single-threaded, extrapolated from 10.5ms/req) |
| **Warmup** | 600s time-based | 100 count-based |
| **Data model** | Hybrid Table (`$ONLINE` table) | Postgres (Online Service) |
| **Online store config** | `OnlineConfig(enable=True)` | `OnlineConfig(enable=True, target_lag="10s", store_type=OnlineStoreType.POSTGRES)` |

The Postgres Online Service delivers significantly lower raw latency (10.5ms vs
43.78ms p50) through its dedicated serving layer with persistent HTTP/2 connections.
The Hybrid Table approach benefits from simpler infrastructure (no separate online
service to provision) and negligible SDK overhead.

## Recommendations

| Use Case | Backend | Method | Expected p50 |
|----------|---------|--------|--------------|
| Lowest latency, Postgres available | Postgres | SDK (`read_feature_view`) or Direct REST | ~10-11ms |
| Production ML inference on SPCS | Hybrid Table | SDK (`read_feature_view`) | ~47ms |
| Lowest latency on Hybrid Table | Hybrid Table | Direct SQL (`cursor.execute`) | ~44ms |
| Higher throughput (HT) | Hybrid Table | Increase threads / multi-cluster WH | ~44-70ms |
