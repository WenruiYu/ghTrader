# Data System Optimization Report

## Executive Summary

The ghTrader data system uses QuestDB for storage, TqSdk Pro for ingestion, and validation pipelines. The architecture is solid but has optimization opportunities in ingestion throughput, validation efficiency, and query performance.

**Current State:**
- ✅ QuestDB as canonical store with proper partitioning and WAL
- ✅ Idempotent ingestion via DEDUP UPSERT KEYS
- ✅ Comprehensive validation (coverage + field quality)
- ⚠️ Sequential day-by-day ingestion (no parallelization)
- ⚠️ Single DataFrame per day ILP writes (no batching)
- ⚠️ Validation runs separately from ingestion

**Key Metrics:**
- Ingestion: ~1 day at a time, sequential
- Storage: QuestDB partitioned by day, WAL enabled
- Validation: Two-phase (main_l5 coverage + field quality)

---

## 1. Data Ingestion Optimization

### 1.1 Current Implementation Analysis

**Location:** `src/ghtrader/tq/ingest.py`

**Current Flow:**
1. Sequential processing: one trading day at a time
2. Single TqApi connection reused across days
3. Per-day DataFrame → single ILP write
4. Retry logic with exponential backoff
5. Maintenance detection with long wait (2100s)

**Performance Characteristics:**
- **Latency per day:** ~5-30 seconds (depends on tick volume)
- **Throughput:** Limited by sequential processing
- **Bottleneck:** TqSdk API rate limits + sequential writes

### 1.2 Optimization Opportunities

**Priority 1: Parallel Day Processing**

**Current Code (lines 173-304):**
```python
for idx, day in enumerate(unique_days, start=1):
    # Process one day at a time
    df = api.get_tick_data_series(...)
    backend.ingest_df(table=_QUESTDB_TICKS_MAIN_L5_TABLE, df=out)
```

**Recommendation:**
- Use `concurrent.futures.ThreadPoolExecutor` or `multiprocessing.Pool` for parallel day downloads
- Limit concurrency to avoid TqSdk rate limits (suggest 3-5 parallel workers)
- Maintain separate TqApi instances per worker thread
- Aggregate results and write manifests after completion

**Expected Impact:**
- **3-5x throughput improvement** for multi-day ranges
- Better CPU utilization during I/O waits

**Implementation:**
```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def _download_day_worker(day, underlying_symbol, ...):
    # Create per-thread TqApi instance
    api = TqApi(auth=auth)
    try:
        # Download and return DataFrame
        return download_single_day(api, day, ...)
    finally:
        api.close()

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(_download_day_worker, day, ...): day 
               for day in unique_days}
    for future in as_completed(futures):
        df = future.result()
        backend.ingest_df(table=..., df=df)
```

**Priority 2: Batch ILP Writes**

**Current Code (line 282):**
```python
backend.ingest_df(table=_QUESTDB_TICKS_MAIN_L5_TABLE, df=out)
```

**Issue:** Each day triggers a separate ILP connection and flush

**Recommendation:**
- Accumulate multiple days into larger DataFrames (e.g., 5-10 days)
- Use a single ILP sender context for batch writes
- Flush after each batch, not per day

**Expected Impact:**
- **20-30% reduction** in ILP overhead
- Fewer connection establishments

**Priority 3: Connection Pooling**

**Current Code (lines 140-175 in `serving_db.py`):**
```python
def ingest_df(self, *, table: str, df: pd.DataFrame) -> None:
    Sender = self._sender()
    # Creates new connection each time
    with sender_ctx as sender:
        sender.dataframe(df, table_name=table, at="ts")
```

**Recommendation:**
- Implement a connection pool for ILP senders
- Reuse sender contexts across multiple DataFrame writes
- Use context managers for lifecycle management

**Expected Impact:**
- **10-15% reduction** in connection overhead

---

## 2. Storage Optimization

### 2.1 QuestDB Configuration Analysis

**Current Configuration:** `infra/questdb/docker-compose.yml`

**Strengths:**
- ✅ High worker counts (256 shared, 96 ILP writers)
- ✅ WAL enabled for durability
- ✅ Proper partitioning (DAY)
- ✅ DEDUP UPSERT KEYS for idempotency

**Optimization Opportunities:**

**Priority 1: O3 Commit Lag Tuning**

**Current:** `QDB_CAIRO_O3_COMMIT_LAG=300000000` (5 minutes)

**Analysis:** For historical ingestion (backfill), this is conservative

**Recommendation:**
- Increase to `600000000` (10 minutes) for bulk ingestion
- Add environment variable to toggle between ingestion mode (10min) and live mode (5min)
- Document trade-off: longer lag = better throughput, but delayed visibility

**Priority 2: Symbol Column Indexing**

**Current:** Symbol is SYMBOL type (efficient), but no explicit index

**Recommendation:**
- QuestDB SYMBOL columns are already optimized, but verify query patterns
- Consider adding explicit indexes if multi-symbol queries are common
- Monitor query performance via QuestDB metrics

**Priority 3: Partition Pruning Optimization**

**Current:** Queries use `trading_day` filtering

**Analysis:** `trading_day` is SYMBOL, which is efficient, but ensure queries leverage partition pruning

**Recommendation:**
- Verify queries use `ts` (TIMESTAMP) for time-range filtering when possible
- QuestDB automatically prunes partitions based on `ts` column
- Add query hints if needed: `SELECT ... WHERE ts >= '2024-01-01' AND ts < '2024-01-02'`

---

## 3. Validation Optimization

### 3.1 Current Validation Architecture

**Two-Phase Validation:**
1. **Main L5 Coverage Validation** (`main_l5_validate.py`)
   - Detects missing segments, gaps, cadence violations
   - Stores summary + gap detail tables
   - Can probe TqSdk for source attribution (slow)

2. **Field Quality Validation** (`field_quality.py`)
   - Computes null rates, outliers, violations
   - Stores per-day quality metrics

**Performance Characteristics:**
- Coverage validation: **O(n)** scan per trading day
- Field quality: **O(n)** scan per trading day
- TqSdk probing: **Very slow** (disabled by default)

### 3.2 Optimization Opportunities

**Priority 1: Incremental Validation**

**Current:** Full re-validation on each run

**Recommendation:**
- Track last validated trading_day per symbol
- Only validate new days since last run
- Store validation checkpoint in QuestDB metadata table

**Expected Impact:**
- **10-100x faster** for incremental updates
- Critical for daily production runs

**Implementation:**
```python
def get_last_validated_day(cfg, symbol):
    # Query QuestDB for last validated trading_day
    sql = """
    SELECT max(cast(trading_day as string)) 
    FROM ghtrader_main_l5_validate_summary_v2 
    WHERE symbol=%s
    """
    # Return date or None

def validate_incremental(symbol, start_day, end_day):
    last_validated = get_last_validated_day(cfg, symbol)
    if last_validated:
        start_day = max(start_day, last_validated + timedelta(days=1))
    # Only validate new days
```

**Priority 2: Parallel Validation**

**Current:** Sequential day-by-day validation

**Recommendation:**
- Use `ThreadPoolExecutor` to validate multiple days in parallel
- Limit concurrency to avoid QuestDB connection exhaustion
- Batch PGWire writes for validation results

**Expected Impact:**
- **3-5x faster** validation for multi-day ranges

**Priority 3: Sampling for Large Days**

**Current:** Validates every tick

**Recommendation:**
- For days with >1M ticks, use statistical sampling
- Validate full coverage but sample field quality checks
- Maintain accuracy while reducing compute

**Expected Impact:**
- **50-80% faster** for high-volume days

**Priority 4: Integrated Validation**

**Current:** Validation runs as separate CLI command

**Recommendation:**
- Add optional validation step after ingestion
- Make it configurable (always/never/on-error)
- Stream validation results to QuestDB as ingestion progresses

**Expected Impact:**
- **Eliminates separate validation pass**
- Faster feedback on data quality issues

---

## 4. Query Performance Optimization

### 4.1 Current Query Patterns

**Location:** `src/ghtrader/questdb/queries.py`

**Key Queries:**
1. `query_symbol_day_bounds` - Coverage queries
2. `fetch_ticks_for_symbol_day` - Day-level tick retrieval
3. `query_symbol_latest` - Freshness checks

**Performance Characteristics:**
- Uses thread-local connection pooling (`_get_thread_conn`)
- Proper use of partition pruning via `trading_day` filtering
- LIMIT clauses for safety

### 4.2 Optimization Opportunities

**Priority 1: Query Result Caching**

**Current:** Every coverage query hits QuestDB

**Recommendation:**
- Cache symbol coverage results (first_day, last_day, n_days)
- Invalidate cache on new ingestion
- Use Redis or in-memory cache with TTL

**Expected Impact:**
- **100-1000x faster** for repeated coverage queries
- Critical for dashboard UI responsiveness

**Priority 2: Prepared Statements**

**Current:** Dynamic SQL construction

**Recommendation:**
- Use psycopg prepared statements for common queries
- Reduces query parsing overhead

**Expected Impact:**
- **5-10% faster** for repeated query patterns

**Priority 3: Batch Symbol Queries**

**Current:** Some queries process symbols one-by-one

**Recommendation:**
- Use `IN (...)` clauses for multi-symbol queries
- Leverage QuestDB's parallel query execution

**Expected Impact:**
- **2-3x faster** for multi-symbol operations

---

## 5. Data Lineage & Reproducibility

### 5.1 Current Manifest System

**Location:** `src/ghtrader/data/manifest.py`

**Strengths:**
- ✅ Lightweight JSON manifests
- ✅ Includes schema hash, code version, row counts
- ✅ Atomic writes via `write_json_atomic`

**Optimization Opportunities:**

**Priority 1: QuestDB-Stored Manifests**

**Current:** File-based manifests in `data/manifests/`

**Recommendation:**
- Store manifests in QuestDB table for queryability
- Keep file-based manifests for backward compatibility
- Enable SQL queries like "which manifests cover date X?"

**Expected Impact:**
- Better lineage tracking
- Enables automated data quality reports

**Priority 2: Manifest Aggregation**

**Current:** One manifest per ingestion run

**Recommendation:**
- Aggregate manifests for multi-day ranges
- Create "coverage reports" that summarize multiple manifests
- Store in QuestDB for dashboard queries

---

## 6. Monitoring & Observability

### 6.1 Current State

**Strengths:**
- ✅ Structured logging via `structlog`
- ✅ Progress reporting every N days or seconds
- ✅ QuestDB Prometheus metrics available

**Gaps:**
- No ingestion rate metrics (rows/sec, MB/sec)
- No validation performance tracking
- No query latency histograms

### 6.2 Recommendations

**Priority 1: Ingestion Metrics**

**Recommendation:**
- Track ingestion rate (rows/sec) per symbol/day
- Log to structured logs + optional Prometheus export
- Alert on ingestion rate drops

**Priority 2: Validation Metrics**

**Recommendation:**
- Track validation duration per day
- Track gap counts, quality scores over time
- Store metrics in QuestDB for trend analysis

**Priority 3: Query Performance Dashboard**

**Recommendation:**
- Add query latency tracking to `queries.py`
- Export to Prometheus or QuestDB metrics table
- Create dashboard showing P50/P95/P99 latencies

---

## 7. Implementation Priority Matrix

| Optimization | Impact | Effort | Priority | Estimated Speedup |
|-------------|--------|--------|----------|-------------------|
| Parallel day ingestion | High | Medium | P0 | 3-5x |
| Incremental validation | High | Low | P0 | 10-100x |
| Batch ILP writes | Medium | Low | P1 | 1.2-1.3x |
| Query result caching | High | Medium | P1 | 100-1000x |
| Parallel validation | Medium | Medium | P1 | 3-5x |
| Connection pooling | Low | Medium | P2 | 1.1-1.15x |
| O3 commit lag tuning | Low | Low | P2 | 1.1-1.2x |
| Prepared statements | Low | Low | P2 | 1.05-1.1x |

---

## 8. Recommended Implementation Plan

**Phase 1 (Week 1-2): Quick Wins**
1. Implement incremental validation checkpointing
2. Add query result caching for coverage queries
3. Tune O3 commit lag for bulk ingestion

**Phase 2 (Week 3-4): Throughput Improvements**
1. Implement parallel day ingestion (3-5 workers)
2. Add batch ILP writes (accumulate 5-10 days)
3. Parallel validation execution

**Phase 3 (Week 5-6): Advanced Optimizations**
1. Connection pooling for ILP senders
2. QuestDB-stored manifests
3. Monitoring & metrics dashboard

---

## 9. Risk Assessment

**Low Risk:**
- Incremental validation (backward compatible)
- Query caching (can be disabled)
- O3 commit lag tuning (environment variable)

**Medium Risk:**
- Parallel ingestion (requires testing TqSdk rate limits)
- Batch ILP writes (need to handle partial failures)

**Mitigation:**
- Feature flags for all optimizations
- Gradual rollout (test on single symbol first)
- Comprehensive testing before production deployment

---

## 10. Success Metrics

**Ingestion Performance:**
- Target: **5x faster** multi-day ingestion (3-5 parallel workers)
- Measure: Days ingested per hour

**Validation Performance:**
- Target: **50x faster** incremental validation
- Measure: Validation time for 30-day range

**Query Performance:**
- Target: **<10ms** P95 latency for coverage queries (with caching)
- Measure: Query latency percentiles

**Storage Efficiency:**
- Target: **No degradation** in query performance
- Measure: Query latency before/after optimizations

---

## Conclusion

The data system is well-architected with QuestDB as the canonical store. The main opportunities are:

1. **Parallelization** - Unlock multi-core performance for ingestion and validation
2. **Incremental Processing** - Avoid redundant work on already-processed data
3. **Caching** - Reduce database load for frequently-accessed metadata

Implementing these optimizations should yield **5-10x overall performance improvement** while maintaining data integrity and reproducibility.

**Next Steps:**
1. Review and approve optimization priorities
2. Create feature branches for each optimization
3. Implement with feature flags for gradual rollout
4. Benchmark before/after performance
5. Document configuration changes

---

*Report generated: 2026-01-24*  
*Based on codebase analysis of ghTrader data system*