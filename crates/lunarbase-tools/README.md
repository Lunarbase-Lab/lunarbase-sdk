# `lunarbase-tools`

Validation and load-test commands for LunarBase SDK development.

## Use

Run from the project root:

```bash
make test-process-e2e
make load
make performance-baseline
make performance-capture PERF_OUTPUT=/tmp/lunarbase-performance-baseline
make performance-gate \
  PERF_BASELINE=/tmp/lunarbase-performance-baseline \
  PERF_CURRENT=/tmp/lunarbase-performance-current
make monad-live-validate
```

- `lunarbase-e2e` validates process startup, dependencies, replicas, and recovery.
  Its managed Redis scenario kills and restarts both the event worker and Redis,
  verifies AOF replay, inclusive-backfill deduplication, and consumer-group
  pending-entry reclamation.
- `lunarbase-load` reports quote throughput and latency percentiles.
- `lunarbase-quote-bench` measures the real connected-client hot path against
  deterministic, fully available in-memory state. `make performance-baseline`
  runs 15/64 lanes and batch sizes 1/16/256. Timing uses 128 readers; allocation
  counting is a separate single-threaded build so instrumentation cannot skew
  the latency baseline. Mixed mode applies quote-critical events through the
  real source/reducer while those readers quote.
- `lunarbase-monad-validate` validates Monad source, RPC, and indexer behavior.

These commands may open local ports and start subprocesses.

Performance reports use a versioned JSON schema and contain no timestamp. Keep
baseline output as a CI artifact from the same pinned machine and release
profile; absolute timings from different hosts are not comparable. Capture uses
10 timing/mixed samples per scenario. The gate compares medians, permits at most
3% quote-throughput, 5% p99, and 5% peak-RSS regression, and permits no allocation
growth. A mixed sample is invalid unless the reducer applies every published
update.
