# `lunarbase-tools`

Validation and load-test commands for LunarBase SDK development.

## Use

Run from the project root:

```bash
make test-process-e2e
make load
make performance-baseline
make monad-live-validate
```

- `lunarbase-e2e` validates process startup, dependencies, replicas, and recovery.
- `lunarbase-load` reports quote throughput and latency percentiles.
- `lunarbase-quote-bench` measures the real connected-client hot path against
  deterministic, fully available in-memory state. `make performance-baseline`
  runs 15/64 lanes and batch sizes 1/16/256. Timing uses 128 readers; allocation
  counting is a separate single-threaded build so instrumentation cannot skew
  the latency baseline.
- `lunarbase-monad-validate` validates Monad source, RPC, and indexer behavior.

These commands may open local ports and start subprocesses.

Performance reports use a versioned JSON schema and contain no timestamp. Keep
baseline output as a CI artifact from the same pinned machine and release
profile; absolute timings from different hosts are not comparable.
