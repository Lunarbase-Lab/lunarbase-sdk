# `lunarbase-tools`

Internal process E2E, load, and live-validation binaries for LunarBase SDK.
This crate is not published.

## Commands

From the repository root:

```bash
make test-process-e2e
make load
make monad-live-validate
```

- `lunarbase-e2e` starts real indexer processes and test RPC, WebSocket, and
  Redis dependencies, including multi-replica and recovery scenarios.
- `lunarbase-load` benchmarks the configured lane and pair corpus and reports
  throughput and latency percentiles.
- `lunarbase-monad-validate` performs parser/RPC/indexer validation against a
  live Monad environment.

These tools may open ports and start subprocesses. They are validation
utilities, not runtime dependencies of client libraries.
