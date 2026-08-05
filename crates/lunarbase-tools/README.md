# `lunarbase-tools`

Validation and load-test commands for LunarBase SDK development.

## Use

Run from the project root:

```bash
make test-process-e2e
make load
make monad-live-validate
```

- `lunarbase-e2e` validates process startup, dependencies, replicas, and recovery.
- `lunarbase-load` reports quote throughput and latency percentiles.
- `lunarbase-monad-validate` validates Monad source, RPC, and indexer behavior.

These commands may open local ports and start subprocesses.
