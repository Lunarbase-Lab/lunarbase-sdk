# `lunarbase-client-monad`

Experimental Monad execution-events adapter for the common LunarBase runtime.

## Portable parser client

```toml
[dependencies]
lunarbase-client-monad = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
```

```rust
let client = lunarbase_client_monad::connect_monad_parser(
    config,
    optional_checkpoint,
)
.await?;
```

The portable implementation consumes the parser WebSocket and uses RPC for
bootstrap and canonical recovery.

## Native event ring

On Linux, enable the colocated shared-memory reader:

```toml
lunarbase-client-monad = {
  git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git",
  rev = "<approved-revision>",
  features = ["native-event-ring"]
}
```

Then use `connect_monad_event_ring`. The native feature depends on the official
Monad execution-events crates and is intended to run beside a Monad node.

This package remains experimental until parser and native-ring sequencing,
gap recovery, commitment transitions, and long-running node soak tests pass.
