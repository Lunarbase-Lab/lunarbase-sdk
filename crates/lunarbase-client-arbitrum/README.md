# `lunarbase-client-arbitrum`

Experimental Arbitrum Nitro adapter for the common LunarBase runtime.

## Install and connect

```toml
[dependencies]
lunarbase-client-arbitrum = { git = "https://github.com/Lunarbase-Lab/lunarbase-sdk.git", rev = "<approved-revision>" }
```

```rust
let client = lunarbase_client_arbitrum::connect_arbitrum(
    config,
    optional_checkpoint,
)
.await?;
```

The adapter consumes executed logs and keeps the EVM execution-block context
separate from stream ordering metadata.

Do not mark this package production-ready until execution `block.number`
semantics, reconnect behavior, and canonical recovery have been validated
against a real Nitro node.
