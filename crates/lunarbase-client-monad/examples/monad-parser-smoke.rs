use futures_util::StreamExt;
use lunarbase_client_core::{ChainEventSource, ContractFilter};
use lunarbase_client_monad::{MonadParserConfig, MonadParserSource, MonadRpcCanonicalBackend};
use lunarbase_math::Address;
use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ws_url = env::var("LUNARBASE_MONAD_PARSER_WS")
        .unwrap_or_else(|_| "ws://127.0.0.1:8080/ws/subscriptions".into());
    let core = Address::from_hex(&env::var("LUNARBASE_CORE")?)?;
    let chain_id = env::var("LUNARBASE_CHAIN_ID")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(143);
    let config = MonadParserConfig {
        ws_url,
        core,
        chain_id,
        ..Default::default()
    };
    let rpc_url =
        env::var("LUNARBASE_MONAD_RPC").unwrap_or_else(|_| "http://127.0.0.1:8545".into());
    let canonical = Arc::new(MonadRpcCanonicalBackend::new(rpc_url, chain_id));
    let source = MonadParserSource::new(config, canonical)?;
    let filter = ContractFilter {
        address: core,
        topics: Vec::new(),
    };
    let mut stream = source.subscribe(filter).await?;
    while let Some(update) = stream.next().await {
        println!("{:#?}", update?);
    }
    Ok(())
}
