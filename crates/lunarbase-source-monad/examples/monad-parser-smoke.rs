//! Connects to a portable Monad parser and prints normalized execution updates.

use futures_util::StreamExt;
use lunarbase_client::prelude::{ChainDataSource, ContractFilter};
use lunarbase_math::prelude::Address;
use lunarbase_source_monad::prelude::{MonadParserConfig, MonadParserSource};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ws_url = env::var("LUNARBASE_MONAD_PARSER_WS")
        .unwrap_or_else(|_| MonadParserConfig::default().ws_url);
    let core = env::var("LUNARBASE_CORE")?.parse::<Address>()?;
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
    let source = MonadParserSource::new(config, rpc_url)?;
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
