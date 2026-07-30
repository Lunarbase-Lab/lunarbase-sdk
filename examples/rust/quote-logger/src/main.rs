//! Runs the embeddable EVM client and periodically logs offchain quotes.

use clap::{Parser, ValueEnum};
use dotenvy::from_path;
use lunarbase_client::model::MATH_COMPATIBILITY_VERSION;
use lunarbase_client::prelude::{
    ClientConnectConfig, ClientRuntimeEvent, Commitment, ConnectedQuoteClient, ContractFilter,
    DeploymentConfig, Network,
};
use lunarbase_client::protocol::abi::quote_critical_topics;
use lunarbase_client::protocol::proxy::{ERC1967_IMPLEMENTATION_SLOT, decode_implementation};
use lunarbase_math::prelude::{Address, QuoteMode, QuoteOutcome, QuoteRequest, U256};
use lunarbase_source_evm::prelude::{EvmRpcSource, RpcHttpClient};
use std::{error::Error, io, num::NonZeroU64, path::Path, str::FromStr, sync::Arc, time::Duration};
use tokio::{sync::broadcast, time::MissedTickBehavior};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;

const DEMO_ROUTER: &str = "0x000000000000000000000000000000000000dead";
type AnyError = Box<dyn Error + Send + Sync>;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum SourceProfile {
    /// Canonical EVM `logs + newHeads` subscriptions.
    Evm,
    /// Base `pendingLogs + newHeads` Flashblocks subscriptions.
    BaseFlashblocks,
}

impl SourceProfile {
    const fn network(self) -> Network {
        match self {
            Self::Evm => Network::Evm,
            Self::BaseFlashblocks => Network::Base,
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Streams LunarBase state and logs bit-exact offchain quotes")]
struct Args {
    /// Canonical HTTP JSON-RPC endpoint.
    #[arg(long, env = "RPC_URL")]
    rpc_url: Url,
    /// Deployed LunarBase Core address.
    #[arg(long, env = "CORE_ADDRESS")]
    core: Address,
    /// Realtime WebSocket endpoint; derived from RPC_URL when omitted.
    #[arg(long, env = "WS_URL")]
    ws_url: Option<Url>,
    /// Realtime subscription profile.
    #[arg(long, env = "SOURCE_PROFILE", value_enum, default_value = "evm")]
    source_profile: SourceProfile,
    /// Router whose whitelist and partner fee profile is quoted.
    #[arg(long, env = "ROUTER_ADDRESS")]
    router: Option<Address>,
    #[arg(long, env = "EXPECT_WHITELISTED", default_value_t = false)]
    expect_whitelisted: bool,
    #[arg(long, env = "DEPLOYMENT_BLOCK", default_value_t = 0)]
    deployment_block: u64,
    /// Explicit active lanes, comma-separated; avoids historical log discovery.
    #[arg(long, env = "LANE_ASSETS", value_delimiter = ',')]
    lane_assets: Vec<Address>,
    #[arg(long, env = "QUOTE_AMOUNT", default_value = "1000000000000000000")]
    quote_amount: U256,
    #[arg(long, env = "QUOTE_INTERVAL_SECONDS", default_value = "2")]
    quote_interval_seconds: NonZeroU64,
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    load_dotenv()?;
    init_tracing()?;

    let args = Args::parse();
    let ws_url = match args.ws_url {
        Some(url) => validate_ws_url(url)?,
        None => derive_ws_url(&args.rpc_url)?,
    };
    let rpc = RpcHttpClient::new(args.rpc_url.to_string())?;
    let chain_id = rpc.chain_id().await?;
    let head = rpc
        .block_cursor("latest", chain_id, Commitment::Canonical)
        .await?;
    let block_hash = head
        .block_hash
        .ok_or_else(|| io::Error::other("latest block has no hash"))?;
    let implementation = decode_implementation(
        rpc.get_storage_at_hash(args.core, ERC1967_IMPLEMENTATION_SLOT, block_hash)
            .await?,
    )
    .ok_or_else(|| io::Error::other("Core has an invalid ERC-1967 implementation"))?;
    let implementation_code_hash = rpc
        .runtime_code_hash_at_hash(implementation, block_hash)
        .await?;
    let uses_demo_router = args.router.is_none();
    let router = args
        .router
        .unwrap_or(Address::from_str(DEMO_ROUTER).expect("valid demo router"));
    let quote_interval = Duration::from_secs(args.quote_interval_seconds.get());

    if uses_demo_router {
        warn!(
            router = %format!("{router:#x}"),
            "ROUTER_ADDRESS is unset; using a non-whitelisted demonstration fee profile"
        );
    }

    let deployment = DeploymentConfig {
        network: args.source_profile.network(),
        chain_id,
        core: args.core,
        router,
        expect_whitelisted: args.expect_whitelisted,
        deployment_block: args.deployment_block,
        expected_implementation: implementation,
        expected_implementation_code_hash: implementation_code_hash,
        contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
        explicit_lane_assets: args.lane_assets,
    };
    let config = ClientConnectConfig {
        filter: ContractFilter {
            address: args.core,
            topics: quote_critical_topics().to_vec(),
        },
        deployment,
        buffer_capacity: 4096,
        reconnect_delay: Duration::from_secs(1),
        source_stall_timeout: Duration::from_secs(30),
    };
    info!(
        chain_id,
        core = %format!("{:#x}", args.core),
        router = %format!("{router:#x}"),
        source_profile = ?args.source_profile,
        rpc_ws = %ws_url,
        "connecting LunarBase EVM client"
    );
    let source = Arc::new(match args.source_profile {
        SourceProfile::Evm => {
            EvmRpcSource::new(rpc, ws_url.to_string(), Network::Evm, chain_id, "latest")
        }
        SourceProfile::BaseFlashblocks => {
            EvmRpcSource::base_flashblocks(rpc, ws_url.to_string(), chain_id)
        }
    });
    let client = ConnectedQuoteClient::connect(config, source, None).await?;
    client.await_ready(Commitment::Realtime).await?;

    let checkpoint = client
        .checkpoint()?
        .ok_or_else(|| io::Error::other("client returned no bootstrap checkpoint"))?;
    let cash = checkpoint.state.cash;
    let mut lanes = checkpoint
        .state
        .lanes
        .iter()
        .filter_map(|(asset, lane)| lane.exists().then_some(*asset))
        .collect::<Vec<_>>();
    lanes.sort_unstable();
    if lanes.is_empty() {
        client.shutdown().await;
        return Err(io::Error::other(
            "no active lanes discovered; verify CORE_ADDRESS, DEPLOYMENT_BLOCK, or LANE_ASSETS",
        )
        .into());
    }

    info!(
        cash = %format!("{cash:#x}"),
        lanes = lanes.len(),
        quote_amount = %args.quote_amount,
        "client ready"
    );
    run_quote_loop(&client, cash, &lanes, args.quote_amount, quote_interval).await;
    client.shutdown_gracefully(Duration::from_secs(10)).await?;
    info!("quote logger stopped");
    Ok(())
}

async fn run_quote_loop(
    client: &ConnectedQuoteClient,
    cash: Address,
    lanes: &[Address],
    amount: U256,
    interval_duration: Duration,
) {
    let requests = quote_requests(cash, lanes, amount);
    let mut interval = tokio::time::interval(interval_duration);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut runtime_events = client.subscribe_runtime_events();
    let mut events_open = true;

    loop {
        tokio::select! {
            _ = interval.tick() => log_quote_batch(client, &requests),
            event = runtime_events.recv(), if events_open => {
                events_open = log_runtime_event(event);
            }
            signal = tokio::signal::ctrl_c() => {
                if let Err(detail) = signal {
                    error!(%detail, "failed to listen for Ctrl+C");
                }
                info!("shutdown requested");
                break;
            }
        }
    }
}

fn quote_requests(cash: Address, lanes: &[Address], amount: U256) -> Vec<QuoteRequest> {
    let mut requests = Vec::with_capacity(lanes.len() * 2);
    for lane in lanes {
        requests.push(QuoteRequest {
            asset_in: *lane,
            asset_out: cash,
            amount,
            mode: QuoteMode::ExactIn,
        });
        requests.push(QuoteRequest {
            asset_in: cash,
            asset_out: *lane,
            amount,
            mode: QuoteMode::ExactIn,
        });
    }
    requests
}

fn log_quote_batch(client: &ConnectedQuoteClient, requests: &[QuoteRequest]) {
    let batch = match client.quote_many(requests) {
        Ok(batch) => batch,
        Err(detail) => {
            warn!(%detail, "quote batch unavailable");
            return;
        }
    };
    for (request, outcome) in requests.iter().zip(batch.outcomes.iter()) {
        match outcome {
            QuoteOutcome::Available(result) => info!(
                block = batch.execution_block_number,
                commitment = ?batch.cursor.commitment,
                asset_in = %format!("{:#x}", request.asset_in),
                asset_out = %format!("{:#x}", request.asset_out),
                amount_in = %result.amount_in,
                amount_out = %result.amount_out,
                fee_asset = %format!("{:#x}", result.fee_asset),
                fee_amount = %result.fee_amount,
                partner_fee = %result.partner_fee,
                treasury_fee = %result.treasury_fee,
                "quote"
            ),
            QuoteOutcome::Unavailable(reason) => warn!(
                block = batch.execution_block_number,
                asset_in = %format!("{:#x}", request.asset_in),
                asset_out = %format!("{:#x}", request.asset_out),
                ?reason,
                "quote unavailable"
            ),
        }
    }
}

fn log_runtime_event(event: Result<ClientRuntimeEvent, broadcast::error::RecvError>) -> bool {
    match event {
        Ok(event) => {
            let code = event.code();
            let detail = event.detail();
            warn!(code, %detail, "client runtime event");
            true
        }
        Err(broadcast::error::RecvError::Lagged(skipped)) => {
            warn!(skipped, "runtime event receiver lagged");
            true
        }
        Err(broadcast::error::RecvError::Closed) => {
            warn!("runtime event channel closed");
            false
        }
    }
}

fn load_dotenv() -> Result<(), dotenvy::Error> {
    let local = Path::new(env!("CARGO_MANIFEST_DIR")).join(".env");
    if local.exists() {
        from_path(local)?;
    } else {
        let _ = dotenvy::dotenv();
    }
    Ok(())
}

fn init_tracing() -> Result<(), AnyError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("lunarbase_quote_logger=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()?;
    Ok(())
}

fn derive_ws_url(rpc_url: &Url) -> Result<Url, AnyError> {
    let mut ws_url = rpc_url.clone();
    let scheme = match rpc_url.scheme() {
        "https" => "wss",
        "http" => "ws",
        _ => return Err(io::Error::other("RPC_URL must use http or https").into()),
    };
    ws_url
        .set_scheme(scheme)
        .map_err(|()| io::Error::other("failed to derive WS_URL"))?;
    Ok(ws_url)
}

fn validate_ws_url(url: Url) -> Result<Url, AnyError> {
    if matches!(url.scheme(), "ws" | "wss") {
        Ok(url)
    } else {
        Err(io::Error::other("WS_URL must use ws or wss").into())
    }
}

#[cfg(test)]
mod tests {
    use crate::{DEMO_ROUTER, derive_ws_url, quote_requests};
    use lunarbase_math::prelude::{Address, QuoteMode, U256};
    use std::str::FromStr;
    use url::Url;

    #[test]
    fn derives_websocket_urls_from_http_urls() {
        assert_eq!(
            derive_ws_url(&Url::parse("https://rpc.example/v1/key").unwrap())
                .unwrap()
                .as_str(),
            "wss://rpc.example/v1/key"
        );
        assert_eq!(
            derive_ws_url(&Url::parse("http://127.0.0.1:8545").unwrap())
                .unwrap()
                .as_str(),
            "ws://127.0.0.1:8545/"
        );
    }

    #[test]
    fn demonstration_router_is_a_valid_address() {
        assert!(Address::from_str(DEMO_ROUTER).is_ok());
    }

    #[test]
    fn builds_two_directions_for_each_lane() {
        let cash = Address::from([1_u8; 20]);
        let first = Address::from([2_u8; 20]);
        let second = Address::from([3_u8; 20]);
        let requests = quote_requests(cash, &[first, second], U256::from(42));

        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0].asset_in, first);
        assert_eq!(requests[0].asset_out, cash);
        assert_eq!(requests[1].asset_in, cash);
        assert_eq!(requests[1].asset_out, first);
        assert_eq!(requests[2].asset_in, second);
        assert_eq!(requests[3].asset_out, second);
        assert!(
            requests
                .iter()
                .all(|request| request.mode == QuoteMode::ExactIn)
        );
    }
}
