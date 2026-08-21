//! Layered production configuration for one Core deployment and fee policy.

use clap::{Args, Parser};
use lunarbase_client::indexer::client_types::ClientConnectConfig;
use lunarbase_client::model::{
    ContractFilter, DeploymentConfig, MATH_COMPATIBILITY_VERSION, Network,
};
use lunarbase_client::protocol::abi::quote_critical_topics;
use lunarbase_math::{Address, B256, FeeClass};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};
use thiserror::Error;

#[derive(Debug, Parser)]
#[command(name = "lunarbase-indexer")]
/// Command-line options for the runnable indexer.
pub struct Cli {
    /// Optional base TOML; CLI flags and `LUNARBASE_*` variables override it.
    #[arg(long, env = "LUNARBASE_CONFIG")]
    pub config: Option<PathBuf>,
    /// Deployment and runtime values supplied directly or through the environment.
    #[command(flatten)]
    pub values: ConfigValues,
}

#[derive(Args, Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
/// Partial configuration shared by TOML, CLI flags, and environment variables.
pub struct ConfigValues {
    /// Adapter family name: `evm`, `base`, `monad`, or `arbitrum`.
    #[arg(long, env = "LUNARBASE_NETWORK")]
    pub network: Option<String>,
    /// EIP-155 chain identifier expected from RPC and realtime sources.
    #[arg(long, env = "LUNARBASE_CHAIN_ID")]
    pub chain_id: Option<u64>,
    /// LunarBase Core contract address encoded as an EVM hex string.
    #[arg(long, env = "LUNARBASE_CORE")]
    pub core: Option<String>,
    /// Economic quote class: `whitelisted` or `non-whitelisted`.
    #[arg(long, env = "LUNARBASE_FEE_CLASS")]
    pub fee_class: Option<String>,
    /// Optional router whose partner/treasury allocation is chain-verified.
    #[arg(long, env = "LUNARBASE_VERIFIED_ROUTER")]
    pub verified_router: Option<String>,
    /// First deployment block included in lane discovery.
    #[arg(long, env = "LUNARBASE_DEPLOYMENT_BLOCK")]
    pub deployment_block: Option<u64>,
    /// Exact ERC-1967 implementation behind the Core proxy.
    #[arg(long, env = "LUNARBASE_EXPECTED_IMPLEMENTATION")]
    pub expected_implementation: Option<String>,
    /// Exact non-zero runtime bytecode hash of the implementation.
    #[arg(long, env = "LUNARBASE_EXPECTED_IMPLEMENTATION_CODE_HASH")]
    pub expected_implementation_code_hash: Option<String>,
    /// Quote-math compatibility profile expected by the runtime.
    #[arg(long, env = "LUNARBASE_CONTRACT_COMPATIBILITY_VERSION")]
    pub contract_compatibility_version: Option<String>,
    /// HTTP JSON-RPC endpoint used only for bootstrap and recovery.
    #[arg(long, env = "LUNARBASE_HTTP_RPC_URL")]
    pub http_rpc_url: Option<String>,
    /// Network-source realtime endpoint or native event-ring locator.
    #[arg(long, env = "LUNARBASE_REALTIME_URL")]
    pub realtime_url: Option<String>,
    /// Live source delivery policy: `realtime`, `block-ordered`, or `finalized`.
    #[arg(long, env = "LUNARBASE_DELIVERY_MODE")]
    pub delivery_mode: Option<String>,
    /// Optional lane allowlist that avoids a full discovery replay.
    #[arg(
        long = "lane",
        env = "LUNARBASE_LANES",
        value_delimiter = ',',
        value_name = "ADDRESS"
    )]
    #[serde(default)]
    pub explicit_lane_assets: Vec<String>,
    /// Socket address for quote, health, and metrics HTTP endpoints.
    #[arg(long, env = "LUNARBASE_BIND")]
    pub bind: Option<String>,
    /// Maximum normalized updates waiting for the single reducer.
    #[arg(long, env = "LUNARBASE_QUEUE_BOUND")]
    pub queue_bound: Option<usize>,
    /// Maximum retained bytes waiting for the single reducer.
    #[arg(long, env = "LUNARBASE_QUEUE_BYTE_BOUND")]
    pub queue_byte_bound: Option<usize>,
    /// Delay before reopening a failed realtime subscription.
    #[arg(long, env = "LUNARBASE_RECONNECT_DELAY_MILLISECONDS")]
    pub reconnect_delay_milliseconds: Option<u64>,
    /// Maximum interval without a source update before fail-closed recovery.
    #[arg(long, env = "LUNARBASE_SOURCE_STALL_TIMEOUT_MILLISECONDS")]
    pub source_stall_timeout_milliseconds: Option<u64>,
    /// Maximum duration of one source handshake, snapshot, or recovery RPC.
    #[arg(long, env = "LUNARBASE_SOURCE_OPERATION_TIMEOUT_MILLISECONDS")]
    pub source_operation_timeout_milliseconds: Option<u64>,
    /// Maximum quote HTTP requests executing concurrently; excess is rejected.
    #[arg(long, env = "LUNARBASE_MAX_IN_FLIGHT_QUOTES")]
    pub max_in_flight_quotes: Option<usize>,
    /// Optional Redis URL used solely to accelerate restarts.
    #[arg(long, env = "LUNARBASE_REDIS_URL")]
    pub redis_url: Option<String>,
    /// Period between best-effort full checkpoint writes.
    #[arg(long, env = "LUNARBASE_CHECKPOINT_INTERVAL_SECONDS")]
    pub checkpoint_interval_seconds: Option<u64>,
    /// Maximum graceful-shutdown duration before the process exits.
    #[arg(long, env = "LUNARBASE_SHUTDOWN_TIMEOUT_SECONDS")]
    pub shutdown_timeout_seconds: Option<u64>,
}

#[derive(Clone, Debug)]
/// Validated runtime configuration.
pub struct Config {
    /// Validated embeddable client identity and runtime bounds.
    pub client: ClientConnectConfig,
    /// Canonical HTTP JSON-RPC endpoint used for bootstrap and recovery.
    pub http_rpc_url: String,
    /// Realtime endpoint or native event-ring locator used by the selected source.
    pub realtime_url: String,
    /// Validated live source delivery and confidence policy.
    pub delivery_mode: DeliveryMode,
    /// Parsed HTTP listen address.
    pub bind: SocketAddr,
    /// Hard admission bound for concurrently executing quote requests.
    pub max_in_flight_quotes: usize,
    /// Optional Redis checkpoint endpoint.
    pub redis_url: Option<String>,
    /// Period between background checkpoint attempts.
    pub checkpoint_interval: Duration,
    /// Deadline shared by client shutdown and final checkpoint handling.
    pub shutdown_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Live source delivery and confidence policy selected for quote state.
pub enum DeliveryMode {
    /// Publish provider updates immediately in receive order.
    Realtime,
    /// Close each executed block and publish its logs in transaction order.
    BlockOrdered,
    /// Publish only updates covered by the source's finalized watermark.
    Finalized,
}

#[derive(Debug, Error)]
/// Configuration loading or validation failure.
pub enum ConfigError {
    /// The TOML configuration file could not be read.
    #[error("read config: {0}")]
    Read(#[from] std::io::Error),
    /// The configuration does not match the strict TOML schema.
    #[error("parse config: {0}")]
    Toml(#[from] toml::de::Error),
    /// A required deployment identity or source value was not supplied.
    #[error(
        "missing `{0}`; use its CLI flag, LUNARBASE_* environment variable, or an optional --config file"
    )]
    Missing(&'static str),
    /// A parsed value violates a deployment or resource invariant.
    #[error("invalid {field}: {detail}")]
    Invalid {
        /// Stable configuration field or section name.
        field: &'static str,
        /// Parsing, range, or semantic validation failure.
        detail: String,
    },
}

impl Config {
    /// Resolves optional TOML, environment-backed CLI values, and explicit
    /// CLI flags in increasing precedence, then validates the result.
    pub fn load(cli: &Cli) -> Result<Self, ConfigError> {
        let file_values = match &cli.config {
            Some(path) => toml::from_str(&std::fs::read_to_string(path)?)?,
            None => ConfigValues::default(),
        };
        file_values.overlay(cli.values.clone()).validate()
    }
}

impl ConfigValues {
    fn overlay(mut self, overrides: Self) -> Self {
        self.network = overrides.network.or(self.network);
        self.chain_id = overrides.chain_id.or(self.chain_id);
        self.core = overrides.core.or(self.core);
        self.fee_class = overrides.fee_class.or(self.fee_class);
        self.verified_router = overrides.verified_router.or(self.verified_router);
        self.deployment_block = overrides.deployment_block.or(self.deployment_block);
        self.expected_implementation = overrides
            .expected_implementation
            .or(self.expected_implementation);
        self.expected_implementation_code_hash = overrides
            .expected_implementation_code_hash
            .or(self.expected_implementation_code_hash);
        self.contract_compatibility_version = overrides
            .contract_compatibility_version
            .or(self.contract_compatibility_version);
        self.http_rpc_url = overrides.http_rpc_url.or(self.http_rpc_url);
        self.realtime_url = overrides.realtime_url.or(self.realtime_url);
        self.delivery_mode = overrides.delivery_mode.or(self.delivery_mode);
        if !overrides.explicit_lane_assets.is_empty() {
            self.explicit_lane_assets = overrides.explicit_lane_assets;
        }
        self.bind = overrides.bind.or(self.bind);
        self.queue_bound = overrides.queue_bound.or(self.queue_bound);
        self.queue_byte_bound = overrides.queue_byte_bound.or(self.queue_byte_bound);
        self.reconnect_delay_milliseconds = overrides
            .reconnect_delay_milliseconds
            .or(self.reconnect_delay_milliseconds);
        self.source_stall_timeout_milliseconds = overrides
            .source_stall_timeout_milliseconds
            .or(self.source_stall_timeout_milliseconds);
        self.source_operation_timeout_milliseconds = overrides
            .source_operation_timeout_milliseconds
            .or(self.source_operation_timeout_milliseconds);
        self.max_in_flight_quotes = overrides.max_in_flight_quotes.or(self.max_in_flight_quotes);
        self.redis_url = overrides.redis_url.or(self.redis_url);
        self.checkpoint_interval_seconds = overrides
            .checkpoint_interval_seconds
            .or(self.checkpoint_interval_seconds);
        self.shutdown_timeout_seconds = overrides
            .shutdown_timeout_seconds
            .or(self.shutdown_timeout_seconds);
        self
    }

    fn validate(self) -> Result<Config, ConfigError> {
        let network_name = required(self.network, "network")?;
        let network = match network_name.to_ascii_lowercase().as_str() {
            "evm" => Network::Evm,
            "base" => Network::Base,
            "monad" => Network::Monad,
            "arbitrum" => Network::Arbitrum,
            _ => return invalid("network", "expected evm, base, monad, or arbitrum"),
        };
        let delivery_mode =
            parse_delivery_mode(self.delivery_mode.as_deref().unwrap_or("realtime"))?;
        let chain_id = required(self.chain_id, "chain_id")?;
        let core = parse_address(&required(self.core, "core")?, "core")?;
        let fee_class = match required(self.fee_class, "fee_class")?
            .to_ascii_lowercase()
            .as_str()
        {
            "whitelisted" => FeeClass::Whitelisted,
            "non-whitelisted" | "non_whitelisted" => FeeClass::NonWhitelisted,
            _ => return invalid("fee_class", "expected whitelisted or non-whitelisted"),
        };
        let verified_router = self
            .verified_router
            .as_deref()
            .map(|value| parse_address(value, "verified_router"))
            .transpose()?;
        let explicit_lane_assets = self
            .explicit_lane_assets
            .iter()
            .map(|value| parse_address(value, "explicit_lane_assets"))
            .collect::<Result<Vec<_>, _>>()?;
        let expected_implementation = parse_address(
            &required(self.expected_implementation, "expected_implementation")?,
            "expected_implementation",
        )?;
        let expected_implementation_code_hash = parse_hash(
            &required(
                self.expected_implementation_code_hash,
                "expected_implementation_code_hash",
            )?,
            "expected_implementation_code_hash",
        )?;
        let bind = self
            .bind
            .unwrap_or_else(default_bind)
            .parse::<SocketAddr>()
            .map_err(|error| ConfigError::Invalid {
                field: "bind",
                detail: error.to_string(),
            })?;
        let queue_bound = self.queue_bound.unwrap_or_else(default_queue_bound);
        let queue_byte_bound = self
            .queue_byte_bound
            .unwrap_or_else(default_queue_byte_bound);
        let reconnect_delay_milliseconds = self
            .reconnect_delay_milliseconds
            .unwrap_or_else(default_reconnect_milliseconds);
        let source_stall_timeout_milliseconds = self
            .source_stall_timeout_milliseconds
            .unwrap_or_else(default_source_stall_milliseconds);
        let source_operation_timeout_milliseconds = self
            .source_operation_timeout_milliseconds
            .unwrap_or_else(default_source_operation_milliseconds);
        let max_in_flight_quotes = self
            .max_in_flight_quotes
            .unwrap_or_else(default_max_in_flight_quotes);
        let checkpoint_interval_seconds = self
            .checkpoint_interval_seconds
            .unwrap_or_else(default_checkpoint_seconds);
        let shutdown_timeout_seconds = self
            .shutdown_timeout_seconds
            .unwrap_or_else(default_shutdown_seconds);
        if queue_bound == 0
            || queue_byte_bound < lunarbase_client::model::MIN_UPDATE_QUEUE_BYTE_CAPACITY
            || queue_byte_bound > u32::MAX as usize
            || reconnect_delay_milliseconds == 0
            || source_stall_timeout_milliseconds == 0
            || source_operation_timeout_milliseconds == 0
            || max_in_flight_quotes == 0
            || checkpoint_interval_seconds == 0
            || shutdown_timeout_seconds == 0
        {
            return invalid(
                "runtime",
                "all queue and timing bounds must be non-zero and the byte queue must be at least 1024 bytes",
            );
        }
        if self.redis_url.as_deref().is_some_and(str::is_empty) {
            return invalid("redis_url", "empty URL is not valid");
        }
        let http_rpc_url = required(self.http_rpc_url, "http_rpc_url")?;
        let realtime_url = required(self.realtime_url, "realtime_url")?;
        if http_rpc_url.is_empty() || realtime_url.is_empty() {
            return invalid("source", "HTTP RPC and realtime endpoints are required");
        }
        let deployment = DeploymentConfig {
            network,
            chain_id,
            core,
            fee_class,
            verified_router,
            deployment_block: self.deployment_block.unwrap_or(0),
            expected_implementation,
            expected_implementation_code_hash,
            contract_compatibility_version: self
                .contract_compatibility_version
                .unwrap_or_else(default_compatibility),
            explicit_lane_assets,
        };
        let client = ClientConnectConfig {
            filter: ContractFilter {
                address: core,
                topics: quote_critical_topics().to_vec(),
            },
            deployment,
            buffer_capacity: queue_bound,
            buffer_byte_capacity: queue_byte_bound,
            reconnect_delay: Duration::from_millis(reconnect_delay_milliseconds),
            source_stall_timeout: Duration::from_millis(source_stall_timeout_milliseconds),
            source_operation_timeout: Duration::from_millis(source_operation_timeout_milliseconds),
        };
        client.validate().map_err(|error| ConfigError::Invalid {
            field: "deployment",
            detail: error.to_string(),
        })?;
        Ok(Config {
            client,
            http_rpc_url,
            realtime_url,
            delivery_mode,
            bind,
            max_in_flight_quotes,
            redis_url: self.redis_url,
            checkpoint_interval: Duration::from_secs(checkpoint_interval_seconds),
            shutdown_timeout: Duration::from_secs(shutdown_timeout_seconds),
        })
    }
}

fn required<T>(value: Option<T>, field: &'static str) -> Result<T, ConfigError> {
    value.ok_or(ConfigError::Missing(field))
}

fn parse_address(value: &str, field: &'static str) -> Result<Address, ConfigError> {
    Address::from_str(value).map_err(|error| ConfigError::Invalid {
        field,
        detail: error.to_string(),
    })
}

fn parse_hash(value: &str, field: &'static str) -> Result<B256, ConfigError> {
    B256::from_str(value).map_err(|error| ConfigError::Invalid {
        field,
        detail: error.to_string(),
    })
}

fn parse_delivery_mode(value: &str) -> Result<DeliveryMode, ConfigError> {
    match value.to_ascii_lowercase().as_str() {
        "realtime" => Ok(DeliveryMode::Realtime),
        "block-ordered" | "block_ordered" | "canonical" => Ok(DeliveryMode::BlockOrdered),
        "finalized" => Ok(DeliveryMode::Finalized),
        _ => invalid(
            "delivery_mode",
            "expected realtime, block-ordered, or finalized",
        ),
    }
}

fn invalid<T>(field: &'static str, detail: &str) -> Result<T, ConfigError> {
    Err(ConfigError::Invalid {
        field,
        detail: detail.into(),
    })
}

fn default_compatibility() -> String {
    MATH_COMPATIBILITY_VERSION.into()
}
fn default_bind() -> String {
    "127.0.0.1:8080".into()
}
fn default_queue_bound() -> usize {
    4096
}
fn default_queue_byte_bound() -> usize {
    64 * 1024 * 1024
}
fn default_reconnect_milliseconds() -> u64 {
    1_000
}
fn default_source_stall_milliseconds() -> u64 {
    30_000
}
fn default_source_operation_milliseconds() -> u64 {
    15_000
}
fn default_max_in_flight_quotes() -> usize {
    1_024
}
fn default_checkpoint_seconds() -> u64 {
    30
}
fn default_shutdown_seconds() -> u64 {
    15
}

#[cfg(test)]
mod tests {
    use super::{Cli, ConfigError, ConfigValues, DeliveryMode};
    use clap::Parser;
    use lunarbase_client::protocol::abi::quote_critical_topics;

    const CORE: &str = "0x0000000000000000000000000000000000000001";
    const ROUTER: &str = "0x0000000000000000000000000000000000000002";
    const IMPLEMENTATION: &str = "0x0000000000000000000000000000000000000003";
    const CODE_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000004";

    fn complete_values() -> ConfigValues {
        ConfigValues {
            network: Some("base".into()),
            chain_id: Some(8453),
            core: Some(CORE.into()),
            fee_class: Some("whitelisted".into()),
            verified_router: Some(ROUTER.into()),
            expected_implementation: Some(IMPLEMENTATION.into()),
            expected_implementation_code_hash: Some(CODE_HASH.into()),
            http_rpc_url: Some("http://localhost:8545".into()),
            realtime_url: Some("ws://localhost:8546".into()),
            ..Default::default()
        }
    }

    #[test]
    fn accepts_configuration_without_a_file() {
        let config = complete_values().validate().unwrap();
        assert_eq!(config.client.deployment.chain_id, 8453);
        assert_eq!(config.delivery_mode, DeliveryMode::Realtime);
        assert_eq!(config.client.buffer_capacity, 4096);
        assert_eq!(config.max_in_flight_quotes, 1024);
        assert_eq!(
            config.client.source_operation_timeout,
            std::time::Duration::from_secs(15)
        );
        assert_eq!(
            config.client.deployment.fee_class,
            lunarbase_math::FeeClass::Whitelisted
        );
        assert_eq!(
            config.client.filter.topics,
            quote_critical_topics(),
            "the runnable indexer subscribes only to quote-critical events"
        );
    }

    #[test]
    fn direct_values_override_file_values() {
        let file_values = complete_values();
        let direct = ConfigValues {
            network: Some("evm".into()),
            chain_id: Some(97),
            http_rpc_url: Some("https://rpc.example".into()),
            delivery_mode: Some("block-ordered".into()),
            explicit_lane_assets: vec![CORE.into()],
            ..Default::default()
        };
        let config = file_values.overlay(direct).validate().unwrap();
        assert_eq!(config.client.deployment.chain_id, 97);
        assert_eq!(config.http_rpc_url, "https://rpc.example");
        assert_eq!(config.delivery_mode, DeliveryMode::BlockOrdered);
        assert_eq!(config.client.deployment.explicit_lane_assets.len(), 1);
    }

    #[test]
    fn clap_accepts_repeated_lane_arguments() {
        let cli =
            Cli::try_parse_from(["lunarbase-indexer", "--lane", CORE, "--lane", ROUTER]).unwrap();
        assert_eq!(cli.values.explicit_lane_assets, [CORE, ROUTER]);
    }

    #[test]
    fn missing_required_values_are_explicit() {
        assert!(matches!(
            ConfigValues::default().validate(),
            Err(ConfigError::Missing("network"))
        ));
    }

    #[test]
    fn rejects_zero_quote_admission_capacity() {
        let mut values = complete_values();
        values.max_in_flight_quotes = Some(0);
        assert!(matches!(
            values.validate(),
            Err(ConfigError::Invalid {
                field: "runtime",
                ..
            })
        ));
    }

    #[test]
    fn rejects_unknown_delivery_mode() {
        let mut values = complete_values();
        values.delivery_mode = Some("latest".into());
        assert!(matches!(
            values.validate(),
            Err(ConfigError::Invalid {
                field: "delivery_mode",
                ..
            })
        ));
    }
}
