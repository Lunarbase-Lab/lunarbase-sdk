//! Minimal production configuration for one Core/router deployment.

use clap::Parser;
use lunarbase_client_core::indexer::client_types::ClientConnectConfig;
use lunarbase_client_core::model::{
    ContractFilter, DeploymentConfig, MATH_COMPATIBILITY_VERSION, Network,
};
use lunarbase_client_core::protocol::abi::quote_critical_topics;
use lunarbase_math::types::{Address, B256};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};
use thiserror::Error;

#[derive(Debug, Parser)]
#[command(name = "lunarbase-indexer")]
/// Command-line options for the runnable indexer.
pub struct Cli {
    /// TOML deployment configuration.
    #[arg(long, default_value = "config/base.toml")]
    pub config: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Human-editable service configuration.
pub struct RawConfig {
    /// Adapter family name: `base`, `monad`, or `arbitrum`.
    pub network: String,
    /// EIP-155 chain identifier expected from RPC and realtime sources.
    pub chain_id: u64,
    /// LunarBase Core contract address encoded as an EVM hex string.
    pub core: String,
    /// Single configured router whose fee profile is tracked.
    pub router: String,
    /// Required bootstrap whitelist status for the configured router.
    #[serde(default = "default_true")]
    pub expect_whitelisted: bool,
    /// First deployment block included in lane discovery.
    pub deployment_block: u64,
    /// Pinned Core runtime bytecode hash, or the configured compatibility sentinel.
    pub expected_runtime_code_hash: String,
    /// Contracts revision expected by the runtime and checkpoint schema.
    #[serde(default = "default_compatibility")]
    pub contract_compatibility_version: String,
    /// HTTP JSON-RPC endpoint used only for bootstrap and recovery.
    pub http_rpc_url: String,
    /// Network-adapter realtime endpoint or native event-ring locator.
    pub realtime_url: String,
    /// Optional lane allowlist that avoids a full discovery replay.
    #[serde(default)]
    pub explicit_lane_assets: Vec<String>,
    /// Socket address for quote, health, and metrics HTTP endpoints.
    #[serde(default = "default_bind")]
    pub bind: String,
    /// Maximum normalized updates waiting for the single reducer.
    #[serde(default = "default_queue_bound")]
    pub queue_bound: usize,
    /// Delay before reopening a failed realtime subscription.
    #[serde(default = "default_reconnect_milliseconds")]
    pub reconnect_delay_milliseconds: u64,
    /// Optional Redis URL used solely to accelerate restarts.
    #[serde(default)]
    pub redis_url: Option<String>,
    /// Period between best-effort full checkpoint writes.
    #[serde(default = "default_checkpoint_seconds")]
    pub checkpoint_interval_seconds: u64,
    /// Maximum graceful-shutdown duration before the process exits.
    #[serde(default = "default_shutdown_seconds")]
    pub shutdown_timeout_seconds: u64,
}

#[derive(Clone, Debug)]
/// Validated runtime configuration.
pub struct Config {
    /// Validated embeddable client identity and runtime bounds.
    pub client: ClientConnectConfig,
    /// Parsed HTTP listen address.
    pub bind: SocketAddr,
    /// Optional Redis checkpoint endpoint.
    pub redis_url: Option<String>,
    /// Period between background checkpoint attempts.
    pub checkpoint_interval: Duration,
    /// Deadline shared by client shutdown and final checkpoint handling.
    pub shutdown_timeout: Duration,
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
    /// Loads and validates one TOML file.
    pub fn load(path: &PathBuf) -> Result<Self, ConfigError> {
        let raw: RawConfig = toml::from_str(&std::fs::read_to_string(path)?)?;
        raw.validate()
    }
}

impl RawConfig {
    fn validate(self) -> Result<Config, ConfigError> {
        let network = match self.network.to_ascii_lowercase().as_str() {
            "base" => Network::Base,
            "monad" => Network::Monad,
            "arbitrum" => Network::Arbitrum,
            _ => return invalid("network", "expected base, monad, or arbitrum"),
        };
        let core = parse_address(&self.core, "core")?;
        let router = parse_address(&self.router, "router")?;
        let explicit_lane_assets = self
            .explicit_lane_assets
            .iter()
            .map(|value| parse_address(value, "explicit_lane_assets"))
            .collect::<Result<Vec<_>, _>>()?;
        let expected_runtime_code_hash = parse_hash(
            &self.expected_runtime_code_hash,
            "expected_runtime_code_hash",
        )?;
        let bind = self
            .bind
            .parse::<SocketAddr>()
            .map_err(|error| ConfigError::Invalid {
                field: "bind",
                detail: error.to_string(),
            })?;
        if self.queue_bound == 0
            || self.reconnect_delay_milliseconds == 0
            || self.checkpoint_interval_seconds == 0
            || self.shutdown_timeout_seconds == 0
        {
            return invalid("runtime", "all queue and timing bounds must be non-zero");
        }
        if self.redis_url.as_deref().is_some_and(str::is_empty) {
            return invalid("redis_url", "empty URL is not valid");
        }
        let deployment = DeploymentConfig {
            network,
            chain_id: self.chain_id,
            core,
            router,
            expect_whitelisted: self.expect_whitelisted,
            deployment_block: self.deployment_block,
            expected_runtime_code_hash,
            contract_compatibility_version: self.contract_compatibility_version,
            http_rpc_url: self.http_rpc_url,
            realtime_source: self.realtime_url,
            explicit_lane_assets,
        };
        let client = ClientConnectConfig {
            filter: ContractFilter {
                address: core,
                topics: quote_critical_topics().to_vec(),
            },
            deployment,
            buffer_capacity: self.queue_bound,
            reconnect_delay: Duration::from_millis(self.reconnect_delay_milliseconds),
        };
        client.validate().map_err(|error| ConfigError::Invalid {
            field: "deployment",
            detail: error.to_string(),
        })?;
        Ok(Config {
            client,
            bind,
            redis_url: self.redis_url,
            checkpoint_interval: Duration::from_secs(self.checkpoint_interval_seconds),
            shutdown_timeout: Duration::from_secs(self.shutdown_timeout_seconds),
        })
    }
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

fn invalid<T>(field: &'static str, detail: &str) -> Result<T, ConfigError> {
    Err(ConfigError::Invalid {
        field,
        detail: detail.into(),
    })
}

fn default_true() -> bool {
    true
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
fn default_reconnect_milliseconds() -> u64 {
    1_000
}
fn default_checkpoint_seconds() -> u64 {
    30
}
fn default_shutdown_seconds() -> u64 {
    15
}
