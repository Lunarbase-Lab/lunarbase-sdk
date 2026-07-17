//! TOML configuration and validation.

use lunarbase_client_core::{DeploymentConfig, Network, RedisConfig, MATH_COMPATIBILITY_VERSION};
use lunarbase_math::Address;
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Top-level daemon configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerConfig {
    pub network: NetworkName,
    pub core: String,
    pub deployment_block: u64,
    pub expected_runtime_code_hash: String,
    pub http_rpc_url: String,
    pub realtime_url: String,
    #[serde(default)]
    pub chain_id: Option<u64>,
    #[serde(default = "default_contract_compatibility")]
    pub contract_compatibility_version: String,
    #[serde(default = "default_snapshot_tag")]
    pub snapshot_tag: String,
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default)]
    pub explicit_lane_assets: Vec<String>,
    #[serde(default)]
    pub eager_routers: Vec<String>,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub redis: ServiceRedisConfig,
    #[serde(default)]
    pub writer_lease: WriterLeaseConfig,
    #[serde(default)]
    pub transport: TransportConfig,
    #[serde(default)]
    pub shutdown: ShutdownConfig,
    #[serde(default)]
    pub alerts: AlertsConfig,
}

/// Network name accepted by the TOML file.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkName {
    Base,
    Monad,
    Arbitrum,
}

impl From<NetworkName> for Network {
    fn from(value: NetworkName) -> Self {
        match value {
            NetworkName::Base => Self::Base,
            NetworkName::Monad => Self::Monad,
            NetworkName::Arbitrum => Self::Arbitrum,
        }
    }
}

/// Queue and reconnect bounds for the common runtime.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    pub buffer_capacity: usize,
    pub reconnect_delay_milliseconds: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 4096,
            reconnect_delay_milliseconds: 1_000,
        }
    }
}

/// Optional Redis-backed checkpoint persistence.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServiceRedisConfig {
    pub enabled: bool,
    pub url: String,
    pub io_timeout_milliseconds: u64,
    pub stream_max_len: usize,
    pub dedup_ttl_seconds: u64,
    pub checkpoint_interval_updates: usize,
}

impl Default for ServiceRedisConfig {
    fn default() -> Self {
        let defaults = RedisConfig::default();
        Self {
            enabled: false,
            url: defaults.url,
            io_timeout_milliseconds: 2_000,
            stream_max_len: defaults.stream_max_len,
            dedup_ttl_seconds: defaults.dedup_ttl_seconds,
            checkpoint_interval_updates: defaults.checkpoint_interval_updates,
        }
    }
}

/// Redis-backed single-writer coordination for horizontally scaled replicas.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WriterLeaseConfig {
    /// Defaults to the Redis setting when omitted: Redis deployments are safe
    /// by default, while in-memory deployments do not attempt coordination.
    pub enabled: Option<bool>,
    /// Stable replica identity. An empty value is generated per process.
    pub owner: String,
    pub ttl_milliseconds: u64,
    pub renew_interval_milliseconds: u64,
    pub retry_interval_milliseconds: u64,
}

impl Default for WriterLeaseConfig {
    fn default() -> Self {
        Self {
            enabled: None,
            owner: String::new(),
            ttl_milliseconds: 15_000,
            renew_interval_milliseconds: 5_000,
            retry_interval_milliseconds: 2_000,
        }
    }
}

/// Network transport memory and verification bounds.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransportConfig {
    pub max_frame_bytes: usize,
    pub reorder_capacity: usize,
    pub require_evm_parent_context: bool,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: 512 * 1024,
            reorder_capacity: 4096,
            require_evm_parent_context: true,
        }
    }
}

/// Graceful process shutdown deadline.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ShutdownConfig {
    pub timeout_seconds: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: 15,
        }
    }
}

/// Operational alert polling, deduplication, and webhook delivery settings.
///
/// Structured error logs remain enabled even when no webhook URL is set.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AlertsConfig {
    pub enabled: bool,
    pub webhook_url: String,
    pub poll_interval_seconds: u64,
    pub not_ready_after_seconds: u64,
    pub repeat_interval_seconds: u64,
    pub request_timeout_seconds: u64,
}

impl Default for AlertsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            webhook_url: String::new(),
            poll_interval_seconds: 5,
            not_ready_after_seconds: 30,
            repeat_interval_seconds: 300,
            request_timeout_seconds: 5,
        }
    }
}

/// Validated alert settings used by the asynchronous supervisor.
#[derive(Clone, Debug)]
pub struct ValidatedAlertsConfig {
    pub enabled: bool,
    pub webhook_url: Option<String>,
    pub poll_interval: Duration,
    pub not_ready_after: Duration,
    pub repeat_interval: Duration,
    pub request_timeout: Duration,
}

/// Validated single-writer lease timing and replica identity.
#[derive(Clone, Debug)]
pub struct ValidatedWriterLeaseConfig {
    pub enabled: bool,
    pub owner: String,
    pub ttl: Duration,
    pub renew_interval: Duration,
    pub retry_interval: Duration,
}

/// Fully parsed runtime configuration.
#[derive(Clone, Debug)]
pub struct ValidatedConfig {
    pub deployment: DeploymentConfig,
    pub snapshot_tag: String,
    pub bind: SocketAddr,
    pub runtime: RuntimeConfig,
    pub redis_enabled: bool,
    pub redis_io_timeout: Duration,
    pub writer_lease: ValidatedWriterLeaseConfig,
    pub transport: TransportConfig,
    pub shutdown_timeout: Duration,
    pub alerts: ValidatedAlertsConfig,
}

/// Configuration loading or semantic validation failure.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config `{path}`: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("invalid TOML config: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid config field `{field}`: {detail}")]
    Invalid { field: &'static str, detail: String },
}

impl IndexerConfig {
    /// Reads one TOML configuration file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        Ok(toml::from_str(&contents)?)
    }

    /// Parses addresses, code hash, socket address, and runtime bounds.
    pub fn validate(self) -> Result<ValidatedConfig, ConfigError> {
        let network = Network::from(self.network);
        let core = parse_address(&self.core, "core")?;
        let expected_runtime_code_hash = parse_hash(
            &self.expected_runtime_code_hash,
            "expected_runtime_code_hash",
        )?;
        let explicit_lane_assets = self
            .explicit_lane_assets
            .iter()
            .map(|value| parse_address(value, "explicit_lane_assets"))
            .collect::<Result<Vec<_>, _>>()?;
        let eager_routers = self
            .eager_routers
            .iter()
            .map(|value| parse_address(value, "eager_routers"))
            .collect::<Result<Vec<_>, _>>()?;
        let bind = self
            .bind
            .parse::<SocketAddr>()
            .map_err(|error| ConfigError::Invalid {
                field: "bind",
                detail: error.to_string(),
            })?;
        if self.runtime.buffer_capacity == 0 || self.runtime.reconnect_delay_milliseconds == 0 {
            return Err(ConfigError::Invalid {
                field: "runtime",
                detail: "buffer and reconnect delay must be non-zero".into(),
            });
        }
        if self.transport.max_frame_bytes == 0 || self.transport.reorder_capacity == 0 {
            return Err(ConfigError::Invalid {
                field: "transport",
                detail: "frame and reorder bounds must be non-zero".into(),
            });
        }
        if self.shutdown.timeout_seconds == 0 {
            return Err(ConfigError::Invalid {
                field: "shutdown.timeout_seconds",
                detail: "shutdown timeout must be non-zero".into(),
            });
        }
        if self.redis.io_timeout_milliseconds == 0 {
            return Err(ConfigError::Invalid {
                field: "redis.io_timeout_milliseconds",
                detail: "Redis I/O timeout must be non-zero".into(),
            });
        }
        if self.redis.io_timeout_milliseconds.saturating_mul(2)
            > self.shutdown.timeout_seconds.saturating_mul(1_000)
        {
            return Err(ConfigError::Invalid {
                field: "redis.io_timeout_milliseconds",
                detail: "two bounded Redis attempts must fit inside the shutdown timeout".into(),
            });
        }
        let writer_lease_enabled = self.writer_lease.enabled.unwrap_or(self.redis.enabled);
        if writer_lease_enabled && !self.redis.enabled {
            return Err(ConfigError::Invalid {
                field: "writer_lease.enabled",
                detail: "writer lease requires Redis persistence".into(),
            });
        }
        if self.writer_lease.ttl_milliseconds == 0
            || self.writer_lease.renew_interval_milliseconds == 0
            || self.writer_lease.retry_interval_milliseconds == 0
        {
            return Err(ConfigError::Invalid {
                field: "writer_lease",
                detail: "TTL, renew interval, and retry interval must be non-zero".into(),
            });
        }
        if self
            .writer_lease
            .renew_interval_milliseconds
            .saturating_add(self.redis.io_timeout_milliseconds.saturating_mul(2))
            >= self.writer_lease.ttl_milliseconds
        {
            return Err(ConfigError::Invalid {
                field: "writer_lease",
                detail: "lease TTL must exceed the renew interval plus two Redis I/O timeouts"
                    .into(),
            });
        }
        if self.alerts.poll_interval_seconds == 0
            || self.alerts.not_ready_after_seconds == 0
            || self.alerts.repeat_interval_seconds == 0
            || self.alerts.request_timeout_seconds == 0
        {
            return Err(ConfigError::Invalid {
                field: "alerts",
                detail: "all alert timing bounds must be non-zero".into(),
            });
        }
        if self.alerts.request_timeout_seconds > self.shutdown.timeout_seconds {
            return Err(ConfigError::Invalid {
                field: "alerts.request_timeout_seconds",
                detail: "alert request timeout cannot exceed the shutdown timeout".into(),
            });
        }
        let redis = RedisConfig {
            url: self.redis.url,
            stream_max_len: self.redis.stream_max_len,
            dedup_ttl_seconds: self.redis.dedup_ttl_seconds,
            checkpoint_interval_updates: self.redis.checkpoint_interval_updates,
        };
        let deployment = DeploymentConfig {
            network,
            chain_id: self.chain_id.unwrap_or_else(|| network.default_chain_id()),
            core,
            deployment_block: self.deployment_block,
            expected_runtime_code_hash,
            contract_compatibility_version: self.contract_compatibility_version,
            http_rpc_url: self.http_rpc_url,
            realtime_source: self.realtime_url,
            redis,
            explicit_lane_assets,
            eager_routers,
        };
        deployment
            .validate()
            .map_err(|error| ConfigError::Invalid {
                field: "deployment",
                detail: error.to_string(),
            })?;
        if self.snapshot_tag != "latest"
            && self.snapshot_tag != "safe"
            && self.snapshot_tag != "finalized"
        {
            return Err(ConfigError::Invalid {
                field: "snapshot_tag",
                detail: "expected latest, safe, or finalized".into(),
            });
        }
        let webhook_url = std::env::var("LUNARBASE_ALERT_WEBHOOK_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                let value = self.alerts.webhook_url.trim();
                (!value.is_empty()).then(|| value.to_owned())
            });
        if let Some(webhook_url) = &webhook_url {
            let parsed =
                reqwest::Url::parse(webhook_url).map_err(|error| ConfigError::Invalid {
                    field: "alerts.webhook_url",
                    detail: error.to_string(),
                })?;
            if parsed.scheme() != "http" && parsed.scheme() != "https" {
                return Err(ConfigError::Invalid {
                    field: "alerts.webhook_url",
                    detail: "expected an http or https URL".into(),
                });
            }
        }
        Ok(ValidatedConfig {
            deployment,
            snapshot_tag: self.snapshot_tag,
            bind,
            runtime: self.runtime,
            redis_enabled: self.redis.enabled,
            redis_io_timeout: Duration::from_millis(self.redis.io_timeout_milliseconds),
            writer_lease: ValidatedWriterLeaseConfig {
                enabled: writer_lease_enabled,
                owner: writer_lease_owner(&self.writer_lease.owner),
                ttl: Duration::from_millis(self.writer_lease.ttl_milliseconds),
                renew_interval: Duration::from_millis(
                    self.writer_lease.renew_interval_milliseconds,
                ),
                retry_interval: Duration::from_millis(
                    self.writer_lease.retry_interval_milliseconds,
                ),
            },
            transport: self.transport,
            shutdown_timeout: Duration::from_secs(self.shutdown.timeout_seconds),
            alerts: ValidatedAlertsConfig {
                enabled: self.alerts.enabled,
                webhook_url,
                poll_interval: Duration::from_secs(self.alerts.poll_interval_seconds),
                not_ready_after: Duration::from_secs(self.alerts.not_ready_after_seconds),
                repeat_interval: Duration::from_secs(self.alerts.repeat_interval_seconds),
                request_timeout: Duration::from_secs(self.alerts.request_timeout_seconds),
            },
        })
    }
}

fn writer_lease_owner(configured: &str) -> String {
    if let Ok(owner) = std::env::var("LUNARBASE_WRITER_ID") {
        if !owner.trim().is_empty() {
            return owner;
        }
    }
    if !configured.trim().is_empty() {
        return configured.to_owned();
    }
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".into());
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{host}:{}:{started}", std::process::id())
}

fn parse_address(value: &str, field: &'static str) -> Result<Address, ConfigError> {
    Address::from_hex(value).map_err(|error| ConfigError::Invalid {
        field,
        detail: error.to_string(),
    })
}

fn parse_hash(value: &str, field: &'static str) -> Result<[u8; 32], ConfigError> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 64 {
        return Err(ConfigError::Invalid {
            field,
            detail: "expected a 32-byte hexadecimal value".into(),
        });
    }
    let mut result = [0; 32];
    for (index, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).map_err(|_| {
            ConfigError::Invalid {
                field,
                detail: "expected a 32-byte hexadecimal value".into(),
            }
        })?;
    }
    Ok(result)
}

fn default_contract_compatibility() -> String {
    MATH_COMPATIBILITY_VERSION.into()
}

fn default_snapshot_tag() -> String {
    "finalized".into()
}

fn default_bind() -> String {
    "127.0.0.1:8080".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_is_valid_without_explicit_chain_id() {
        let config: IndexerConfig = toml::from_str(
            r#"
network = "base"
core = "0x0000000000000000000000000000000000000001"
deployment_block = 1
expected_runtime_code_hash = "0x0000000000000000000000000000000000000000000000000000000000000001"
http_rpc_url = "http://127.0.0.1:8545"
realtime_url = "ws://127.0.0.1:8546"
"#,
        )
        .unwrap();
        let config = config.validate().unwrap();
        assert_eq!(config.deployment.chain_id, 8453);
        assert_eq!(config.deployment.network, Network::Base);
    }

    #[test]
    fn checked_in_network_templates_are_valid() {
        let templates = [
            (include_str!("../../../config/base.toml"), Network::Base),
            (include_str!("../../../config/monad.toml"), Network::Monad),
            (
                include_str!("../../../config/arbitrum.toml"),
                Network::Arbitrum,
            ),
            (
                include_str!("../../../config/production.base.toml"),
                Network::Base,
            ),
        ];
        for (source, expected_network) in templates {
            let config: IndexerConfig = toml::from_str(source).unwrap();
            assert_eq!(
                config.validate().unwrap().deployment.network,
                expected_network
            );
        }
    }
}
