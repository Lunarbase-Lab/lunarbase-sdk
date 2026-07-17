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

