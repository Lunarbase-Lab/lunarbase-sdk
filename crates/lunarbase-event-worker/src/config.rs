//! Command-line and environment configuration for the standalone worker.

use alloy_primitives::Address;
use clap::Parser;
use lunarbase_client::model::{Commitment, Network};
use std::{net::SocketAddr, str::FromStr, time::Duration};
use thiserror::Error;

#[derive(Debug, Parser)]
#[command(name = "lunarbase-event-worker")]
pub(crate) struct Cli {
    /// Source adapter: evm, base, monad, or arbitrum.
    #[arg(long, env = "LUNARBASE_EVENT_NETWORK")]
    network: String,
    /// EIP-155 identity required from every source connection.
    #[arg(long, env = "LUNARBASE_EVENT_CHAIN_ID")]
    chain_id: u64,
    /// Core contract whose raw logs are persisted.
    #[arg(long, env = "LUNARBASE_EVENT_CORE")]
    core: String,
    /// First block used when Redis has no durable cursor.
    #[arg(long, env = "LUNARBASE_EVENT_DEPLOYMENT_BLOCK")]
    deployment_block: u64,
    /// Canonical HTTP endpoint used only for recovery backfill.
    #[arg(long, env = "LUNARBASE_EVENT_HTTP_RPC_URL")]
    http_rpc_url: String,
    /// Dedicated realtime endpoint or native event-ring locator.
    #[arg(long, env = "LUNARBASE_EVENT_REALTIME_URL")]
    realtime_url: String,
    /// Dedicated Redis instance configured with AOF fsync-always.
    #[arg(long, env = "LUNARBASE_EVENT_REDIS_URL")]
    redis_url: String,
    /// Redis key prefix. Deployment identity is appended automatically.
    #[arg(
        long,
        env = "LUNARBASE_EVENT_REDIS_NAMESPACE",
        default_value = "lunarbase"
    )]
    redis_namespace: String,
    /// Consumer group created at 0-0 for at-least-once downstream replay.
    #[arg(
        long,
        env = "LUNARBASE_EVENT_CONSUMER_GROUP",
        default_value = "lunarbase-processors"
    )]
    consumer_group: String,
    /// Minimum source commitment persisted to the stream.
    #[arg(
        long,
        env = "LUNARBASE_EVENT_MIN_COMMITMENT",
        default_value = "realtime"
    )]
    minimum_commitment: String,
    /// Independent liveness, readiness, and metrics listen address.
    #[arg(long, env = "LUNARBASE_EVENT_BIND", default_value = "127.0.0.1:9091")]
    bind: SocketAddr,
    /// Maximum source updates waiting while Redis is slower than ingestion.
    #[arg(
        long,
        env = "LUNARBASE_EVENT_SOURCE_QUEUE_BOUND",
        default_value_t = 4096
    )]
    source_queue_bound: usize,
    /// Maximum inclusive block span requested by one recovery page.
    #[arg(
        long,
        env = "LUNARBASE_EVENT_BACKFILL_PAGE_BLOCKS",
        default_value_t = 1_000
    )]
    backfill_page_blocks: u64,
    /// Maximum commands waiting for the dedicated blocking Redis connection.
    #[arg(long, env = "LUNARBASE_EVENT_REDIS_QUEUE_BOUND", default_value_t = 8)]
    redis_queue_bound: usize,
    /// Source and dependency retry delay.
    #[arg(
        long,
        env = "LUNARBASE_EVENT_RECONNECT_DELAY_MILLISECONDS",
        default_value_t = 1000
    )]
    reconnect_delay_milliseconds: u64,
    /// Maximum silence from an established realtime source.
    #[arg(
        long,
        env = "LUNARBASE_EVENT_SOURCE_STALL_TIMEOUT_MILLISECONDS",
        default_value_t = 30_000
    )]
    source_stall_timeout_milliseconds: u64,
    /// Redis connect, read, and write deadline.
    #[arg(
        long,
        env = "LUNARBASE_EVENT_REDIS_TIMEOUT_MILLISECONDS",
        default_value_t = 2000
    )]
    redis_timeout_milliseconds: u64,
    /// Native Monad event-ring polling interval.
    #[arg(
        long,
        env = "LUNARBASE_EVENT_NATIVE_POLL_INTERVAL_MICROSECONDS",
        default_value_t = 100
    )]
    native_poll_interval_microseconds: u64,
    /// Maximum cooperative shutdown duration.
    #[arg(
        long,
        env = "LUNARBASE_EVENT_SHUTDOWN_TIMEOUT_SECONDS",
        default_value_t = 10
    )]
    shutdown_timeout_seconds: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub network: Network,
    pub chain_id: u64,
    pub core: Address,
    pub deployment_block: u64,
    pub http_rpc_url: String,
    pub realtime_url: String,
    pub redis_url: String,
    pub redis_namespace: String,
    pub consumer_group: String,
    pub minimum_commitment: Commitment,
    pub bind: SocketAddr,
    pub source_queue_bound: usize,
    pub backfill_page_blocks: u64,
    pub redis_queue_bound: usize,
    pub reconnect_delay: Duration,
    pub source_stall_timeout: Duration,
    pub redis_timeout: Duration,
    #[cfg(all(feature = "monad-native", target_os = "linux"))]
    pub native_poll_interval: Duration,
    pub shutdown_timeout: Duration,
}

#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    #[error("invalid {field}: {detail}")]
    Invalid { field: &'static str, detail: String },
}

impl Cli {
    pub(crate) fn validate(self) -> Result<Config, ConfigError> {
        let network = match self.network.to_ascii_lowercase().as_str() {
            "evm" => Network::Evm,
            "base" => Network::Base,
            "monad" => Network::Monad,
            "arbitrum" => Network::Arbitrum,
            _ => return invalid("network", "expected evm, base, monad, or arbitrum"),
        };
        let core = Address::from_str(&self.core)
            .map_err(|error| invalid_value("core", error.to_string()))?;
        if core == Address::ZERO {
            return invalid("core", "zero address is not a deployment identity");
        }
        let minimum_commitment = match self.minimum_commitment.to_ascii_lowercase().as_str() {
            "realtime" => Commitment::Realtime,
            "canonical" | "block-ordered" => Commitment::Canonical,
            "finalized" => Commitment::Finalized,
            _ => {
                return invalid(
                    "minimum_commitment",
                    "expected realtime, block-ordered, or finalized",
                );
            }
        };
        if self.http_rpc_url.is_empty() || self.realtime_url.is_empty() || self.redis_url.is_empty()
        {
            return invalid(
                "endpoints",
                "HTTP, realtime, and Redis endpoints are required",
            );
        }
        if self.redis_namespace.is_empty()
            || !self
                .redis_namespace
                .bytes()
                .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'-' | b'_'))
        {
            return invalid(
                "redis_namespace",
                "use only ASCII letters, digits, '-' or '_'",
            );
        }
        if self.consumer_group.is_empty()
            || self
                .consumer_group
                .bytes()
                .any(|value| value.is_ascii_control())
        {
            return invalid(
                "consumer_group",
                "must be non-empty and contain no control characters",
            );
        }
        if self.chain_id == 0
            || self.source_queue_bound == 0
            || self.backfill_page_blocks == 0
            || self.redis_queue_bound == 0
            || self.reconnect_delay_milliseconds == 0
            || self.source_stall_timeout_milliseconds == 0
            || self.redis_timeout_milliseconds == 0
            || self.native_poll_interval_microseconds == 0
            || self.shutdown_timeout_seconds == 0
        {
            return invalid(
                "resource_bounds",
                "all queue and timing bounds must be non-zero",
            );
        }
        Ok(Config {
            network,
            chain_id: self.chain_id,
            core,
            deployment_block: self.deployment_block,
            http_rpc_url: self.http_rpc_url,
            realtime_url: self.realtime_url,
            redis_url: self.redis_url,
            redis_namespace: self.redis_namespace,
            consumer_group: self.consumer_group,
            minimum_commitment,
            bind: self.bind,
            source_queue_bound: self.source_queue_bound,
            backfill_page_blocks: self.backfill_page_blocks,
            redis_queue_bound: self.redis_queue_bound,
            reconnect_delay: Duration::from_millis(self.reconnect_delay_milliseconds),
            source_stall_timeout: Duration::from_millis(self.source_stall_timeout_milliseconds),
            redis_timeout: Duration::from_millis(self.redis_timeout_milliseconds),
            #[cfg(all(feature = "monad-native", target_os = "linux"))]
            native_poll_interval: Duration::from_micros(self.native_poll_interval_microseconds),
            shutdown_timeout: Duration::from_secs(self.shutdown_timeout_seconds),
        })
    }
}

fn invalid<T>(field: &'static str, detail: &str) -> Result<T, ConfigError> {
    Err(invalid_value(field, detail.into()))
}

fn invalid_value(field: &'static str, detail: String) -> ConfigError {
    ConfigError::Invalid { field, detail }
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::Parser;
    use lunarbase_client::model::Commitment;

    #[test]
    fn parses_block_ordered_commitment_and_rejects_zero_bounds() {
        let valid = Cli::try_parse_from(arguments("block-ordered", "8"))
            .unwrap()
            .validate()
            .unwrap();
        assert_eq!(valid.minimum_commitment, Commitment::Canonical);
        assert_eq!(valid.backfill_page_blocks, 1_000);

        let error = Cli::try_parse_from(arguments("realtime", "0"))
            .unwrap()
            .validate()
            .unwrap_err();
        assert!(error.to_string().contains("resource_bounds"));

        let mut zero_page = arguments("finalized", "8");
        zero_page.extend(["--backfill-page-blocks", "0"]);
        let error = Cli::try_parse_from(zero_page)
            .unwrap()
            .validate()
            .unwrap_err();
        assert!(error.to_string().contains("resource_bounds"));
    }

    fn arguments<'a>(commitment: &'a str, queue_bound: &'a str) -> Vec<&'a str> {
        vec![
            "worker",
            "--network",
            "base",
            "--chain-id",
            "8453",
            "--core",
            "0x1111111111111111111111111111111111111111",
            "--deployment-block",
            "1",
            "--http-rpc-url",
            "http://localhost:8545",
            "--realtime-url",
            "ws://localhost:8546",
            "--redis-url",
            "redis://localhost:6379",
            "--minimum-commitment",
            commitment,
            "--source-queue-bound",
            queue_bound,
        ]
    }
}
