//! TOML configuration and validation.

use lunarbase_client_core::{DeploymentConfig, Network, RedisConfig, MATH_COMPATIBILITY_VERSION};
use lunarbase_math::Address;
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

include!("config/types.rs");
include!("config/validation.rs");
include!("config/parsing.rs");

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
