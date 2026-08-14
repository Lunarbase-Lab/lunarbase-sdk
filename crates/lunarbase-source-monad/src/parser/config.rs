//! Parser identity, delivery, and resource configuration.

use crate::execution::MonadDeliveryMode;
use lunarbase_client::model::SourceError;
use lunarbase_math::Address;
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Wire protocol exposed by the execution-events parser.
pub enum MonadParserProtocol {
    /// Best-effort decoded subscriptions retained for backwards compatibility.
    LegacyV1,
    /// Durable raw execution stream with identity, replay, and acknowledgements.
    DurableV2,
}

/// Resource and identity settings for the local Monad parser connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MonadParserConfig {
    /// Parser WebSocket subscription endpoint.
    pub ws_url: String,
    /// Core contract used to reject unrelated parser logs.
    pub core: Address,
    /// EIP-155 chain identifier attached to normalized updates.
    pub chain_id: u64,
    /// Parser wire protocol selected at startup.
    pub protocol: MonadParserProtocol,
    /// Point in proposal lifecycle at which matching logs are published.
    pub delivery_mode: MonadDeliveryMode,
    /// Retains published Core logs long enough to emit abandoned-branch removals.
    pub emit_removed_logs: bool,
    /// Maximum accepted WebSocket frame size before fail-closed recovery.
    pub max_frame_bytes: usize,
    /// Maximum notifications retained while subscription acknowledgements arrive.
    pub max_prefetched_frames: usize,
    /// Maximum proposal candidates retained between execution and finality.
    pub max_pending_proposals: usize,
    /// Maximum matching logs retained by non-realtime delivery modes.
    pub max_pending_logs: usize,
    /// Byte budget for retained matching log topics and data.
    pub max_pending_bytes: usize,
    /// Maximum records processed between protocol acknowledgements.
    pub ack_interval: u64,
}

impl Default for MonadParserConfig {
    fn default() -> Self {
        Self {
            ws_url: "ws://127.0.0.1:8080/ws/subscriptions".into(),
            core: Address::ZERO,
            chain_id: 143,
            protocol: MonadParserProtocol::LegacyV1,
            delivery_mode: MonadDeliveryMode::Realtime,
            emit_removed_logs: false,
            max_frame_bytes: 64 * 1024,
            max_prefetched_frames: 4096,
            max_pending_proposals: 64,
            max_pending_logs: 16_384,
            max_pending_bytes: 64 * 1024 * 1024,
            ack_interval: 256,
        }
    }
}

impl MonadParserConfig {
    /// Returns the production raw-stream profile; identity fields remain caller supplied.
    pub fn durable_v2() -> Self {
        Self {
            ws_url: "ws://127.0.0.1:8080/ws/v2".into(),
            protocol: MonadParserProtocol::DurableV2,
            max_frame_bytes: 1024 * 1024,
            ..Self::default()
        }
    }

    /// Validates parser endpoint, chain id, and frame-memory bounds.
    pub fn validate(&self) -> Result<(), SourceError> {
        let url = Url::parse(&self.ws_url).map_err(|error| {
            SourceError::Unavailable(format!("invalid Monad parser URL: {error}"))
        })?;
        if !matches!(url.scheme(), "ws" | "wss") {
            return Err(SourceError::Unavailable(
                "Monad parser URL must use ws or wss".into(),
            ));
        }
        if self.chain_id == 0
            || self.core == Address::ZERO
            || self.max_frame_bytes == 0
            || self.max_prefetched_frames == 0
            || self.max_pending_proposals == 0
            || self.max_pending_logs == 0
            || self.max_pending_bytes == 0
            || self.ack_interval == 0
            || self.ack_interval > 1024
        {
            return Err(SourceError::Unavailable(
                "Monad parser chain id, Core, and resource bounds must be non-zero".into(),
            ));
        }
        if self.protocol == MonadParserProtocol::DurableV2
            && !cfg!(all(feature = "protocol-v2", target_os = "linux"))
        {
            return Err(SourceError::Unavailable(
                "Monad parser protocol v2 requires the protocol-v2 feature on Linux".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_profiles_are_explicit_and_feature_independent() {
        let legacy = MonadParserConfig::default();
        assert_eq!(legacy.protocol, MonadParserProtocol::LegacyV1);
        assert!(legacy.ws_url.ends_with("/ws/subscriptions"));
        assert_eq!(legacy.max_frame_bytes, 64 * 1024);

        let durable = MonadParserConfig::durable_v2();
        assert_eq!(durable.protocol, MonadParserProtocol::DurableV2);
        assert!(durable.ws_url.ends_with("/ws/v2"));
        assert_eq!(durable.max_frame_bytes, 1024 * 1024);
    }
}
