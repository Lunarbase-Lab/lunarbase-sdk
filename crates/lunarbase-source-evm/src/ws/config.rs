//! Delivery semantics and transport bounds for EVM subscriptions.

use super::EvmRpcSource;
use crate::rpc::{backend::RpcHttpBackend, client::RpcHttpClient};
use lunarbase_client::model::{Commitment, Network, SourceError};
use std::sync::Arc;

/// Ordering and confidence policy applied before updates leave the source.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EvmDeliveryMode {
    /// Publish logs and heads immediately in provider receive order.
    Realtime,
    /// Close each executed block, then publish its logs in transaction order.
    #[default]
    BlockOrdered,
    /// Publish only logs at or below the provider's finalized watermark.
    Finalized,
}

impl EvmDeliveryMode {
    pub(super) const fn snapshot_tag(self) -> &'static str {
        match self {
            Self::Finalized => "finalized",
            Self::Realtime | Self::BlockOrdered => "latest",
        }
    }

    pub(super) const fn commitment(self) -> Commitment {
        match self {
            Self::Realtime => Commitment::Realtime,
            Self::BlockOrdered => Commitment::Canonical,
            Self::Finalized => Commitment::Finalized,
        }
    }
}

/// Bounds transport frames, buffered updates, and finalized catch-up pages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WsRpcConfig {
    /// Maximum accepted WebSocket frame size before the stream fails closed.
    pub max_frame_bytes: usize,
    /// Maximum normalized updates retained while waiting for an ordering watermark.
    pub reorder_capacity: usize,
    /// Ethereum subscription method, either `logs` or provider-specific `pendingLogs`.
    pub logs_subscription: String,
    /// Accept multiple monotonically sequenced heads at one block height.
    pub progressive_heads: bool,
    /// Ordering and confidence policy applied to published updates.
    pub delivery_mode: EvmDeliveryMode,
    /// Maximum block span fetched per finalized catch-up request.
    pub backfill_page_blocks: u64,
}

impl Default for WsRpcConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: 256 * 1024,
            reorder_capacity: 4096,
            logs_subscription: "logs".into(),
            progressive_heads: false,
            delivery_mode: EvmDeliveryMode::BlockOrdered,
            backfill_page_blocks: 1_000,
        }
    }
}

impl WsRpcConfig {
    /// Returns the official Base `pendingLogs + newHeads` profile.
    pub fn base_flashblocks() -> Self {
        Self {
            logs_subscription: "pendingLogs".into(),
            progressive_heads: true,
            delivery_mode: EvmDeliveryMode::Realtime,
            ..Self::default()
        }
    }

    /// Rejects zero resource bounds and unsupported subscription methods.
    pub fn validate(&self) -> Result<(), SourceError> {
        if self.max_frame_bytes == 0
            || self.reorder_capacity == 0
            || self.backfill_page_blocks == 0
            || !matches!(self.logs_subscription.as_str(), "logs" | "pendingLogs")
        {
            return Err(SourceError::Unavailable(
                "WS frame, reorder, and backfill bounds must be non-zero".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn holds_standard_logs_until_successor(&self) -> bool {
        self.delivery_mode == EvmDeliveryMode::BlockOrdered && self.logs_subscription == "logs"
    }
}

impl EvmRpcSource {
    /// Creates a standard backend, inferring finalized delivery from its snapshot tag.
    pub fn new(
        rpc: RpcHttpClient,
        ws_endpoint: impl Into<String>,
        network: Network,
        chain_id: u64,
        snapshot_tag: impl Into<String>,
    ) -> Self {
        let snapshot_tag = snapshot_tag.into();
        let mut config = WsRpcConfig::default();
        if snapshot_tag == "finalized" {
            config.delivery_mode = EvmDeliveryMode::Finalized;
        }
        Self::with_config(rpc, ws_endpoint, network, chain_id, snapshot_tag, config)
    }

    /// Creates a source whose snapshot, recovery, and live delivery share one policy.
    pub fn with_delivery_mode(
        rpc: RpcHttpClient,
        ws_endpoint: impl Into<String>,
        network: Network,
        chain_id: u64,
        delivery_mode: EvmDeliveryMode,
    ) -> Self {
        let config = WsRpcConfig {
            delivery_mode,
            ..WsRpcConfig::default()
        };
        Self::with_config(
            rpc,
            ws_endpoint,
            network,
            chain_id,
            delivery_mode.snapshot_tag(),
            config,
        )
    }

    /// Creates the official Base `pendingLogs` source.
    pub fn base_flashblocks(
        rpc: RpcHttpClient,
        ws_endpoint: impl Into<String>,
        chain_id: u64,
    ) -> Self {
        Self::with_config(
            rpc,
            ws_endpoint,
            Network::Base,
            chain_id,
            "latest",
            WsRpcConfig::base_flashblocks(),
        )
    }

    /// Creates a WebSocket backend with explicit resource limits.
    pub fn with_config(
        rpc: RpcHttpClient,
        ws_endpoint: impl Into<String>,
        network: Network,
        chain_id: u64,
        snapshot_tag: impl Into<String>,
        config: WsRpcConfig,
    ) -> Self {
        Self {
            http: RpcHttpBackend::new(rpc, network, chain_id, snapshot_tag),
            ws_endpoint: Arc::from(ws_endpoint.into()),
            config,
        }
    }

    /// Returns the configured WebSocket endpoint.
    pub fn endpoint(&self) -> &str {
        &self.ws_endpoint
    }

    /// Returns the transport and delivery policy.
    pub fn config(&self) -> &WsRpcConfig {
        &self.config
    }

    /// Overrides the bounded block span used by finalized live catch-up.
    pub fn with_backfill_page_blocks(mut self, blocks: u64) -> Self {
        self.config.backfill_page_blocks = blocks;
        self
    }

    pub(super) fn validate_config(&self) -> Result<(), SourceError> {
        self.config.validate()?;
        let finalized_tag = self.http.snapshot_tag() == "finalized";
        if (self.config.delivery_mode == EvmDeliveryMode::Finalized) != finalized_tag {
            return Err(SourceError::Unavailable(
                "EVM delivery mode and HTTP snapshot tag disagree".into(),
            ));
        }
        Ok(())
    }
}
