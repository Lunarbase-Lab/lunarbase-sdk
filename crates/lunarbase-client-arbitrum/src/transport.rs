//! Arbitrum Nitro wrapper around the executed-state JSON-RPC WebSocket.
//!
//! Nitro's `newHeads` payload may include `l1BlockNumber`, the block number
//! visible to the EVM `NUMBER` opcode.  When block-delay semantics are used,
//! accepting a head without that context would make the quote predicate
//! unverifiable, so this adapter fails closed.

use async_stream::stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use lunarbase_client_core::{
    BackfillRequest, ChainCursor, ChainUpdate, ContractFilter, ContractLog, Network,
    NormalizedBackend, RpcHttpClient, SourceError, SourceStream, WsRpcBackend, WsRpcConfig,
};

#[derive(Clone)]
pub struct ArbitrumNitroBackend {
    inner: WsRpcBackend,
    require_evm_parent_context: bool,
}

impl ArbitrumNitroBackend {
    /// Creates a fail-closed Nitro backend.
    ///
    /// By default, a realtime head is accepted only when the provider includes
    /// the EVM-visible parent context (`l1BlockNumber`). This prevents a
    /// block-delay quote policy from being reported as fresh when its execution
    /// predicate cannot be proven.
    pub fn new(rpc: RpcHttpClient, ws_endpoint: impl Into<String>, chain_id: u64) -> Self {
        Self::with_config(rpc, ws_endpoint, chain_id, WsRpcConfig::default())
    }

    /// Creates a Nitro backend with explicit WebSocket resource limits.
    pub fn with_config(
        rpc: RpcHttpClient,
        ws_endpoint: impl Into<String>,
        chain_id: u64,
        config: WsRpcConfig,
    ) -> Self {
        Self {
            inner: WsRpcBackend::with_config(
                rpc,
                ws_endpoint,
                Network::Arbitrum,
                chain_id,
                "finalized",
                config,
            ),
            require_evm_parent_context: true,
        }
    }

    /// Allows heads without Nitro's EVM-parent context.
    ///
    /// Use this only when the caller does not rely on block-delay semantics;
    /// the default is deliberately conservative.
    pub fn allow_missing_evm_parent_context(mut self) -> Self {
        self.require_evm_parent_context = false;
        self
    }

    /// Returns the underlying generic Ethereum WebSocket backend.
    pub fn inner(&self) -> &WsRpcBackend {
        &self.inner
    }
}

#[async_trait]
impl NormalizedBackend for ArbitrumNitroBackend {
    async fn snapshot_cursor(&self, network: Network) -> Result<ChainCursor, SourceError> {
        self.inner.snapshot_cursor(network).await
    }

    async fn backfill(&self, request: BackfillRequest) -> Result<Vec<ContractLog>, SourceError> {
        self.inner.backfill(request).await
    }

    async fn subscribe(
        &self,
        network: Network,
        filter: ContractFilter,
    ) -> Result<SourceStream, SourceError> {
        if network != Network::Arbitrum {
            return Err(SourceError::NetworkMismatch);
        }
        let stream = self.inner.subscribe(network, filter).await?;
        let require_context = self.require_evm_parent_context;
        let output = stream! {
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                match item {
                    Ok(ChainUpdate::Head(cursor)) if require_context && cursor.source_sequence.is_none() => {
                        yield Ok(ChainUpdate::Gap {
                            cursor: Some(cursor),
                            reason: "Arbitrum Nitro head omitted l1BlockNumber/EVM parent context".into(),
                        });
                        break;
                    }
                    Ok(update) => yield Ok(update),
                    Err(error) => yield Err(error),
                }
            }
        };
        Ok(Box::pin(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nitro_backend_defaults_to_fail_closed_parent_context() {
        let backend = ArbitrumNitroBackend::new(
            RpcHttpClient::new("http://127.0.0.1:8545"),
            "ws://127.0.0.1:8546",
            42161,
        );
        assert!(backend.require_evm_parent_context);
        assert!(
            !backend
                .clone()
                .allow_missing_evm_parent_context()
                .require_evm_parent_context
        );
    }
}
