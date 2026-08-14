//! Coherent block-tagged reconstruction of quote-critical Core state.

use crate::rpc::client::RpcHttpClient;
use alloy_primitives::Bytes;
use alloy_sol_types::SolCall;
use futures_util::{StreamExt, TryStreamExt, stream};
use lunarbase_client::bootstrap::{BootstrapSnapshot, VerifiedRouterSnapshot};
use lunarbase_client::model::{
    BackfillRequest, Commitment, ContractFilter, DeploymentConfig, QuoteEvent, SourceError,
};
use lunarbase_client::protocol::abi::{core, decode_core_event, lane_discovery_topics};
use lunarbase_client::protocol::proxy::{ERC1967_IMPLEMENTATION_SLOT, decode_implementation};
use lunarbase_math::arithmetic::BPS;
use lunarbase_math::{Address, U256};
use lunarbase_math::{LaneState, QuoteState};
use std::{collections::BTreeSet, sync::Arc};

const SNAPSHOT_CONCURRENCY: usize = 16;

#[derive(Clone)]
/// Produces one coherent, block-tagged quote state from Core view calls.
pub struct RpcSnapshotProvider {
    /// Read-only Alloy client used for Core views, code, heads, and lane discovery.
    rpc: RpcHttpClient,
    /// Block tag applied to every view in one snapshot operation.
    snapshot_tag: Arc<str>,
}

impl RpcSnapshotProvider {
    /// Creates a block-tagged snapshot provider (`finalized` or `latest`).
    pub fn new(rpc: RpcHttpClient, snapshot_tag: impl Into<String>) -> Self {
        Self {
            rpc,
            snapshot_tag: Arc::from(snapshot_tag.into()),
        }
    }

    /// Returns the RPC client used for snapshot calls.
    pub fn rpc(&self) -> &RpcHttpClient {
        &self.rpc
    }

    /// Reads chain-wide quote state and optional verified-router accounting.
    pub async fn snapshot(
        &self,
        config: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        config.validate()?;
        let rpc_chain_id = self.rpc.chain_id().await?;
        if rpc_chain_id != config.chain_id {
            return Err(SourceError::Unavailable(format!(
                "HTTP RPC chain id mismatch: expected {}, got {rpc_chain_id}",
                config.chain_id
            )));
        }
        let commitment = if self.snapshot_tag.as_ref() == "finalized" {
            Commitment::Finalized
        } else {
            Commitment::Canonical
        };
        let cursor = self
            .rpc
            .block_cursor(&self.snapshot_tag, config.chain_id, commitment)
            .await?;
        let block_hash = cursor.block_hash.ok_or_else(|| {
            SourceError::Unavailable("snapshot block has no canonical hash".into())
        })?;
        if cursor.block_number < config.deployment_block {
            return Err(SourceError::Unavailable(
                "snapshot block precedes deployment block".into(),
            ));
        }
        let block_tag = format!("0x{:x}", cursor.block_number);
        let implementation_word = self
            .rpc
            .get_storage_at_hash(config.core, ERC1967_IMPLEMENTATION_SLOT, block_hash)
            .await?;
        let implementation = decode_implementation(implementation_word).ok_or_else(|| {
            SourceError::Unavailable("Core has an invalid ERC-1967 implementation slot".into())
        })?;
        if implementation != config.expected_implementation {
            return Err(SourceError::Unavailable(
                "Core implementation does not match deployment config".into(),
            ));
        }
        let implementation_code_hash = self
            .rpc
            .runtime_code_hash_at_hash(implementation, block_hash)
            .await?;
        if implementation_code_hash != config.expected_implementation_code_hash {
            return Err(SourceError::Unavailable(
                "implementation code hash does not match deployment config".into(),
            ));
        }

        let assets = self
            .resolve_lane_assets(config, cursor.block_number)
            .await?;
        let (cash, blacklist_fee_multiplier) = tokio::try_join!(
            self.read(config.core, core::cashCall {}, block_hash),
            self.read(config.core, core::blacklistFeeMultiplierCall {}, block_hash,),
        )?;
        let mut state = QuoteState {
            cash,
            blacklist_fee_multiplier,
            ..Default::default()
        };

        let lane_states = stream::iter(assets.iter().copied())
            .map(|asset| async move {
                let (lane, reserves) = tokio::try_join!(
                    self.read(config.core, core::laneCall { asset }, block_hash),
                    self.read(config.core, core::reservesCall { asset }, block_hash),
                )?;
                Ok::<_, SourceError>((
                    asset,
                    LaneState::new(
                        U256::from_be_slice(lane.as_slice()),
                        reserves.assetReserve,
                        reserves.totalPrincipalAmount,
                    ),
                ))
            })
            .buffer_unordered(SNAPSHOT_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        if !config.explicit_lane_assets.is_empty()
            && lane_states.iter().any(|(_, lane)| !lane.exists())
        {
            return Err(SourceError::Unavailable(
                "explicit lane asset is not active at the snapshot block".into(),
            ));
        }
        state.lanes.extend(lane_states);
        state.cash_reserve = self
            .read(config.core, core::reservesCall { asset: cash }, block_hash)
            .await?
            .assetReserve;

        let verified_router = if let Some(router) = config.verified_router {
            let whitelisted = self
                .read(
                    config.core,
                    core::whitelistCall { account: router },
                    block_hash,
                )
                .await?;
            if whitelisted != config.fee_class.is_whitelisted() {
                return Err(SourceError::Unavailable(format!(
                    "verified router fee class mismatch: expected {}, got {whitelisted}",
                    config.fee_class.is_whitelisted()
                )));
            }
            let mut partner_assets = assets;
            if !partner_assets.contains(&cash) {
                partner_assets.push(cash);
            }
            let partner_fees = stream::iter(partner_assets)
                .map(|asset| async move {
                    let partner = self
                        .read(
                            config.core,
                            core::partnersCall { router, asset },
                            block_hash,
                        )
                        .await?;
                    if U256::from(partner.fee) > BPS {
                        return Err(SourceError::Unavailable("partner fee exceeds BPS".into()));
                    }
                    Ok((asset, partner.fee))
                })
                .buffer_unordered(SNAPSHOT_CONCURRENCY)
                .try_collect()
                .await?;
            Some(VerifiedRouterSnapshot {
                router,
                partner_fee_bps: partner_fees,
            })
        } else {
            None
        };
        let verified = self
            .rpc
            .block_cursor(&block_tag, config.chain_id, commitment)
            .await?;
        if verified.block_hash != Some(block_hash) {
            return Err(SourceError::Unavailable(
                "snapshot block changed while state was reconstructed".into(),
            ));
        }
        Ok(BootstrapSnapshot {
            state,
            cursor,
            implementation,
            implementation_code_hash,
            verified_router,
        })
    }

    async fn read<C: SolCall>(
        &self,
        core: Address,
        call: C,
        block_hash: lunarbase_math::B256,
    ) -> Result<C::Return, SourceError> {
        let response = self
            .rpc
            .call_at_hash(core, Bytes::from(call.abi_encode()), block_hash)
            .await?;
        C::abi_decode_returns_validate(&response).map_err(|error| {
            SourceError::Unavailable(format!("invalid Core ABI response: {error}"))
        })
    }

    async fn resolve_lane_assets(
        &self,
        config: &DeploymentConfig,
        snapshot_block: u64,
    ) -> Result<Vec<Address>, SourceError> {
        if !config.explicit_lane_assets.is_empty() {
            return Ok(config.explicit_lane_assets.clone());
        }
        let request = BackfillRequest {
            from_block: config.deployment_block,
            to_block: snapshot_block,
            filter: ContractFilter {
                address: config.core,
                topics: lane_discovery_topics().to_vec(),
            },
        };
        let mut history = self
            .rpc
            .get_logs(&request, config.chain_id, Commitment::Canonical)
            .await?;
        history.sort_by_key(|log| log.cursor.event_order());
        let mut discovered = BTreeSet::new();
        for log in history {
            match decode_core_event(&log)
                .map_err(|error| SourceError::Unavailable(error.to_string()))?
            {
                Some(QuoteEvent::LaneAdded { asset }) => {
                    discovered.insert(asset);
                }
                Some(QuoteEvent::LaneRemoved { asset }) => {
                    discovered.remove(&asset);
                }
                _ => {}
            }
        }
        Ok(discovered.into_iter().collect())
    }
}
