//! Coherent block-tagged reconstruction of quote-critical Core state.

use crate::bootstrap::BootstrapSnapshot;
use crate::model::{
    BackfillRequest, Commitment, ContractFilter, DeploymentConfig, QuoteEvent, SourceError,
};
use crate::protocol::abi::{core, decode_core_event, lane_discovery_topics};
use crate::transport::rpc::client::RpcHttpClient;
use alloy_primitives::{Bytes, keccak256};
use alloy_sol_types::SolCall;
use lunarbase_math::arithmetic::BPS;
use lunarbase_math::state::{LaneState, QuoteState};
use lunarbase_math::types::{Address, B256, U256};
use std::{collections::BTreeSet, sync::Arc};

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

    /// Reads a coherent quote snapshot for the configured router profile.
    pub async fn snapshot(
        &self,
        config: &DeploymentConfig,
    ) -> Result<BootstrapSnapshot, SourceError> {
        config.validate()?;
        let commitment = if self.snapshot_tag.as_ref() == "finalized" {
            Commitment::Finalized
        } else {
            Commitment::Canonical
        };
        let cursor = self
            .rpc
            .block_cursor(&self.snapshot_tag, config.chain_id, commitment)
            .await?;
        if cursor.block_number < config.deployment_block {
            return Err(SourceError::Unavailable(
                "snapshot block precedes deployment block".into(),
            ));
        }
        let code = self.rpc.get_code(config.core, &self.snapshot_tag).await?;
        let runtime_code_hash = keccak256(&code);
        if config.expected_runtime_code_hash != B256::ZERO
            && runtime_code_hash != config.expected_runtime_code_hash
        {
            return Err(SourceError::Unavailable(
                "runtime code hash does not match deployment config".into(),
            ));
        }

        let assets = self
            .resolve_lane_assets(config, cursor.block_number)
            .await?;
        let cash = self.read(config.core, core::cashCall {}).await?;
        let whitelist = self
            .read(
                config.core,
                core::whitelistCall {
                    account: config.router,
                },
            )
            .await?;
        if whitelist != config.expect_whitelisted {
            return Err(SourceError::Unavailable(format!(
                "configured router whitelist status mismatch: expected {}, got {}",
                config.expect_whitelisted, whitelist
            )));
        }
        let blacklist_fee_multiplier = self
            .read(config.core, core::blacklistFeeMultiplierCall {})
            .await?;
        let mut state = QuoteState {
            cash,
            ..Default::default()
        };
        state.fee_profile.whitelisted = whitelist;
        state.fee_profile.blacklist_fee_multiplier = blacklist_fee_multiplier;

        for asset in &assets {
            let (lane, reserves) = tokio::try_join!(
                self.read(config.core, core::laneCall { asset: *asset }),
                self.read(config.core, core::reservesCall { asset: *asset }),
            )?;
            state.lanes.insert(
                *asset,
                LaneState::new(
                    U256::from_be_slice(lane.slot0.as_slice()),
                    reserves.totalPrincipalAmount,
                    lane.slippageKBps,
                    lane.blockDelay,
                    lane.exists,
                    lane.paused,
                ),
            );
        }

        let mut partner_assets = assets.clone();
        if !partner_assets.contains(&cash) {
            partner_assets.push(cash);
        }
        for asset in partner_assets {
            let partner = self
                .read(
                    config.core,
                    core::partnersCall {
                        router: config.router,
                        asset,
                    },
                )
                .await?;
            if U256::from(partner.fee) > BPS {
                return Err(SourceError::Unavailable("partner fee exceeds BPS".into()));
            }
            state.fee_profile.partner_fee_bps.insert(asset, partner.fee);
        }
        Ok(BootstrapSnapshot {
            state,
            cursor,
            runtime_code_hash,
        })
    }

    async fn read<C: SolCall>(&self, core: Address, call: C) -> Result<C::Return, SourceError> {
        let response = self
            .rpc
            .call_at(
                core,
                Bytes::from(call.abi_encode()),
                self.snapshot_tag.as_ref(),
            )
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
        if config.explicit_lane_assets.is_empty() {
            return Ok(discovered.into_iter().collect());
        }
        if config
            .explicit_lane_assets
            .iter()
            .any(|asset| !discovered.contains(asset))
        {
            return Err(SourceError::Unavailable(
                "explicit lane asset was not active in deployment history".into(),
            ));
        }
        Ok(config.explicit_lane_assets.clone())
    }
}
