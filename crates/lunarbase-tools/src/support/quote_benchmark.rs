//! Deterministic quote topology shared by performance measurements.

use lunarbase_client::model::MATH_COMPATIBILITY_VERSION;
use lunarbase_client::prelude::{
    BootstrapSnapshot, ChainCursor, ClientConnectConfig, Commitment, ContractFilter,
    DeploymentConfig, Network,
};
use lunarbase_math::slot0::{LaneSlot0, encode_lane_slot0};
use lunarbase_math::{
    Address, B256, FeeClass, LaneState, QuoteMode, QuoteRequest, QuoteState, U256, WAD,
};
use std::collections::HashMap;
use std::time::Duration;

const CHAIN_ID: u64 = 8453;
const EXECUTION_BLOCK: u64 = 10_000;
const CASH_ID: u64 = 1;
const CORE_ID: u64 = 2;
const IMPLEMENTATION_ID: u64 = 4;
const FIRST_LANE_ID: u64 = 100;
const RESERVE: u128 = 1_000_000_000_000_000_000_000_000_000_000;
const PRINCIPAL: u128 = 1_000_000_000_000_000_000_000_000_000;

#[derive(Clone, Debug)]
/// Fully initialized deterministic inputs for one quote benchmark topology.
pub struct QuoteBenchmarkFixture {
    /// Connected-client configuration matching the synthetic snapshot.
    pub connect: ClientConnectConfig,
    /// Snapshot returned by the synthetic source during bootstrap.
    pub snapshot: BootstrapSnapshot,
    /// Distinct available quote requests exercised by the benchmark.
    pub requests: Vec<QuoteRequest>,
}

/// Builds a quote topology whose requested pairs all execute successfully.
///
/// # Errors
///
/// Returns an error when fewer than two lanes are requested, the requested
/// pair count exceeds the number of distinct directed pairs, or lane packing
/// fails.
pub fn fixture(lanes: usize, pairs: usize) -> Result<QuoteBenchmarkFixture, String> {
    if lanes < 2 {
        return Err("quote benchmark requires at least two lanes".into());
    }
    let maximum_pairs = lanes
        .checked_mul(lanes + 1)
        .ok_or_else(|| "quote benchmark topology is too large".to_string())?;
    if pairs == 0 || pairs > maximum_pairs {
        return Err(format!(
            "pairs must be in 1..={maximum_pairs} for {lanes} lanes"
        ));
    }

    let cash = address(CASH_ID);
    let lane_assets = (0..lanes)
        .map(|index| address(FIRST_LANE_ID + index as u64))
        .collect::<Vec<_>>();
    let mut lane_states = HashMap::with_capacity(lanes);
    for (index, asset) in lane_assets.iter().copied().enumerate() {
        let price = u128::try_from(WAD).map_err(|error| error.to_string())?
            + (index as u128 % 25) * 1_000_000_000_000_000;
        let slot0 = encode_lane_slot0(&LaneSlot0 {
            price,
            ask_fee_bps: 3_000,
            bid_fee_bps: 2_000,
            latest_update_block: EXECUTION_BLOCK,
            exists: true,
            block_delay: u8::MAX,
            slippage_k_bps: 1_000,
            ..Default::default()
        })
        .map_err(|error| error.to_string())?;
        lane_states.insert(asset, LaneState::new(slot0, RESERVE, PRINCIPAL));
    }
    let state = QuoteState {
        cash,
        cash_reserve: RESERVE,
        lanes: lane_states,
        blacklist_fee_multiplier: U256::ONE,
    };
    let implementation = address(IMPLEMENTATION_ID);
    let implementation_code_hash = B256::new([7; 32]);
    let cursor = ChainCursor::block(
        CHAIN_ID,
        EXECUTION_BLOCK,
        Some(B256::new([8; 32])),
        Commitment::Finalized,
    );
    let core = address(CORE_ID);
    let connect = ClientConnectConfig {
        deployment: DeploymentConfig {
            network: Network::Base,
            chain_id: CHAIN_ID,
            core,
            fee_class: FeeClass::Whitelisted,
            verified_router: None,
            deployment_block: 1,
            expected_implementation: implementation,
            expected_implementation_code_hash: implementation_code_hash,
            contract_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            explicit_lane_assets: lane_assets.clone(),
        },
        filter: ContractFilter {
            address: core,
            topics: Vec::new(),
        },
        buffer_capacity: 4_096,
        reconnect_delay: Duration::from_secs(1),
        source_stall_timeout: Duration::from_secs(3_600),
    };
    let requests = quote_requests(cash, &lane_assets, pairs);
    debug_assert_eq!(requests.len(), pairs);
    Ok(QuoteBenchmarkFixture {
        connect,
        snapshot: BootstrapSnapshot {
            state,
            cursor,
            implementation,
            implementation_code_hash,
            verified_router: None,
        },
        requests,
    })
}

/// Prebuilds every rotating request batch outside the measured region.
///
/// # Errors
///
/// Returns an error for an empty request set or a batch outside `1..=256`.
pub fn rotating_batches(
    requests: &[QuoteRequest],
    batch_size: usize,
) -> Result<Vec<Vec<QuoteRequest>>, String> {
    if requests.is_empty() {
        return Err("quote benchmark requires at least one request".into());
    }
    if !(1..=256).contains(&batch_size) {
        return Err("batch size must be in 1..=256".into());
    }
    Ok((0..requests.len())
        .map(|start| {
            (0..batch_size)
                .map(|offset| requests[(start + offset) % requests.len()].clone())
                .collect()
        })
        .collect())
}

fn quote_requests(cash: Address, lanes: &[Address], pairs: usize) -> Vec<QuoteRequest> {
    let mut assets = Vec::with_capacity(pairs);
    for offset in 1..lanes.len() {
        for index in 0..lanes.len() {
            assets.push((lanes[index], lanes[(index + offset) % lanes.len()]));
            if assets.len() == pairs {
                return typed_requests(assets);
            }
        }
        if offset == 1 {
            for asset in lanes.iter().copied() {
                assets.push((cash, asset));
                if assets.len() == pairs {
                    return typed_requests(assets);
                }
                assets.push((asset, cash));
                if assets.len() == pairs {
                    return typed_requests(assets);
                }
            }
        }
    }
    typed_requests(assets)
}

fn typed_requests(pairs: Vec<(Address, Address)>) -> Vec<QuoteRequest> {
    pairs
        .into_iter()
        .enumerate()
        .map(|(index, (asset_in, asset_out))| QuoteRequest {
            asset_in,
            asset_out,
            amount: U256::from(1_000_000_000_000_000_000u64 + index as u64 * 1_000_000),
            mode: if index % 2 == 0 {
                QuoteMode::ExactIn
            } else {
                QuoteMode::ExactOut
            },
        })
        .collect()
}

fn address(value: u64) -> Address {
    let mut bytes = [0; 20];
    bytes[12..].copy_from_slice(&value.to_be_bytes());
    Address::new(bytes)
}

#[cfg(test)]
mod tests {
    use super::{fixture, rotating_batches};
    use std::collections::HashSet;

    #[test]
    fn fixture_pairs_are_distinct_and_cover_every_lane() {
        for lanes in [15, 64] {
            let fixture = fixture(lanes, 100).unwrap();
            let pairs = fixture
                .requests
                .iter()
                .map(|request| (request.asset_in, request.asset_out))
                .collect::<HashSet<_>>();
            assert_eq!(pairs.len(), 100);
            for asset in &fixture.connect.deployment.explicit_lane_assets {
                assert!(
                    pairs
                        .iter()
                        .any(|(input, output)| input == asset || output == asset)
                );
            }
        }
    }

    #[test]
    fn rotating_batches_have_the_requested_shape() {
        let fixture = fixture(15, 100).unwrap();
        for batch_size in [1, 16, 256] {
            let batches = rotating_batches(&fixture.requests, batch_size).unwrap();
            assert_eq!(batches.len(), 100);
            assert!(batches.iter().all(|batch| batch.len() == batch_size));
        }
    }
}
