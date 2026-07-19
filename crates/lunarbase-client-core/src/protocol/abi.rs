//! Pinned Core ABI and strict Alloy event decoding.

use crate::model::{ContractLog, LogDecodeError, QuoteEvent};
use alloy_sol_types::SolEvent;
use lunarbase_math::types::{B256, U256};

/// Generated function and event types shared by bootstrap and replay.
pub mod core {
    use alloy_sol_types::sol;

    sol! {
        function cash() external view returns (address cashAddress);
        function lane(address asset)
            external
            view
            returns (bytes32 slot0, bool exists, bool paused, uint8 blockDelay, uint32 slippageKBps);
        function reserves(address asset)
            external
            view
            returns (
                uint128 assetReserve,
                uint128 treasuryFees,
                uint128 partnerFees,
                uint128 escrowedAssets,
                uint128 totalPrincipalAmount
            );
        function whitelist(address account) external view returns (bool whitelisted);
        function blacklistFeeMultiplier() external view returns (uint256 multiplier);
        function partners(address router, address asset)
            external
            view
            returns (uint128 cumFees, uint32 fee, uint32 latestWithdrawTimestamp, address operator);

        event LaneAdded(address indexed asset);
        event LaneRemoved(address indexed asset);
        event LaneUpdated(address indexed asset, bytes32 slot0);
        event SlippageKSet(address indexed asset, uint32 previousK, uint32 newK);
        event PartnerInfoSet(
            address indexed router,
            address indexed asset,
            uint32 fee,
            address indexed operator
        );
        event PartnerFeeSet(address indexed router, address indexed asset, uint32 fee);
        event WhitelistSet(address indexed account, bool whitelisted);
        event BlacklistFeeMultiplierSet(uint256 multiplier);
        event DepositExecuted(
            uint256 indexed id,
            address indexed lpAuthority,
            address indexed asset,
            uint128 principalAmount
        );
        event WithdrawalExecuted(
            uint256 indexed id,
            address indexed lpAuthority,
            address indexed asset,
            uint128 principalAmount,
            uint256 principalOut,
            uint256 penaltyAmount,
            address principalReceiver
        );
    }
}

pub const TOPIC_LANE_ADDED: B256 = core::LaneAdded::SIGNATURE_HASH;
pub const TOPIC_LANE_REMOVED: B256 = core::LaneRemoved::SIGNATURE_HASH;
const TOPIC_LANE_UPDATED: B256 = core::LaneUpdated::SIGNATURE_HASH;
const TOPIC_SLIPPAGE_K_SET: B256 = core::SlippageKSet::SIGNATURE_HASH;
const TOPIC_PARTNER_INFO_SET: B256 = core::PartnerInfoSet::SIGNATURE_HASH;
const TOPIC_PARTNER_FEE_SET: B256 = core::PartnerFeeSet::SIGNATURE_HASH;
const TOPIC_WHITELIST_SET: B256 = core::WhitelistSet::SIGNATURE_HASH;
const TOPIC_BLACKLIST_MULTIPLIER_SET: B256 = core::BlacklistFeeMultiplierSet::SIGNATURE_HASH;
const TOPIC_DEPOSIT_EXECUTED: B256 = core::DepositExecuted::SIGNATURE_HASH;
const TOPIC_WITHDRAWAL_EXECUTED: B256 = core::WithdrawalExecuted::SIGNATURE_HASH;

/// Returns the `LaneAdded` and `LaneRemoved` topic0 values used for discovery.
pub fn lane_discovery_topics() -> [B256; 2] {
    [TOPIC_LANE_ADDED, TOPIC_LANE_REMOVED]
}

/// Returns every quote-critical Core topic accepted by the reducer.
pub fn quote_critical_topics() -> [B256; 10] {
    [
        TOPIC_LANE_ADDED,
        TOPIC_LANE_REMOVED,
        TOPIC_LANE_UPDATED,
        TOPIC_SLIPPAGE_K_SET,
        TOPIC_PARTNER_INFO_SET,
        TOPIC_PARTNER_FEE_SET,
        TOPIC_WHITELIST_SET,
        TOPIC_BLACKLIST_MULTIPLIER_SET,
        TOPIC_DEPOSIT_EXECUTED,
        TOPIC_WITHDRAWAL_EXECUTED,
    ]
}

fn expect_shape(log: &ContractLog, topics: usize, data_words: usize) -> Result<(), LogDecodeError> {
    if log.topics.len() != topics {
        return Err(LogDecodeError::InvalidTopicCount);
    }
    if log.data.len() != data_words * 32 {
        return Err(LogDecodeError::InvalidDataLength);
    }
    Ok(())
}

fn decode<E: SolEvent>(log: &ContractLog, error: LogDecodeError) -> Result<E, LogDecodeError> {
    E::decode_raw_log_validate(log.topics.iter().copied(), &log.data).map_err(|_| error)
}

/// Decodes quote-critical events with generated Alloy ABI types.
///
/// Unknown events return `Ok(None)`. Known events require exact topic and data
/// arity before Alloy validates indexed addresses, booleans, and integer widths.
pub fn decode_core_event(log: &ContractLog) -> Result<Option<QuoteEvent>, LogDecodeError> {
    let topic0 = *log.topics.first().ok_or(LogDecodeError::MissingTopic0)?;
    let event = if topic0 == TOPIC_LANE_ADDED {
        expect_shape(log, 2, 0)?;
        let event = decode::<core::LaneAdded>(log, LogDecodeError::InvalidAddress)?;
        Some(QuoteEvent::LaneAdded { asset: event.asset })
    } else if topic0 == TOPIC_LANE_REMOVED {
        expect_shape(log, 2, 0)?;
        let event = decode::<core::LaneRemoved>(log, LogDecodeError::InvalidAddress)?;
        Some(QuoteEvent::LaneRemoved { asset: event.asset })
    } else if topic0 == TOPIC_LANE_UPDATED {
        expect_shape(log, 2, 1)?;
        let event = decode::<core::LaneUpdated>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::LaneUpdated {
            asset: event.asset,
            slot0: U256::from_be_slice(event.slot0.as_slice()),
        })
    } else if topic0 == TOPIC_SLIPPAGE_K_SET {
        expect_shape(log, 2, 2)?;
        let event = decode::<core::SlippageKSet>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::SlippageKSet {
            asset: event.asset,
            new_k: U256::from(event.newK),
        })
    } else if topic0 == TOPIC_PARTNER_INFO_SET {
        expect_shape(log, 4, 1)?;
        let event = decode::<core::PartnerInfoSet>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::PartnerInfoSet {
            router: event.router,
            asset: event.asset,
            fee: U256::from(event.fee),
        })
    } else if topic0 == TOPIC_PARTNER_FEE_SET {
        expect_shape(log, 3, 1)?;
        let event = decode::<core::PartnerFeeSet>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::PartnerFeeSet {
            router: event.router,
            asset: event.asset,
            fee: U256::from(event.fee),
        })
    } else if topic0 == TOPIC_WHITELIST_SET {
        expect_shape(log, 2, 1)?;
        let event = decode::<core::WhitelistSet>(log, LogDecodeError::InvalidBoolean)?;
        Some(QuoteEvent::WhitelistSet {
            router: event.account,
            whitelisted: event.whitelisted,
        })
    } else if topic0 == TOPIC_BLACKLIST_MULTIPLIER_SET {
        expect_shape(log, 1, 1)?;
        let event =
            decode::<core::BlacklistFeeMultiplierSet>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::BlacklistFeeMultiplierSet {
            multiplier: event.multiplier,
        })
    } else if topic0 == TOPIC_DEPOSIT_EXECUTED {
        expect_shape(log, 4, 1)?;
        let event = decode::<core::DepositExecuted>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::DepositExecuted {
            asset: event.asset,
            principal: U256::from(event.principalAmount),
        })
    } else if topic0 == TOPIC_WITHDRAWAL_EXECUTED {
        expect_shape(log, 4, 4)?;
        let event = decode::<core::WithdrawalExecuted>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::WithdrawalExecuted {
            asset: event.asset,
            principal: U256::from(event.principalAmount),
        })
    } else {
        None
    };
    Ok(event)
}

#[cfg(test)]
mod tests {
    use crate::model::{ChainCursor, Commitment, ContractLog, QuoteEvent};
    use crate::protocol::abi::{
        TOPIC_DEPOSIT_EXECUTED, TOPIC_WITHDRAWAL_EXECUTED, decode_core_event,
    };
    use lunarbase_math::types::{Address, B256, U256};

    #[test]
    fn generated_topics_match_the_pinned_solidity_abi() {
        assert_eq!(
            format!("{TOPIC_DEPOSIT_EXECUTED:#x}"),
            "0x9fb4891ffe3e11f428f3f10fa362b7938a364beebc215a2ec1db56a8d05ba20f"
        );
        assert_eq!(
            format!("{TOPIC_WITHDRAWAL_EXECUTED:#x}"),
            "0x722ca578dc087cbf283dc08891a94ba45b7119723568feda14da8f4c9c35d251"
        );
    }

    #[test]
    fn withdrawal_accepts_the_complete_four_word_payload() {
        let asset = Address::new([0x11; 20]);
        let mut asset_topic = [0_u8; 32];
        asset_topic[12..].copy_from_slice(asset.as_slice());
        let mut data = vec![0_u8; 4 * 32];
        data[31] = 7;
        let log = ContractLog {
            address: Address::new([0x22; 20]),
            topics: vec![
                TOPIC_WITHDRAWAL_EXECUTED,
                B256::from(U256::ONE.to_be_bytes::<32>()),
                B256::from(U256::from(2_u8).to_be_bytes::<32>()),
                B256::from(asset_topic),
            ],
            data: data.into(),
            removed: false,
            cursor: ChainCursor {
                chain_id: 1,
                block_number: 1,
                execution_block_number: 1,
                block_hash: Some(B256::new([0x33; 32])),
                transaction_index: Some(0),
                log_index: Some(0),
                source_sequence: Some(1),
                source_sub_index: Some(0),
                commitment: Commitment::Canonical,
            },
        };

        assert_eq!(
            decode_core_event(&log).unwrap(),
            Some(QuoteEvent::WithdrawalExecuted {
                asset,
                principal: U256::from(7_u8),
            })
        );
    }
}
