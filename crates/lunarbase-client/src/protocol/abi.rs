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
            returns (bytes32 laneWord);
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
        event LanePausedSet(address indexed asset, bool previousPaused, bool newPaused);
        event PricePushThresholdSet(
            address indexed asset,
            uint8 previousThreshold,
            uint8 newThreshold,
            bool previousEnabled,
            bool newEnabled
        );
        event SlippageKSet(address indexed asset, uint32 previousK, uint32 newK);
        event BlockDelaySet(address indexed asset, uint8 previousBlockDelay, uint8 newBlockDelay);
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
        event Sync(address indexed lane, uint128 assetReserve, uint128 cashReserve);
        event Upgraded(address indexed implementation);
    }
}

/// Event signature used to discover newly configured lanes.
pub const TOPIC_LANE_ADDED: B256 = core::LaneAdded::SIGNATURE_HASH;
/// Event signature used to remove lanes during discovery replay.
pub const TOPIC_LANE_REMOVED: B256 = core::LaneRemoved::SIGNATURE_HASH;
const TOPIC_LANE_UPDATED: B256 = core::LaneUpdated::SIGNATURE_HASH;
const TOPIC_LANE_PAUSED_SET: B256 = core::LanePausedSet::SIGNATURE_HASH;
const TOPIC_PRICE_PUSH_THRESHOLD_SET: B256 = core::PricePushThresholdSet::SIGNATURE_HASH;
const TOPIC_SLIPPAGE_K_SET: B256 = core::SlippageKSet::SIGNATURE_HASH;
const TOPIC_BLOCK_DELAY_SET: B256 = core::BlockDelaySet::SIGNATURE_HASH;
const TOPIC_PARTNER_INFO_SET: B256 = core::PartnerInfoSet::SIGNATURE_HASH;
const TOPIC_PARTNER_FEE_SET: B256 = core::PartnerFeeSet::SIGNATURE_HASH;
const TOPIC_WHITELIST_SET: B256 = core::WhitelistSet::SIGNATURE_HASH;
const TOPIC_BLACKLIST_MULTIPLIER_SET: B256 = core::BlacklistFeeMultiplierSet::SIGNATURE_HASH;
const TOPIC_DEPOSIT_EXECUTED: B256 = core::DepositExecuted::SIGNATURE_HASH;
const TOPIC_WITHDRAWAL_EXECUTED: B256 = core::WithdrawalExecuted::SIGNATURE_HASH;
const TOPIC_SYNC: B256 = core::Sync::SIGNATURE_HASH;
const TOPIC_UPGRADED: B256 = core::Upgraded::SIGNATURE_HASH;

/// Returns the `LaneAdded` and `LaneRemoved` topic0 values used for discovery.
pub fn lane_discovery_topics() -> [B256; 2] {
    [TOPIC_LANE_ADDED, TOPIC_LANE_REMOVED]
}

/// Returns every quote-critical Core topic accepted by the reducer.
pub fn quote_critical_topics() -> [B256; 15] {
    [
        TOPIC_LANE_ADDED,
        TOPIC_LANE_REMOVED,
        TOPIC_LANE_UPDATED,
        TOPIC_LANE_PAUSED_SET,
        TOPIC_PRICE_PUSH_THRESHOLD_SET,
        TOPIC_SLIPPAGE_K_SET,
        TOPIC_BLOCK_DELAY_SET,
        TOPIC_PARTNER_INFO_SET,
        TOPIC_PARTNER_FEE_SET,
        TOPIC_WHITELIST_SET,
        TOPIC_BLACKLIST_MULTIPLIER_SET,
        TOPIC_DEPOSIT_EXECUTED,
        TOPIC_WITHDRAWAL_EXECUTED,
        TOPIC_SYNC,
        TOPIC_UPGRADED,
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
            new_k: event.newK,
        })
    } else if topic0 == TOPIC_LANE_PAUSED_SET {
        expect_shape(log, 2, 2)?;
        let event = decode::<core::LanePausedSet>(log, LogDecodeError::InvalidBoolean)?;
        Some(QuoteEvent::LanePausedSet {
            asset: event.asset,
            paused: event.newPaused,
        })
    } else if topic0 == TOPIC_PRICE_PUSH_THRESHOLD_SET {
        expect_shape(log, 2, 4)?;
        let event = decode::<core::PricePushThresholdSet>(log, LogDecodeError::InvalidBoolean)?;
        Some(QuoteEvent::PricePushThresholdSet {
            asset: event.asset,
            price_push_threshold: event.newThreshold,
            enabled: event.newEnabled,
        })
    } else if topic0 == TOPIC_BLOCK_DELAY_SET {
        expect_shape(log, 2, 2)?;
        let event = decode::<core::BlockDelaySet>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::BlockDelaySet {
            asset: event.asset,
            block_delay: event.newBlockDelay,
        })
    } else if topic0 == TOPIC_PARTNER_INFO_SET {
        expect_shape(log, 4, 1)?;
        let event = decode::<core::PartnerInfoSet>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::PartnerInfoSet {
            router: event.router,
            asset: event.asset,
            fee: event.fee,
        })
    } else if topic0 == TOPIC_PARTNER_FEE_SET {
        expect_shape(log, 3, 1)?;
        let event = decode::<core::PartnerFeeSet>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::PartnerFeeSet {
            router: event.router,
            asset: event.asset,
            fee: event.fee,
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
            principal: event.principalAmount,
        })
    } else if topic0 == TOPIC_WITHDRAWAL_EXECUTED {
        expect_shape(log, 4, 4)?;
        let event = decode::<core::WithdrawalExecuted>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::WithdrawalExecuted {
            asset: event.asset,
            principal: event.principalAmount,
        })
    } else if topic0 == TOPIC_SYNC {
        expect_shape(log, 2, 2)?;
        let event = decode::<core::Sync>(log, LogDecodeError::InvalidDataLength)?;
        Some(QuoteEvent::Sync {
            asset: event.lane,
            asset_reserve: event.assetReserve,
            cash_reserve: event.cashReserve,
        })
    } else if topic0 == TOPIC_UPGRADED {
        expect_shape(log, 2, 0)?;
        let event = decode::<core::Upgraded>(log, LogDecodeError::InvalidAddress)?;
        Some(QuoteEvent::ImplementationUpgraded {
            implementation: event.implementation,
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
        TOPIC_DEPOSIT_EXECUTED, TOPIC_LANE_ADDED, TOPIC_LANE_PAUSED_SET,
        TOPIC_PRICE_PUSH_THRESHOLD_SET, TOPIC_WITHDRAWAL_EXECUTED, decode_core_event,
    };
    use lunarbase_math::types::{Address, B256, U256};

    #[test]
    fn generated_topics_match_the_pinned_solidity_abi() {
        assert_eq!(
            format!("{TOPIC_LANE_ADDED:#x}"),
            "0x1c61848d54083be4bfb8a26449add9f919cf1efd4ca608005f7f3f6aa0cef958"
        );
        assert_eq!(
            format!("{TOPIC_LANE_PAUSED_SET:#x}"),
            "0x457fade720abbce2ed945bda9c751bcadaddbd87a70e8d0c79b156e9aa4d3399"
        );
        assert_eq!(
            format!("{TOPIC_PRICE_PUSH_THRESHOLD_SET:#x}"),
            "0x6b38206650880c4736891c797636196db2056062d3a8011e4074feecbe8ae337"
        );
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
    fn lane_control_events_decode_the_new_contract_schema() {
        let asset = Address::new([0x11; 20]);
        let added = lane_log(TOPIC_LANE_ADDED, asset, &[]);
        assert_eq!(
            decode_core_event(&added).unwrap(),
            Some(QuoteEvent::LaneAdded { asset })
        );

        let paused = lane_log(TOPIC_LANE_PAUSED_SET, asset, &[U256::ZERO, U256::ONE]);
        assert_eq!(
            decode_core_event(&paused).unwrap(),
            Some(QuoteEvent::LanePausedSet {
                asset,
                paused: true,
            })
        );

        let threshold = lane_log(
            TOPIC_PRICE_PUSH_THRESHOLD_SET,
            asset,
            &[U256::from(9), U256::from(17), U256::ONE, U256::ZERO],
        );
        assert_eq!(
            decode_core_event(&threshold).unwrap(),
            Some(QuoteEvent::PricePushThresholdSet {
                asset,
                price_push_threshold: 17,
                enabled: false,
            })
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
                principal: 7,
            })
        );
    }

    fn lane_log(topic0: B256, asset: Address, words: &[U256]) -> ContractLog {
        let mut data = Vec::with_capacity(words.len() * 32);
        for word in words {
            data.extend_from_slice(&word.to_be_bytes::<32>());
        }
        ContractLog {
            address: Address::new([0x22; 20]),
            topics: vec![topic0, B256::left_padding_from(asset.as_slice())],
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
        }
    }
}
