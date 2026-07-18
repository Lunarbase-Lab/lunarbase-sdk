//! Core ABI topic constants and strict event decoding.

use crate::{ContractLog, LogDecodeError, QuoteEvent};
use alloy_primitives::b256;
use lunarbase_math::{Address, B256, U256};

pub const TOPIC_LANE_ADDED: B256 =
    b256!("1c61848d54083be4bfb8a26449add9f919cf1efd4ca608005f7f3f6aa0cef958");
pub const TOPIC_LANE_REMOVED: B256 =
    b256!("daa054a7d9aa74d7b3ee43f36a9a292169f22fbf60106608accc3161633fba98");
const TOPIC_LANE_UPDATED: B256 =
    b256!("4c5259bbfc22dbcf1f2d79e1e95c193e979499cc1b29f4b9f38d972cb383bd7a");
const TOPIC_SLIPPAGE_K_SET: B256 =
    b256!("284eddda3b70079855640ccd104ec7b972a8a3f8a46b157278ad1a26812cbdf8");
const TOPIC_PARTNER_INFO_SET: B256 =
    b256!("5155dfcae951816ec9ea329d96736edf3278b9d1cbe75191c70f004efe67a377");
const TOPIC_PARTNER_FEE_SET: B256 =
    b256!("785135eb22f3bdb08e949e200b6e47291b10a11aa8d27879046da64dff565b85");
const TOPIC_WHITELIST_SET: B256 =
    b256!("0aa5ec5ffdc7f6f9c4d0dded489d7450297155cb2f71cb771e02427f7dff4f51");
const TOPIC_BLACKLIST_MULTIPLIER_SET: B256 =
    b256!("a15057886e6ebcdf47294bcb091d686031124d1041cafe00740e93667bacd186");
const TOPIC_DEPOSIT_EXECUTED: B256 =
    b256!("9fb4891ffe3e11f428f3f10fa362b7938a364beebc215a2ec1db56a8d05ba20f");
const TOPIC_WITHDRAWAL_EXECUTED: B256 =
    b256!("722ca578dc087cbf283dc08891a94ba45b7119723568feda14da8f4c9c35d251");

/// Returns the `LaneAdded` and `LaneRemoved` topic0 values used for discovery.
pub fn lane_discovery_topics() -> [B256; 2] {
    [TOPIC_LANE_ADDED, TOPIC_LANE_REMOVED]
}

/// Returns every quote-critical Core topic accepted by the reducer.
///
/// `SwapExecuted` is deliberately excluded because quotes derive their state
/// from lane, fee-profile, deposit, and withdrawal transitions.
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
fn topic_address(topic: B256) -> Result<Address, LogDecodeError> {
    let bytes = topic.as_slice();
    if bytes[..12].iter().any(|byte| *byte != 0) {
        return Err(LogDecodeError::InvalidAddress);
    }
    Ok(Address::from_slice(&bytes[12..]))
}

fn data_word(data: &[u8], index: usize) -> Result<U256, LogDecodeError> {
    let start = index
        .checked_mul(32)
        .ok_or(LogDecodeError::InvalidDataLength)?;
    let word = data
        .get(start..start + 32)
        .ok_or(LogDecodeError::InvalidDataLength)?;
    Ok(U256::from_be_bytes::<32>(
        word.try_into().expect("32-byte ABI word"),
    ))
}

fn expect_data_words(data: &[u8], count: usize) -> Result<(), LogDecodeError> {
    if data.len() != count * 32 {
        return Err(LogDecodeError::InvalidDataLength);
    }
    Ok(())
}

fn expect_topics(topics: &[B256], count: usize) -> Result<(), LogDecodeError> {
    if topics.len() != count {
        return Err(LogDecodeError::InvalidTopicCount);
    }
    Ok(())
}

fn decode_bool(word: U256) -> Result<bool, LogDecodeError> {
    match word {
        U256::ZERO => Ok(false),
        U256::ONE => Ok(true),
        _ => Err(LogDecodeError::InvalidBoolean),
    }
}

/// Decode the quote-critical events from the pinned Core ABI. Unknown events
/// return `Ok(None)` so callers can share one Core log subscription with other
/// modules. The returned event deliberately drops non-quote metadata such as
/// partner operator and position ids.
///
/// # Errors
///
/// Returns a typed error for missing topic zero, wrong indexed-topic count,
/// malformed ABI data, non-padded addresses, or invalid booleans.
pub fn decode_core_event(log: &ContractLog) -> Result<Option<QuoteEvent>, LogDecodeError> {
    let topic0 = *log.topics.first().ok_or(LogDecodeError::MissingTopic0)?;
    let topics = &log.topics;
    let data = &log.data;
    let event = if topic0 == TOPIC_LANE_ADDED {
        expect_topics(topics, 2)?;
        expect_data_words(data, 0)?;
        Some(QuoteEvent::LaneAdded {
            asset: topic_address(topics[1])?,
        })
    } else if topic0 == TOPIC_LANE_REMOVED {
        expect_topics(topics, 2)?;
        expect_data_words(data, 0)?;
        Some(QuoteEvent::LaneRemoved {
            asset: topic_address(topics[1])?,
        })
    } else if topic0 == TOPIC_LANE_UPDATED {
        expect_topics(topics, 2)?;
        expect_data_words(data, 1)?;
        Some(QuoteEvent::LaneUpdated {
            asset: topic_address(topics[1])?,
            slot0: data_word(data, 0)?,
        })
    } else if topic0 == TOPIC_SLIPPAGE_K_SET {
        expect_topics(topics, 2)?;
        expect_data_words(data, 2)?;
        Some(QuoteEvent::SlippageKSet {
            asset: topic_address(topics[1])?,
            new_k: data_word(data, 1)?,
        })
    } else if topic0 == TOPIC_PARTNER_INFO_SET {
        expect_topics(topics, 4)?;
        expect_data_words(data, 1)?;
        Some(QuoteEvent::PartnerInfoSet {
            router: topic_address(topics[1])?,
            asset: topic_address(topics[2])?,
            fee: data_word(data, 0)?,
        })
    } else if topic0 == TOPIC_PARTNER_FEE_SET {
        expect_topics(topics, 3)?;
        expect_data_words(data, 1)?;
        Some(QuoteEvent::PartnerFeeSet {
            router: topic_address(topics[1])?,
            asset: topic_address(topics[2])?,
            fee: data_word(data, 0)?,
        })
    } else if topic0 == TOPIC_WHITELIST_SET {
        expect_topics(topics, 2)?;
        expect_data_words(data, 1)?;
        Some(QuoteEvent::WhitelistSet {
            router: topic_address(topics[1])?,
            whitelisted: decode_bool(data_word(data, 0)?)?,
        })
    } else if topic0 == TOPIC_BLACKLIST_MULTIPLIER_SET {
        expect_topics(topics, 1)?;
        expect_data_words(data, 1)?;
        Some(QuoteEvent::BlacklistFeeMultiplierSet {
            multiplier: data_word(data, 0)?,
        })
    } else if topic0 == TOPIC_DEPOSIT_EXECUTED {
        expect_topics(topics, 4)?;
        expect_data_words(data, 1)?;
        Some(QuoteEvent::DepositExecuted {
            asset: topic_address(topics[3])?,
            principal: data_word(data, 0)?,
        })
    } else if topic0 == TOPIC_WITHDRAWAL_EXECUTED {
        expect_topics(topics, 4)?;
        expect_data_words(data, 4)?;
        Some(QuoteEvent::WithdrawalExecuted {
            asset: topic_address(topics[3])?,
            principal: data_word(data, 0)?,
        })
    } else {
        None
    };
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChainCursor, Commitment};

    #[test]
    fn position_topics_match_the_pinned_solidity_abi() {
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
