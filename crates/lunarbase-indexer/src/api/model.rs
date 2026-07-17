//! JSON request parsing and stable response encoding.

use lunarbase_client_core::{ChainCursor, ClientQuote, Commitment, IndexerHealth};
use lunarbase_math::{Address, QuoteMode, QuoteOutcome, QuoteRequest, UnavailableReason, U256};
use serde::Deserialize;
use serde_json::{json, Value};
use std::str::FromStr;
use std::time::UNIX_EPOCH;

/// Wire request for the quote endpoint.
///
/// Amounts and the execution block are strings so TypeScript and JSON callers
/// cannot accidentally round values through an IEEE-754 number.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuoteApiRequest {
    pub router: String,
    pub asset_in: String,
    pub asset_out: String,
    pub amount: String,
    pub mode: ApiQuoteMode,
    pub execution_block_number: String,
    #[serde(default)]
    pub minimum_commitment: Option<ApiCommitment>,
    #[serde(default)]
    pub max_age_blocks: Option<u64>,
}

/// Exact-input or exact-output quote mode accepted by the HTTP API.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ApiQuoteMode {
    ExactIn,
    ExactOut,
}

/// Minimum chain confidence required before returning a quote.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApiCommitment {
    Realtime,
    Canonical,
    Finalized,
}

impl QuoteApiRequest {
    /// Converts string wire fields into the exact Rust math/runtime domain.
    ///
    /// Decimal and `0x`-prefixed hexadecimal `uint256` values are supported;
    /// over-wide, malformed, or invalid addresses are rejected before the
    /// reducer is queried.
    pub fn parse(self) -> Result<(QuoteRequest, U256, Commitment, Option<u64>), String> {
        let request = QuoteRequest {
            router: parse_address(&self.router, "router")?,
            asset_in: parse_address(&self.asset_in, "assetIn")?,
            asset_out: parse_address(&self.asset_out, "assetOut")?,
            amount: parse_u256(&self.amount, "amount")?,
            mode: match self.mode {
                ApiQuoteMode::ExactIn => QuoteMode::ExactIn,
                ApiQuoteMode::ExactOut => QuoteMode::ExactOut,
            },
        };
        let execution_block_number =
            parse_u256(&self.execution_block_number, "executionBlockNumber")?;
        let commitment = match self.minimum_commitment.unwrap_or(ApiCommitment::Realtime) {
            ApiCommitment::Realtime => Commitment::Realtime,
            ApiCommitment::Canonical => Commitment::Canonical,
            ApiCommitment::Finalized => Commitment::Finalized,
        };
        Ok((
            request,
            execution_block_number,
            commitment,
            self.max_age_blocks,
        ))
    }
}

/// Encodes readiness and compatibility metadata as stable camel-case JSON.
pub fn health_json(health: IndexerHealth) -> Value {
    json!({
        "ready": health.ready,
        "commitment": commitment_name(health.commitment),
        "cursor": health.cursor.as_ref().map(cursor_json),
        "codeHash": hex32(health.code_hash),
        "mathCompatibilityVersion": health.math_compatibility_version,
    })
}

/// Encodes a quote together with its cursor, freshness, and compatibility
/// proof. Every potentially wide integer remains a decimal string.
pub fn quote_json(quote: ClientQuote) -> Value {
    let observed_at_milliseconds = quote
        .observed_at
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string();
    json!({
        "outcome": outcome_json(quote.outcome),
        "cursor": cursor_json(&quote.cursor),
        "commitment": commitment_name(quote.commitment),
        "observedAtMilliseconds": observed_at_milliseconds,
        "ageMilliseconds": quote.age.as_millis().to_string(),
        "stale": quote.stale,
        "contractCodeHash": hex32(quote.contract_code_hash),
        "mathCompatibilityVersion": quote.math_compatibility_version,
    })
}

fn outcome_json(outcome: QuoteOutcome) -> Value {
    match outcome {
        QuoteOutcome::Available(result) => json!({
            "status": "available",
            "amountIn": result.amount_in.to_string(),
            "amountOut": result.amount_out.to_string(),
            "feeAsset": result.fee_asset.to_hex(),
            "feeAmount": result.fee_amount.to_string(),
            "partnerFee": result.partner_fee.to_string(),
            "treasuryFee": result.treasury_fee.to_string(),
        }),
        QuoteOutcome::Unavailable(reason) => json!({
            "status": "unavailable",
            "reason": unavailable_json(reason),
        }),
    }
}

fn unavailable_json(reason: UnavailableReason) -> Value {
    match reason {
        UnavailableReason::ZeroAmount => json!({"code": "zeroAmount"}),
        UnavailableReason::EqualAssets => json!({"code": "equalAssets"}),
        UnavailableReason::MissingLane(asset) => {
            json!({"code": "missingLane", "asset": asset.to_hex()})
        }
        UnavailableReason::PausedLane(asset) => {
            json!({"code": "pausedLane", "asset": asset.to_hex()})
        }
        UnavailableReason::DelayedLane(asset) => {
            json!({"code": "delayedLane", "asset": asset.to_hex()})
        }
        UnavailableReason::ZeroPrice(asset) => {
            json!({"code": "zeroPrice", "asset": asset.to_hex()})
        }
        UnavailableReason::ZeroPrincipal(asset) => {
            json!({"code": "zeroPrincipal", "asset": asset.to_hex()})
        }
        UnavailableReason::ZeroAnchor => json!({"code": "zeroAnchor"}),
        UnavailableReason::SpreadConsumesAnchor => {
            json!({"code": "spreadConsumesAnchor"})
        }
    }
}

fn cursor_json(cursor: &ChainCursor) -> Value {
    json!({
        "chainId": cursor.chain_id.to_string(),
        "blockNumber": cursor.block_number.to_string(),
        "blockHash": cursor.block_hash.map(hex32),
        "transactionIndex": cursor.transaction_index,
        "logIndex": cursor.log_index,
        "sourceSequence": cursor.source_sequence.map(|value| value.to_string()),
        "sourceSubIndex": cursor.source_sub_index,
        "commitment": commitment_name(cursor.commitment),
    })
}

fn commitment_name(commitment: Commitment) -> &'static str {
    match commitment {
        Commitment::Realtime => "realtime",
        Commitment::Canonical => "canonical",
        Commitment::Finalized => "finalized",
    }
}

fn parse_address(value: &str, field: &str) -> Result<Address, String> {
    Address::from_hex(value).map_err(|error| format!("{field}: {error}"))
}

fn parse_u256(value: &str, field: &str) -> Result<U256, String> {
    if let Some(hex) = value.strip_prefix("0x") {
        if hex.is_empty() || hex.len() > 64 {
            return Err(format!("{field}: invalid uint256 hexadecimal value"));
        }
        let padded = format!("{hex:0>64}");
        let mut bytes = [0; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&padded[index * 2..index * 2 + 2], 16)
                .map_err(|_| format!("{field}: invalid uint256 hexadecimal value"))?;
        }
        Ok(U256::from_be_bytes(bytes))
    } else {
        U256::from_str(value).map_err(|_| format!("{field}: invalid decimal uint256 value"))
    }
}

fn hex32(value: [u8; 32]) -> String {
    let mut output = String::with_capacity(66);
    output.push_str("0x");
    for byte in value {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_request_accepts_decimal_and_hex_u256() {
        let request = QuoteApiRequest {
            router: "0x0000000000000000000000000000000000000001".into(),
            asset_in: "0x0000000000000000000000000000000000000002".into(),
            asset_out: "0x0000000000000000000000000000000000000003".into(),
            amount: "42".into(),
            mode: ApiQuoteMode::ExactIn,
            execution_block_number: "0x2a".into(),
            minimum_commitment: None,
            max_age_blocks: Some(2),
        };
        let (request, block, commitment, max_age) = request.parse().unwrap();
        assert_eq!(request.amount, U256::from(42));
        assert_eq!(block, U256::from(42));
        assert_eq!(commitment, Commitment::Realtime);
        assert_eq!(max_age, Some(2));
    }
}
