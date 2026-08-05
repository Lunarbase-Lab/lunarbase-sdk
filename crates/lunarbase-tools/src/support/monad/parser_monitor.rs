use crate::support::monad::helpers::{commitment_rank, parse_u64, stop_requested};
use crate::support::monad::types::{MonadError, ParserReport};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub(super) async fn monitor_parser(
    url: &str,
    mut stop: watch::Receiver<bool>,
) -> Result<ParserReport, MonadError> {
    let (socket, _) = connect_async(url).await?;
    let (mut writer, mut reader) = socket.split();
    writer
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "subscribe",
                "params": ["all"],
            })
            .to_string(),
        ))
        .await?;
    let mut report = ParserReport::default();
    let mut block_commitments = BTreeMap::<u64, u8>::new();
    loop {
        let message = tokio::select! {
            biased;
            () = stop_requested(&mut stop) => break,
            message = reader.next() => message,
        };
        let Some(message) = message else {
            break;
        };
        let message = message?;
        let payload = match message {
            Message::Text(text) => text.to_string(),
            Message::Binary(bytes) => String::from_utf8(bytes.to_vec())
                .map_err(|error| MonadError::Validation(error.to_string()))?,
            Message::Ping(bytes) => {
                writer.send(Message::Pong(bytes)).await?;
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => break,
            _ => continue,
        };
        let value: Value = serde_json::from_str(&payload)?;
        report.messages = report.messages.saturating_add(1);
        if value.get("method").and_then(Value::as_str) == Some("subscriptionGap") {
            report.explicit_gaps = report.explicit_gaps.saturating_add(1);
            continue;
        }
        let Some(result) = value.get("result").and_then(Value::as_object) else {
            continue;
        };
        match result.get("type").and_then(Value::as_str) {
            Some("newHead") => {
                report.heads = report.heads.saturating_add(1);
                let sequence = parse_u64(result.get("seqno"));
                if let Some(sequence) = sequence {
                    if report
                        .last_sequence
                        .is_some_and(|previous| sequence < previous)
                    {
                        report.sequence_regressions = report.sequence_regressions.saturating_add(1);
                    }
                    report.last_sequence = Some(
                        report
                            .last_sequence
                            .map_or(sequence, |previous| previous.max(sequence)),
                    );
                }
                if let Some(block) = parse_u64(result.get("blockNumber")) {
                    report.last_block =
                        Some(report.last_block.map_or(block, |last| last.max(block)));
                    let commitment = commitment_rank(
                        result
                            .get("commitment")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    );
                    if block_commitments
                        .get(&block)
                        .is_some_and(|previous| commitment < *previous)
                    {
                        report.commitment_regressions =
                            report.commitment_regressions.saturating_add(1);
                    }
                    block_commitments
                        .entry(block)
                        .and_modify(|previous| *previous = (*previous).max(commitment))
                        .or_insert(commitment);
                    while block_commitments.len() > 512 {
                        block_commitments.pop_first();
                    }
                }
            }
            Some("health") => {
                report.health_messages = report.health_messages.saturating_add(1);
                if result.get("stalled").and_then(Value::as_bool) == Some(true) {
                    report.explicit_gaps = report.explicit_gaps.saturating_add(1);
                }
            }
            Some("alert") => {
                report.alerts = report.alerts.saturating_add(1);
                let message = result
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                if ["gap", "expired", "stalled", "ring"]
                    .iter()
                    .any(|needle| message.contains(needle))
                {
                    report.explicit_gaps = report.explicit_gaps.saturating_add(1);
                }
            }
            _ => {}
        }
    }
    Ok(report)
}
