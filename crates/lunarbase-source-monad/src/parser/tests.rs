use crate::parser::{
    ParserHandshakeState, observe_handshake_payload, subscription_request,
    validate_notification_subscription,
};
use crate::protocol::{ParserMessage, decode_parser_message};
use lunarbase_client::model::ContractFilter;
use lunarbase_math::{Address, B256, U256};
use serde_json::{Value, json};

#[test]
fn subscription_request_matches_parser_shape() {
    let address = "0x0000000000000000000000000000000000000001"
        .parse::<Address>()
        .unwrap();
    let message = subscription_request(
        1,
        "logs",
        Some((
            &address,
            &ContractFilter {
                address,
                topics: vec![B256::new(U256::ONE.to_be_bytes::<32>())],
            },
        )),
    );
    let value: Value = serde_json::from_str(&message).unwrap();
    assert_eq!(value["params"][0], "logs");
    assert_eq!(
        value["params"][1]["address"],
        "0x0000000000000000000000000000000000000001"
    );
    assert_eq!(value["params"][1]["topics"][0][0], format!("0x{:064x}", 1));
}

#[test]
fn fixture_notifications_are_bound_to_acknowledged_subscriptions() {
    let fixture =
        include_str!("../../../../fixtures/event-replay/monad-exec-events/parser-messages.jsonl");
    let frames = fixture.lines().collect::<Vec<_>>();
    validate_notification_subscription(frames[2].as_bytes(), "sub_2", "head").unwrap();
    validate_notification_subscription(frames[3].as_bytes(), "sub_1", "log").unwrap();
    assert!(
        validate_notification_subscription(frames[3].as_bytes(), "sub_2", "log")
            .unwrap_err()
            .to_string()
            .contains("unexpected subscription")
    );
}

#[test]
fn handshake_accepts_stable_duplicate_acknowledgements_and_rejects_conflicts() {
    let mut stable = ParserHandshakeState::default();
    observe_handshake_payload(
        &mut stable,
        br#"{"jsonrpc":"2.0","id":1,"result":"sub_logs"}"#.to_vec(),
        4,
    )
    .unwrap();
    observe_handshake_payload(
        &mut stable,
        br#"{"jsonrpc":"2.0","id":1,"result":"sub_logs"}"#.to_vec(),
        4,
    )
    .unwrap();
    observe_handshake_payload(
        &mut stable,
        br#"{"jsonrpc":"2.0","id":2,"result":"sub_all"}"#.to_vec(),
        4,
    )
    .unwrap();
    assert!(stable.is_complete());
    let stable = stable.finish().unwrap();
    assert_eq!(stable.logs_subscription, "sub_logs");
    assert_eq!(stable.all_subscription, "sub_all");

    let mut conflict = ParserHandshakeState::default();
    observe_handshake_payload(
        &mut conflict,
        br#"{"jsonrpc":"2.0","id":1,"result":"sub_logs"}"#.to_vec(),
        4,
    )
    .unwrap();
    let error = observe_handshake_payload(
        &mut conflict,
        br#"{"jsonrpc":"2.0","id":1,"result":"other_logs"}"#.to_vec(),
        4,
    )
    .unwrap_err();
    assert!(error.to_string().contains("changed subscription id"));
}

#[test]
fn handshake_requires_exact_numeric_acknowledgement_ids() {
    for id in [json!("1"), json!(true), json!(1.0), json!(3)] {
        let mut state = ParserHandshakeState::default();
        let payload = json!({"jsonrpc": "2.0", "id": id, "result": "sub"}).to_string();
        let error = observe_handshake_payload(&mut state, payload.into_bytes(), 4).unwrap_err();
        assert!(error.to_string().contains("unexpected numeric id"));
    }
}

#[test]
fn handshake_bounds_prefetched_notifications() {
    let mut state = ParserHandshakeState::default();
    let notification = br#"{"jsonrpc":"2.0","method":"subscription","result":{"type":"health"}}"#;
    observe_handshake_payload(&mut state, notification.to_vec(), 2).unwrap();
    observe_handshake_payload(&mut state, notification.to_vec(), 2).unwrap();
    let error = observe_handshake_payload(&mut state, notification.to_vec(), 2).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("prefetch exceeded configured bound")
    );
}

#[test]
fn parser_gap_control_message_is_not_downgraded_to_health() {
    let message = br#"{"jsonrpc":"2.0","method":"subscriptionGap","params":{"skipped":42,"resubscribeRequired":true}}"#;
    assert!(
        matches!(decode_parser_message(message).unwrap(), ParserMessage::Gap(reason) if reason.contains("42"))
    );
}
