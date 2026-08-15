use crate::support::e2e::environment::E2eError;
use crate::support::e2e::{CORE, environment::MockLog};
use lunarbase_math::{Address, B256};
use serde_json::{Value, json};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::time::sleep;

pub(super) async fn wait_until<F, Fut>(deadline: Duration, mut predicate: F) -> Result<(), ()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    while started.elapsed() < deadline {
        if predicate().await {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(())
}

pub(super) async fn stop_requested(stop: &mut watch::Receiver<bool>) {
    if *stop.borrow() {
        return;
    }
    loop {
        if stop.changed().await.is_err() || *stop.borrow() {
            return;
        }
    }
}

pub(super) fn free_port() -> Result<u16, E2eError> {
    let listener =
        std::net::TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))?;
    Ok(listener.local_addr()?.port())
}

pub(super) fn address_word(address: &str) -> String {
    let address = address.parse::<Address>().expect("valid E2E address");
    format!("{:#x}", B256::left_padding_from(address.as_slice()))
}

pub(super) fn word_hex(value: B256) -> String {
    format!("{value:#x}")
}

pub(super) fn block_hash(block: u64) -> String {
    format!("{:#x}", B256::left_padding_from(&block.to_be_bytes()))
}

pub(super) fn raw_event_log(log: MockLog) -> Value {
    let mut transaction = [0_u8; 32];
    transaction[..8].copy_from_slice(&log.block.to_be_bytes());
    transaction[8..12].copy_from_slice(&log.log_index.to_be_bytes());
    transaction[31] = log.payload;
    json!({
        "address": CORE,
        "topics": [format!("{:#x}", B256::new([0x99; 32]))],
        "data": format!("0x{:02x}", log.payload),
        "removed": false,
        "blockNumber": format!("0x{:x}", log.block),
        "blockHash": block_hash(log.block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{:x}", log.log_index),
        "transactionHash": format!("{:#x}", B256::new(transaction)),
    })
}

pub(super) fn hex_quantity(value: &Value) -> Option<u64> {
    let value = value.as_str()?;
    u64::from_str_radix(value.strip_prefix("0x")?, 16).ok()
}
