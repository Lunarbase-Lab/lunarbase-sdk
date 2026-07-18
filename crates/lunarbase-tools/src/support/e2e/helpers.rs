use super::*;

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
    format!("0x{}{}", "0".repeat(24), address.trim_start_matches("0x"))
}

pub(super) fn words(values: &[U256]) -> String {
    let mut output = String::from("0x");
    for value in values {
        output.push_str(&format!("{value:064x}"));
    }
    output
}

pub(super) fn word_hex(value: B256) -> String {
    format!("{value:#x}")
}

pub(super) fn block_hash(block: u64) -> String {
    format!("0x{block:064x}")
}
