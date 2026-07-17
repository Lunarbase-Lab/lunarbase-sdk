async fn rpc_block_number(http: &reqwest::Client, rpc_url: &str) -> Result<u64, MonadError> {
    let response: Value = http
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_blockNumber",
            "params": [],
        }))
        .send()
        .await?
        .json()
        .await?;
    let value = response
        .get("result")
        .and_then(Value::as_str)
        .ok_or_else(|| MonadError::Validation("eth_blockNumber result is missing".into()))?;
    u64::from_str_radix(value.trim_start_matches("0x"), 16)
        .map_err(|error| MonadError::Validation(error.to_string()))
}

async fn require_success(
    client: &reqwest::Client,
    url: &str,
    component: &str,
) -> Result<(), MonadError> {
    if is_success(client, url).await {
        Ok(())
    } else {
        Err(MonadError::Validation(format!(
            "{component} is not ready at {url}"
        )))
    }
}

async fn is_success(client: &reqwest::Client, url: &str) -> bool {
    client
        .get(url)
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
}

async fn metrics(client: &reqwest::Client, indexer_url: &str) -> String {
    let url = format!("{}/metrics", indexer_url.trim_end_matches('/'));
    match client.get(url).send().await {
        Ok(response) => response.text().await.unwrap_or_default(),
        Err(_) => String::new(),
    }
}

fn metric_delta(before: &str, after: &str, name: &str) -> f64 {
    metric_value(after, name) - metric_value(before, name)
}

fn metric_value(metrics: &str, name: &str) -> f64 {
    metrics
        .lines()
        .find_map(|line| {
            let (metric, value) = line.split_once(' ')?;
            (metric == name)
                .then(|| value.parse::<f64>().ok())
                .flatten()
        })
        .unwrap_or(0.0)
}

fn parse_u64(value: Option<&Value>) -> Option<u64> {
    value?.as_u64().or_else(|| value?.as_str()?.parse().ok())
}

fn parse_u256_hex(value: &str) -> Option<U256> {
    let value = value.trim_start_matches("0x");
    if value.is_empty() || value.len() > 64 {
        return None;
    }
    let padded = format!("{value:0>64}");
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&padded[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(U256::from_be_bytes(bytes))
}

const fn commitment_rank(commitment: &str) -> u8 {
    match commitment.as_bytes() {
        b"proposed" => 0,
        b"finalized" => 1,
        b"verified" => 2,
        _ => 0,
    }
}

fn latest() -> String {
    "latest".into()
}

async fn stop_requested(stop: &mut watch::Receiver<bool>) {
    if *stop.borrow() {
        return;
    }
    loop {
        if stop.changed().await.is_err() || *stop.borrow() {
            return;
        }
    }
}
