async fn wait_for_role(url: &str, role: &str, ready: bool) -> Result<(), E2eError> {
    let client = reqwest::Client::new();
    let endpoint = format!("{url}/health/ready");
    let started = Instant::now();
    let mut last_observation = "no HTTP response".to_owned();
    while started.elapsed() < Duration::from_secs(12) {
        match client.get(&endpoint).send().await {
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                last_observation = format!("status={status}, body={body}");
                let actual_role = serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|body| body.get("role").and_then(Value::as_str).map(str::to_owned));
                if status.is_success() == ready && actual_role.as_deref() == Some(role) {
                    return Ok(());
                }
            }
            Err(error) => last_observation = error.to_string(),
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(E2eError::Scenario(format!(
        "did not observe role `{role}` at {url}; last observation: {last_observation}"
    )))
}

async fn wait_for_not_ready(url: &str) -> Result<(), E2eError> {
    let client = reqwest::Client::new();
    wait_until(Duration::from_secs(3), || {
        let client = client.clone();
        let endpoint = format!("{url}/health/ready");
        async move {
            client
                .get(endpoint)
                .send()
                .await
                .is_ok_and(|response| response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE)
        }
    })
    .await
    .map_err(|_| E2eError::Scenario("gap never made readiness return 503".into()))
}

async fn wait_for_block(url: &str, expected: u64) -> Result<(), E2eError> {
    let client = reqwest::Client::new();
    wait_until(Duration::from_secs(5), || {
        let client = client.clone();
        let endpoint = format!("{url}/health/ready");
        async move {
            let Ok(response) = client.get(endpoint).send().await else {
                return false;
            };
            response
                .json::<Value>()
                .await
                .ok()
                .and_then(|body| {
                    body.pointer("/cursor/blockNumber")?
                        .as_str()?
                        .parse::<u64>()
                        .ok()
                })
                .is_some_and(|block| block >= expected)
        }
    })
    .await
    .map_err(|_| E2eError::Scenario(format!("indexer did not reach block {expected}")))
}

async fn assert_quote(url: &str) -> Result<(), E2eError> {
    let response = reqwest::Client::new()
        .post(format!("{url}/v1/quote"))
        .json(&json!({
            "router": ROUTER,
            "assetIn": CASH,
            "assetOut": ASSET,
            "amount": "100",
            "mode": "exactIn",
            "executionBlockNumber": "104",
            "minimumCommitment": "realtime",
            "maxAgeBlocks": 10,
        }))
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(E2eError::Scenario(format!(
            "quote failed with status {}: {}",
            response.status(),
            response.text().await.unwrap_or_default()
        )));
    }
    let body: Value = response.json().await?;
    if body.pointer("/outcome/amountOut").is_none()
        || body.pointer("/outcome/status").and_then(Value::as_str) != Some("available")
    {
        return Err(E2eError::Scenario(format!(
            "quote did not contain an available amount: {body}"
        )));
    }
    Ok(())
}

async fn assert_checkpoint(redis_url: &str) -> Result<(), E2eError> {
    let url = redis_url.to_owned();
    let key = RedisNamespace::new(
        8453,
        Address::from_hex(CORE).map_err(|error| E2eError::Scenario(error.to_string()))?,
    )
    .checkpoint;
    let exists = tokio::task::spawn_blocking(move || -> Result<bool, redis::RedisError> {
        use redis::Commands;
        let client = redis::Client::open(url)?;
        let mut connection = client.get_connection()?;
        connection.exists(key)
    })
    .await
    .map_err(|error| E2eError::Scenario(error.to_string()))??;
    if !exists {
        return Err(E2eError::Scenario(
            "final Redis checkpoint key is absent".into(),
        ));
    }
    Ok(())
}

async fn wait_for_redis(url: &str) -> Result<(), E2eError> {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        let url = url.to_owned();
        let ready = tokio::task::spawn_blocking(move || -> bool {
            let Ok(client) = redis::Client::open(url) else {
                return false;
            };
            let Ok(mut connection) = client.get_connection() else {
                return false;
            };
            redis::cmd("PING")
                .query::<String>(&mut connection)
                .is_ok_and(|response| response == "PONG")
        })
        .await
        .unwrap_or(false);
        if ready {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(E2eError::Scenario(format!(
        "Redis at {url} did not become ready"
    )))
}

