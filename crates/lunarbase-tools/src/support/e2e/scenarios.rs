/// Runs bootstrap, realtime, recovery, lease takeover, webhook, checkpoint,
/// and SIGTERM scenarios against actual indexer OS processes.
pub async fn run(arguments: E2eArguments) -> Result<(), E2eError> {
    if !arguments.indexer_bin.is_file() {
        return Err(E2eError::Scenario(format!(
            "indexer binary `{}` does not exist; run `cargo build -p lunarbase-indexer`",
            arguments.indexer_bin.display()
        )));
    }
    timeout(
        Duration::from_secs(arguments.timeout_seconds),
        run_scenarios(arguments),
    )
    .await
    .map_err(|_| E2eError::Scenario("complete E2E deadline exceeded".into()))?
}

async fn run_scenarios(arguments: E2eArguments) -> Result<(), E2eError> {
    let mock = MockChain::start().await?;
    let redis = RedisProcess::start(arguments.redis_url).await?;
    let workspace = Workspace::create()?;
    let primary_port = free_port()?;
    let standby_port = free_port()?;
    let startup_port = free_port()?;
    let primary_config = workspace.config("primary");
    let standby_config = workspace.config("standby");
    let startup_config = workspace.config("startup");
    write_config(
        &primary_config,
        &mock,
        &redis.url,
        primary_port,
        "e2e-primary",
        true,
    )?;
    write_config(
        &standby_config,
        &mock,
        &redis.url,
        standby_port,
        "e2e-standby",
        true,
    )?;
    write_config(
        &startup_config,
        &mock,
        &redis.url,
        startup_port,
        "e2e-startup",
        false,
    )?;

    let mut primary = spawn_indexer(&arguments.indexer_bin, &primary_config)?;
    let primary_url = format!("http://127.0.0.1:{primary_port}");
    wait_for_role(&primary_url, "active", true).await?;
    assert_quote(&primary_url).await?;
    mock.publish(MockEvent::Header(101));
    wait_for_block(&primary_url, 101).await?;

    let mut standby = spawn_indexer(&arguments.indexer_bin, &standby_config)?;
    let standby_url = format!("http://127.0.0.1:{standby_port}");
    wait_for_role(&standby_url, "standby", false).await?;

    mock.state
        .recovery_delay_milliseconds
        .store(750, Ordering::Relaxed);
    mock.publish(MockEvent::Gap(103));
    wait_for_not_ready(&primary_url).await?;
    mock.state
        .recovery_delay_milliseconds
        .store(0, Ordering::Relaxed);
    mock.publish(MockEvent::Header(104));
    wait_for_role(&primary_url, "active", true).await?;
    assert_quote(&primary_url).await?;
    wait_until(Duration::from_secs(5), || async {
        mock.state.webhook_deliveries.load(Ordering::Relaxed) > 0
    })
    .await
    .map_err(|_| E2eError::Scenario("gap did not produce a webhook alert".into()))?;

    terminate(&mut primary)
        .await
        .map_err(|error| E2eError::Scenario(format!("primary shutdown failed: {error}")))?;
    wait_for_role(&standby_url, "active", true).await?;
    assert_quote(&standby_url).await?;
    terminate(&mut standby).await.map_err(|error| {
        E2eError::Scenario(format!("standby takeover shutdown failed: {error}"))
    })?;
    assert_checkpoint(&redis.url).await?;

    mock.state
        .recovery_delay_milliseconds
        .store(5_000, Ordering::Relaxed);
    let mut startup = spawn_indexer(&arguments.indexer_bin, &startup_config)?;
    sleep(Duration::from_millis(150)).await;
    terminate(&mut startup)
        .await
        .map_err(|error| E2eError::Scenario(format!("startup shutdown failed: {error}")))?;
    mock.state
        .recovery_delay_milliseconds
        .store(0, Ordering::Relaxed);

    wait_until(Duration::from_secs(3), || async {
        mock.state.websocket_connections.load(Ordering::Relaxed) == 0
    })
    .await
    .map_err(|_| E2eError::Scenario("indexer left a detached WebSocket task".into()))?;

    println!(
        "{}",
        json!({
            "status": "ok",
            "bootstrapQuote": true,
            "realtimeBlock": 101,
            "gapReturned503": true,
            "recoveredBlock": 104,
            "standbyTakeover": true,
            "sigtermDuringSnapshot": true,
            "finalCheckpoint": true,
            "webhookDeliveries": mock.state.webhook_deliveries.load(Ordering::Relaxed),
            "detachedWebsocketTasks": 0,
        })
    );
    mock.stop().await;
    redis.stop().await;
    Ok(())
}

