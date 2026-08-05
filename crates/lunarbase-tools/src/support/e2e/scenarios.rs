use crate::support::e2e::assertions::{
    assert_checkpoint, fetch_quote, wait_for_block, wait_for_not_ready, wait_for_ready,
};
use crate::support::e2e::environment::{
    E2eArguments, E2eError, MockChain, MockEvent, RedisProcess, Workspace,
};
use crate::support::e2e::helpers::{free_port, wait_until};
use crate::support::e2e::process::{spawn_indexer, terminate, write_config};
use serde_json::json;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Runs bootstrap, multi-replica, recovery, Redis fallback, checkpoint, and
/// SIGTERM scenarios against actual indexer OS processes.
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
    let replica_a_port = free_port()?;
    let replica_b_port = free_port()?;
    let unavailable_port = free_port()?;
    let interrupted_port = free_port()?;
    let replica_a_config = workspace.config("replica-a");
    let replica_b_config = workspace.config("replica-b");
    let unavailable_config = workspace.config("redis-unavailable");
    let interrupted_config = workspace.config("interrupted-bootstrap");
    write_config(&replica_a_config, &mock, Some(&redis.url), replica_a_port)?;
    write_config(&replica_b_config, &mock, Some(&redis.url), replica_b_port)?;
    write_config(
        &unavailable_config,
        &mock,
        Some("redis://127.0.0.1:1/"),
        unavailable_port,
    )?;
    write_config(&interrupted_config, &mock, None, interrupted_port)?;

    let mut replica_a = spawn_indexer(&arguments.indexer_bin, &replica_a_config)?;
    let mut replica_b = spawn_indexer(&arguments.indexer_bin, &replica_b_config)?;
    let replica_a_url = format!("http://127.0.0.1:{replica_a_port}");
    let replica_b_url = format!("http://127.0.0.1:{replica_b_port}");
    wait_for_ready(&replica_a_url).await?;
    wait_for_ready(&replica_b_url).await?;
    let quote_a = fetch_quote(&replica_a_url).await?;
    let quote_b = fetch_quote(&replica_b_url).await?;
    if quote_a != quote_b {
        return Err(E2eError::Scenario(
            "active replicas returned different quotes".into(),
        ));
    }
    mock.publish(MockEvent::Header(101));
    wait_for_block(&replica_a_url, 101).await?;
    wait_for_block(&replica_b_url, 101).await?;

    mock.state
        .recovery_delay_milliseconds
        .store(750, Ordering::Relaxed);
    mock.publish(MockEvent::Gap(103));
    wait_for_not_ready(&replica_a_url).await?;
    wait_for_not_ready(&replica_b_url).await?;
    mock.state
        .recovery_delay_milliseconds
        .store(0, Ordering::Relaxed);
    wait_for_ready(&replica_a_url).await?;
    wait_for_ready(&replica_b_url).await?;
    mock.publish(MockEvent::Header(104));
    wait_for_block(&replica_a_url, 104).await?;
    wait_for_block(&replica_b_url, 104).await?;

    terminate(&mut replica_a)
        .await
        .map_err(|error| E2eError::Scenario(format!("replica A shutdown failed: {error}")))?;
    wait_for_ready(&replica_b_url).await?;
    fetch_quote(&replica_b_url).await?;
    terminate(&mut replica_b)
        .await
        .map_err(|error| E2eError::Scenario(format!("replica B shutdown failed: {error}")))?;
    assert_checkpoint(&redis.url).await?;

    let mut unavailable = spawn_indexer(&arguments.indexer_bin, &unavailable_config)?;
    let unavailable_url = format!("http://127.0.0.1:{unavailable_port}");
    wait_for_ready(&unavailable_url).await?;
    fetch_quote(&unavailable_url).await?;
    terminate(&mut unavailable).await.map_err(|error| {
        E2eError::Scenario(format!("Redis-unavailable shutdown failed: {error}"))
    })?;

    mock.state
        .recovery_delay_milliseconds
        .store(5_000, Ordering::Relaxed);
    let mut interrupted = spawn_indexer(&arguments.indexer_bin, &interrupted_config)?;
    sleep(Duration::from_millis(150)).await;
    terminate(&mut interrupted)
        .await
        .map_err(|error| E2eError::Scenario(format!("bootstrap shutdown failed: {error}")))?;
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
            "activeReplicas": 2,
            "redisUnavailableReady": true,
            "sigtermDuringSnapshot": true,
            "finalCheckpoint": true,
            "detachedWebsocketTasks": 0,
        })
    );
    mock.stop().await;
    redis.stop().await;
    Ok(())
}
