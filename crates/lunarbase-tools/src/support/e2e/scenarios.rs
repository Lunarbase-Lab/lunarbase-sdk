use crate::support::e2e::assertions::{
    assert_checkpoint, assert_consumer_reclaim, fetch_quote, wait_for_block, wait_for_metric,
    wait_for_not_ready, wait_for_ready, wait_for_stream_length,
};
use crate::support::e2e::environment::{
    E2eArguments, E2eError, MockChain, MockEvent, MockLog, RedisProcess, Workspace,
};
use crate::support::e2e::helpers::{free_port, wait_until};
use crate::support::e2e::process::{
    kill_force, spawn_event_worker, spawn_indexer, terminate, write_config,
};
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
    if !arguments.event_worker_bin.is_file() {
        return Err(E2eError::Scenario(format!(
            "event worker binary `{}` does not exist; run `cargo build -p lunarbase-event-worker`",
            arguments.event_worker_bin.display()
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
    let mut redis = RedisProcess::start(arguments.redis_url.clone()).await?;
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

    let redis_crash_tested = run_event_worker_scenarios(&arguments, &mock, &mut redis).await?;

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
            "eventWorkerCrashReplay": true,
            "duplicateDeliverySuppressed": true,
            "consumerPendingReclaimed": true,
            "redisAofCrashReplay": redis_crash_tested,
        })
    );
    mock.stop().await;
    redis.stop().await;
    Ok(())
}

async fn run_event_worker_scenarios(
    arguments: &E2eArguments,
    mock: &MockChain,
    redis: &mut RedisProcess,
) -> Result<bool, E2eError> {
    let port = free_port()?;
    let url = format!("http://127.0.0.1:{port}");
    let mut worker = spawn_event_worker(&arguments.event_worker_bin, mock, &redis.url, port)?;
    wait_for_ready(&url).await?;

    mock.publish_log(MockLog {
        block: 105,
        log_index: 0,
        payload: 0xa5,
    });
    wait_for_stream_length(&redis.url, 1).await?;
    kill_force(&mut worker).await?;

    mock.record_log(MockLog {
        block: 106,
        log_index: 0,
        payload: 0xa6,
    });
    let mut restarted = spawn_event_worker(&arguments.event_worker_bin, mock, &redis.url, port)?;
    wait_for_ready(&url).await?;
    wait_for_stream_length(&redis.url, 2).await?;
    mock.publish(MockEvent::Log(MockLog {
        block: 106,
        log_index: 0,
        payload: 0xa6,
    }));
    mock.publish_log(MockLog {
        block: 107,
        log_index: 0,
        payload: 0xa7,
    });
    wait_for_metric(&url, "lunarbase_event_worker_events_total", 2).await?;
    wait_for_stream_length(&redis.url, 3).await?;
    assert_consumer_reclaim(&redis.url).await?;

    let redis_crash_tested = redis.is_managed();
    if redis_crash_tested {
        redis.crash().await?;
        mock.publish_log(MockLog {
            block: 108,
            log_index: 0,
            payload: 0xa8,
        });
        wait_for_not_ready(&url).await?;
        redis.restart().await?;
        wait_for_stream_length(&redis.url, 4).await?;
        wait_for_ready(&url).await?;
    }

    terminate(&mut restarted)
        .await
        .map_err(|error| E2eError::Scenario(format!("event worker shutdown failed: {error}")))?;
    wait_until(Duration::from_secs(3), || async {
        mock.state.websocket_connections.load(Ordering::Relaxed) == 0
    })
    .await
    .map_err(|_| E2eError::Scenario("event worker left a detached WebSocket task".into()))?;
    Ok(redis_crash_tested)
}
