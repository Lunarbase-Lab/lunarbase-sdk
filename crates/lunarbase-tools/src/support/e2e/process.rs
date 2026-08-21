use crate::support::e2e::environment::{E2eError, MockChain};
use crate::support::e2e::{ASSET, CORE, EMPTY_CODE_HASH, IMPLEMENTATION};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::timeout;

const INDEXER_SHUTDOWN_TIMEOUT_SECONDS: u64 = 8;
const PROCESS_EXIT_TIMEOUT_SECONDS: u64 = 12;

pub(super) fn write_config(
    path: &Path,
    mock: &MockChain,
    redis_url: Option<&str>,
    port: u16,
) -> Result<(), E2eError> {
    let redis = redis_url
        .map(|url| format!("redis_url = \"{url}\"\n"))
        .unwrap_or_default();
    let contents = format!(
        r#"network = "base"
chain_id = 8453
core = "{CORE}"
fee_class = "whitelisted"
deployment_block = 0
expected_implementation = "{IMPLEMENTATION}"
expected_implementation_code_hash = "{EMPTY_CODE_HASH}"
http_rpc_url = "{}"
realtime_url = "{}"
delivery_mode = "realtime"
bind = "127.0.0.1:{port}"
explicit_lane_assets = ["{ASSET}"]
queue_bound = 128
reconnect_delay_milliseconds = 100
{redis}checkpoint_interval_seconds = 1
shutdown_timeout_seconds = {INDEXER_SHUTDOWN_TIMEOUT_SECONDS}
"#,
        mock.rpc_url(),
        mock.websocket_url(),
    );
    std::fs::write(path, contents)?;
    Ok(())
}

pub(super) fn spawn_indexer(binary: &Path, config: &Path) -> Result<Child, E2eError> {
    let mut command = Command::new(binary);
    command
        .arg("--config")
        .arg(config)
        .env("RUST_LOG", "lunarbase_indexer=info")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    Ok(command.spawn()?)
}

pub(super) fn spawn_event_worker(
    binary: &Path,
    mock: &MockChain,
    redis_url: &str,
    port: u16,
) -> Result<Child, E2eError> {
    let mut command = Command::new(binary);
    command
        .args([
            "--network",
            "base",
            "--chain-id",
            "8453",
            "--core",
            CORE,
            "--deployment-block",
            "0",
            "--http-rpc-url",
            &mock.rpc_url(),
            "--realtime-url",
            &mock.websocket_url(),
            "--redis-url",
            redis_url,
            "--redis-namespace",
            "lunarbase-e2e",
            "--consumer-group",
            "lunarbase-e2e-consumers",
            "--minimum-commitment",
            "realtime",
            "--bind",
            &format!("127.0.0.1:{port}"),
            "--source-queue-bound",
            "8",
            "--source-queue-byte-bound",
            "65536",
            "--correction-byte-bound",
            "65536",
            "--redis-queue-bound",
            "2",
            "--redis-queue-byte-bound",
            "65536",
            "--reconnect-delay-milliseconds",
            "50",
            "--source-stall-timeout-milliseconds",
            "5000",
            "--redis-timeout-milliseconds",
            "250",
            "--shutdown-timeout-seconds",
            "4",
        ])
        .env("RUST_LOG", "lunarbase_event_worker=info")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    Ok(command.spawn()?)
}

pub(super) async fn kill_force(child: &mut Child) -> Result<(), E2eError> {
    if child.id().is_none() {
        return Ok(());
    }
    child.start_kill()?;
    child.wait().await?;
    Ok(())
}

pub(super) async fn terminate(child: &mut Child) -> Result<(), E2eError> {
    let Some(pid) = child.id() else {
        return Ok(());
    };
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .await?;
    if !status.success() {
        return Err(E2eError::Scenario(format!(
            "failed to send SIGTERM to process {pid}"
        )));
    }
    let exit = timeout(
        Duration::from_secs(PROCESS_EXIT_TIMEOUT_SECONDS),
        child.wait(),
    )
    .await
    .map_err(|_| E2eError::Scenario(format!("process {pid} ignored SIGTERM")))??;
    if !exit.success() {
        return Err(E2eError::Scenario(format!(
            "process {pid} exited unsuccessfully: {exit}"
        )));
    }
    Ok(())
}
