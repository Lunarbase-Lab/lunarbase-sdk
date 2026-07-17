fn write_config(
    path: &Path,
    mock: &MockChain,
    redis_url: &str,
    port: u16,
    owner: &str,
    lease_enabled: bool,
) -> Result<(), E2eError> {
    let redis_enabled = lease_enabled;
    let contents = format!(
        r#"network = "base"
core = "{CORE}"
deployment_block = 0
expected_runtime_code_hash = "{EMPTY_CODE_HASH}"
http_rpc_url = "{}"
realtime_url = "{}"
snapshot_tag = "finalized"
bind = "127.0.0.1:{port}"
explicit_lane_assets = ["{ASSET}"]
eager_routers = ["{ROUTER}"]

[runtime]
buffer_capacity = 128
reconnect_delay_milliseconds = 100

[redis]
enabled = {redis_enabled}
url = "{redis_url}"
io_timeout_milliseconds = 200
stream_max_len = 1000
dedup_ttl_seconds = 60
checkpoint_interval_updates = 1

[writer_lease]
enabled = {lease_enabled}
owner = "{owner}"
ttl_milliseconds = 2000
renew_interval_milliseconds = 500
retry_interval_milliseconds = 100

[transport]
max_frame_bytes = 262144
reorder_capacity = 128
require_evm_parent_context = true

[shutdown]
timeout_seconds = 4

[alerts]
enabled = true
webhook_url = "{}"
poll_interval_seconds = 1
not_ready_after_seconds = 1
repeat_interval_seconds = 1
request_timeout_seconds = 1
"#,
        mock.rpc_url(),
        mock.websocket_url(),
        mock.webhook_url(),
    );
    std::fs::write(path, contents)?;
    Ok(())
}

fn spawn_indexer(binary: &Path, config: &Path) -> Result<Child, E2eError> {
    let mut command = Command::new(binary);
    command
        .arg("--config")
        .arg(config)
        .env("RUST_LOG", "lunarbase_indexer=warn")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    Ok(command.spawn()?)
}

async fn terminate(child: &mut Child) -> Result<(), E2eError> {
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
    let exit = timeout(Duration::from_secs(8), child.wait())
        .await
        .map_err(|_| E2eError::Scenario(format!("process {pid} ignored SIGTERM")))??;
    if !exit.success() {
        return Err(E2eError::Scenario(format!(
            "process {pid} exited unsuccessfully: {exit}"
        )));
    }
    Ok(())
}

