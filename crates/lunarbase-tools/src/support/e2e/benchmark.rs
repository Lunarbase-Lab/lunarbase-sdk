//! Deterministic load matrix against fresh real indexer processes.

use super::environment::{E2eError, MockChain, Workspace};
use super::helpers::free_port;
use super::process::{spawn_indexer, terminate};
use super::{CORE, EMPTY_CODE_HASH, IMPLEMENTATION};
use crate::support::load::{LoadArguments, LoadReport, execute, lane_address};
use clap::Parser;
use serde::Serialize;
use std::path::{Path, PathBuf};

const REPORT_SCHEMA_VERSION: u32 = 2;

/// Reproducible real-process indexer benchmark settings.
#[derive(Clone, Debug, Parser)]
#[command(name = "lunarbase-indexer-bench")]
pub struct IndexerBenchmarkArguments {
    /// Path to a release-built `lunarbase-indexer` executable.
    #[arg(long, default_value = "target/release/lunarbase-indexer")]
    pub indexer_bin: PathBuf,
    /// Comma-separated fresh-process lane topologies.
    #[arg(long, value_delimiter = ',', default_value = "15,64")]
    pub lanes: Vec<usize>,
    /// Comma-separated quote batch sizes; one uses the single-quote endpoint.
    #[arg(long, value_delimiter = ',', default_value = "1,16,256")]
    pub batch_sizes: Vec<usize>,
    /// Number of distinct directed pairs rotated by every topology.
    #[arg(long, default_value_t = 100)]
    pub pairs: usize,
    /// Measured HTTP calls per topology and batch scenario.
    #[arg(long, default_value_t = 20_000)]
    pub requests: usize,
    /// HTTP calls used to warm connection pools and allocators.
    #[arg(long, default_value_t = 1_024)]
    pub warmup_requests: usize,
    /// Fixed HTTP worker count and indexer admission limit.
    #[arg(long, default_value_t = 128)]
    pub concurrency: usize,
    /// Maximum wait for each fresh process to report ready.
    #[arg(long, default_value_t = 15)]
    pub readiness_timeout_seconds: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexerBenchmarkReport {
    schema_version: u32,
    kind: &'static str,
    release_profile_required: bool,
    environment: EnvironmentFingerprint,
    scenarios: Vec<LoadReport>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvironmentFingerprint {
    target: &'static str,
    build_profile: &'static str,
    cpu_model: String,
    logical_cpus: usize,
    rustc_version: &'static str,
    cargo_version: &'static str,
}

/// Runs all requested scenarios, using and gracefully stopping a fresh indexer each time.
pub async fn run(arguments: IndexerBenchmarkArguments) -> Result<(), E2eError> {
    validate(&arguments)?;
    let environment = environment_fingerprint()?;
    let mock = MockChain::start().await?;
    let workspace = Workspace::create()?;
    let mut scenarios = Vec::with_capacity(arguments.lanes.len() * arguments.batch_sizes.len());
    for &lanes in &arguments.lanes {
        for &batch_size in &arguments.batch_sizes {
            scenarios.push(run_scenario(&arguments, &mock, &workspace, lanes, batch_size).await?);
        }
    }
    mock.stop().await;
    println!(
        "{}",
        serde_json::to_string_pretty(&IndexerBenchmarkReport {
            schema_version: REPORT_SCHEMA_VERSION,
            kind: "lunarbase-indexer-process-matrix",
            release_profile_required: true,
            environment,
            scenarios,
        })
        .map_err(|error| E2eError::Scenario(error.to_string()))?
    );
    Ok(())
}

fn validate(arguments: &IndexerBenchmarkArguments) -> Result<(), E2eError> {
    if !arguments.indexer_bin.is_file() {
        return Err(E2eError::Scenario(format!(
            "release indexer binary `{}` does not exist",
            arguments.indexer_bin.display()
        )));
    }
    if arguments.lanes.is_empty()
        || arguments.lanes.contains(&0)
        || arguments.batch_sizes.is_empty()
        || arguments
            .batch_sizes
            .iter()
            .any(|size| !(1..=256).contains(size))
        || arguments.pairs == 0
        || arguments.requests == 0
        || arguments.warmup_requests == 0
        || arguments.concurrency == 0
        || arguments.readiness_timeout_seconds == 0
    {
        return Err(E2eError::Scenario(
            "lanes, pairs, calls, warmup, concurrency, and readiness timeout must be non-zero; batch sizes must be 1..=256".into(),
        ));
    }
    Ok(())
}

fn environment_fingerprint() -> Result<EnvironmentFingerprint, E2eError> {
    let target = env!("LUNARBASE_BUILD_TARGET");
    let build_profile = env!("LUNARBASE_BUILD_PROFILE");
    let rustc_version = env!("LUNARBASE_BUILD_RUSTC_VERSION");
    let cargo_version = env!("LUNARBASE_BUILD_CARGO_VERSION");
    let cpu_model = std::env::var("LUNARBASE_BENCH_CPU_MODEL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(linux_cpu_model)
        .ok_or_else(|| E2eError::Scenario("could not fingerprint the CPU model".into()))?;
    let logical_cpus = std::thread::available_parallelism()
        .map(usize::from)
        .map_err(|error| {
            E2eError::Scenario(format!("could not fingerprint logical CPUs: {error}"))
        })?;
    for (name, value) in [
        ("target", target),
        ("build profile", build_profile),
        ("rustc version", rustc_version),
        ("Cargo version", cargo_version),
    ] {
        if value == "unknown" || value.is_empty() {
            return Err(E2eError::Scenario(format!(
                "could not fingerprint {name}; rebuild through Cargo"
            )));
        }
    }
    if build_profile != "release" {
        return Err(E2eError::Scenario(format!(
            "process benchmark must use a release-built harness, observed `{build_profile}`"
        )));
    }
    Ok(EnvironmentFingerprint {
        target,
        build_profile,
        cpu_model,
        logical_cpus,
        rustc_version,
        cargo_version,
    })
}

fn linux_cpu_model() -> Option<String> {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    cpuinfo.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.trim() == "model name").then(|| value.trim().to_owned())
    })
}

async fn run_scenario(
    arguments: &IndexerBenchmarkArguments,
    mock: &MockChain,
    workspace: &Workspace,
    lanes: usize,
    batch_size: usize,
) -> Result<LoadReport, E2eError> {
    let port = free_port()?;
    let config = workspace.config(&format!("indexer-{lanes}-{batch_size}"));
    write_config(&config, mock, port, lanes, arguments.concurrency)?;
    let mut child = spawn_indexer(&arguments.indexer_bin, &config)?;
    let pid = child
        .id()
        .ok_or_else(|| E2eError::Scenario("spawned indexer has no process ID".into()))?;
    let measured = execute(LoadArguments {
        indexer_url: format!("http://127.0.0.1:{port}"),
        vectors: None,
        lanes,
        pairs: arguments.pairs,
        batch_size,
        requests: arguments.requests,
        warmup_requests: arguments.warmup_requests,
        concurrency: arguments.concurrency,
        burst_size: 1,
        event_burst_url: None,
        pid: Some(pid),
        readiness_timeout_seconds: arguments.readiness_timeout_seconds,
    })
    .await;
    let shutdown = terminate(&mut child).await;
    let report = measured.map_err(|error| E2eError::Scenario(error.to_string()))?;
    shutdown.map_err(|error| {
        E2eError::Scenario(format!(
            "indexer shutdown failed after {lanes}-lane batch-{batch_size} scenario: {error}"
        ))
    })?;
    report
        .ensure_success()
        .map_err(|error| E2eError::Scenario(error.to_string()))?;
    Ok(report)
}

fn write_config(
    path: &Path,
    mock: &MockChain,
    port: u16,
    lanes: usize,
    concurrency: usize,
) -> Result<(), E2eError> {
    std::fs::write(
        path,
        config_contents(
            &mock.rpc_url(),
            &mock.websocket_url(),
            port,
            lanes,
            concurrency,
        ),
    )?;
    Ok(())
}

fn config_contents(
    rpc_url: &str,
    websocket_url: &str,
    port: u16,
    lanes: usize,
    concurrency: usize,
) -> String {
    let lane_list = (0..lanes)
        .map(|index| format!("\"{}\"", lane_address(index)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"network = "base"
chain_id = 8453
core = "{CORE}"
fee_class = "whitelisted"
deployment_block = 0
expected_implementation = "{IMPLEMENTATION}"
expected_implementation_code_hash = "{EMPTY_CODE_HASH}"
http_rpc_url = "{rpc_url}"
realtime_url = "{websocket_url}"
bind = "127.0.0.1:{port}"
explicit_lane_assets = [{lane_list}]
reconnect_delay_milliseconds = 100
source_stall_timeout_milliseconds = 60000
source_operation_timeout_milliseconds = 15000
max_in_flight_quotes = {concurrency}
checkpoint_interval_seconds = 3600
shutdown_timeout_seconds = 8
"#
    )
}

#[cfg(test)]
mod tests {
    use super::{IndexerBenchmarkArguments, config_contents, validate};

    #[test]
    fn rejects_an_unsupported_batch_size_before_starting_processes() {
        let arguments = IndexerBenchmarkArguments {
            indexer_bin: std::env::current_exe().unwrap(),
            lanes: vec![15],
            batch_sizes: vec![257],
            pairs: 100,
            requests: 1,
            warmup_requests: 1,
            concurrency: 1,
            readiness_timeout_seconds: 1,
        };
        assert!(validate(&arguments).is_err());
    }

    #[test]
    fn benchmark_config_inherits_production_queue_defaults() {
        let contents = config_contents("http://rpc", "ws://stream", 8080, 64, 128);
        assert!(!contents.contains("queue_bound"));
        assert!(!contents.contains("queue_byte_bound"));
        assert!(contents.contains("max_in_flight_quotes = 128"));
    }
}
