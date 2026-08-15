//! Reproducible in-process benchmark for the connected quote hot path.

use clap::{Parser, ValueEnum};
use lunarbase_client::indexer::errors::IndexerError;
use lunarbase_client::prelude::ConnectedQuoteClient;
use lunarbase_math::{QuoteOutcome, QuoteRequest};
use lunarbase_tools::support::quote_benchmark::{fixture, rotating_batches};
use lunarbase_tools::support::quote_mixed::{
    MixedLoadReport, SyntheticSource, UpdateBus, spawn_mixed_publisher, wait_for_reducer_sequence,
};
use serde::Serialize;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum BenchmarkMode {
    Timing,
    Mixed,
    Allocations,
}

#[derive(Debug, Parser)]
#[command(name = "lunarbase-quote-bench")]
struct Arguments {
    #[arg(long, default_value_t = 15)]
    lanes: usize,
    #[arg(long, default_value_t = 100)]
    pairs: usize,
    #[arg(long, default_value_t = 1)]
    batch_size: usize,
    #[arg(long, default_value_t = 128)]
    concurrency: usize,
    #[arg(long, default_value_t = 1_048_576)]
    measured_quotes: usize,
    #[arg(long, default_value_t = 4_096)]
    allocation_calls: usize,
    #[arg(long, default_value_t = 4_096)]
    warmup_calls: usize,
    #[arg(long, value_enum, default_value_t = BenchmarkMode::Timing)]
    mode: BenchmarkMode,
    #[arg(long, default_value_t = 1_000)]
    mixed_events_per_second: u64,
}

#[derive(Debug, Error)]
enum BenchmarkError {
    #[error("invalid benchmark settings: {0}")]
    Invalid(String),
    #[error(transparent)]
    Client(#[from] IndexerError),
    #[error("benchmark worker panicked")]
    WorkerPanicked,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkReport {
    schema_version: u8,
    scenario_id: String,
    mode: &'static str,
    build_profile: &'static str,
    target: String,
    allocator_instrumented: bool,
    lanes: usize,
    pairs: usize,
    batch_size: usize,
    concurrency: usize,
    warmup_calls: usize,
    measured_calls: usize,
    measured_quotes: usize,
    timing: Option<TimingReport>,
    allocations: Option<AllocationReport>,
    event_load: Option<MixedLoadReport>,
    rss_bytes: MemoryReport,
    checksum: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TimingReport {
    duration_ns: u64,
    calls_per_second: f64,
    quotes_per_second: f64,
    latency_ns: LatencyReport,
}

#[derive(Debug, Serialize)]
struct LatencyReport {
    p50: u64,
    p95: u64,
    p99: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AllocationReport {
    count_total: u64,
    count_current: i64,
    count_max: u64,
    bytes_total: u64,
    bytes_current: i64,
    bytes_max: u64,
    count_per_call: f64,
    bytes_per_call: f64,
    count_per_quote: f64,
    bytes_per_quote: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MemoryReport {
    before_fixture: Option<u64>,
    ready: Option<u64>,
    after: Option<u64>,
    peak: Option<u64>,
}

#[derive(Debug)]
struct TimingResult {
    report: TimingReport,
    checksum: u64,
}

#[derive(Debug)]
struct WorkerResult {
    latencies: Vec<u64>,
    checksum: u64,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Arguments::parse()).await {
        eprintln!("lunarbase-quote-bench failed: {error}");
        std::process::exit(1);
    }
}

async fn run(arguments: Arguments) -> Result<(), BenchmarkError> {
    validate_arguments(&arguments)?;
    let before_fixture = process_memory().0;
    let benchmark_fixture =
        fixture(arguments.lanes, arguments.pairs).map_err(BenchmarkError::Invalid)?;
    let batches = Arc::new(
        rotating_batches(&benchmark_fixture.requests, arguments.batch_size)
            .map_err(BenchmarkError::Invalid)?,
    );
    let lane_asset = benchmark_fixture.connect.deployment.explicit_lane_assets[0];
    let lane_slot0 = benchmark_fixture.snapshot.state.lanes[&lane_asset].slot0;
    let mut update_cursor = benchmark_fixture.snapshot.cursor.clone();
    update_cursor.block_number = update_cursor.block_number.saturating_add(1);
    update_cursor.execution_block_number = update_cursor.execution_block_number.saturating_add(1);
    update_cursor.block_hash = Some(lunarbase_math::B256::new([9; 32]));
    let core_address = benchmark_fixture.connect.deployment.core;
    let updates = UpdateBus::new(4_096, 4 * 1024 * 1024);
    let source = Arc::new(SyntheticSource::new(
        benchmark_fixture.snapshot,
        updates.clone(),
    ));
    let client =
        Arc::new(ConnectedQuoteClient::connect(benchmark_fixture.connect, source, None).await?);
    validate_available(&client, &benchmark_fixture.requests)?;
    warm_up(
        &client,
        &batches,
        arguments.batch_size,
        arguments.warmup_calls,
    )?;
    let ready_memory = process_memory().0;

    let result = match arguments.mode {
        BenchmarkMode::Timing => {
            run_timing(&arguments, client.clone(), batches.clone()).map(|timing| {
                (
                    Some(timing.report),
                    None,
                    None,
                    timing.checksum,
                    measured_calls(&arguments),
                )
            })
        }
        BenchmarkMode::Mixed => {
            let initial_sequence = update_cursor.source_sequence.unwrap_or(0);
            let publisher = spawn_mixed_publisher(
                updates,
                core_address,
                lane_asset,
                lane_slot0,
                update_cursor,
                arguments.mixed_events_per_second,
            );
            wait_for_reducer_sequence(&client, initial_sequence.saturating_add(1))
                .await
                .map_err(BenchmarkError::Invalid)?;
            let timing = run_timing(&arguments, client.clone(), batches.clone());
            let mut event_load = publisher.finish().await;
            let expected_sequence = initial_sequence.saturating_add(event_load.published_updates);
            let applied_sequence = wait_for_reducer_sequence(&client, expected_sequence)
                .await
                .map_err(BenchmarkError::Invalid)?;
            event_load.applied_updates = applied_sequence
                .saturating_sub(initial_sequence)
                .min(event_load.published_updates);
            timing.map(|timing| {
                (
                    Some(timing.report),
                    None,
                    Some(event_load),
                    timing.checksum,
                    measured_calls(&arguments),
                )
            })
        }
        BenchmarkMode::Allocations => measure_allocations(
            client.clone(),
            &batches,
            arguments.batch_size,
            arguments.allocation_calls,
        )
        .map(|(allocations, checksum)| {
            (
                None,
                Some(allocations),
                None,
                checksum,
                arguments.allocation_calls,
            )
        }),
    };
    let shutdown = client.shutdown_gracefully(Duration::from_secs(2)).await;
    let (timing, allocations, event_load, checksum, calls) = result?;
    shutdown?;
    let (after, peak) = process_memory();
    let report = BenchmarkReport {
        schema_version: 1,
        scenario_id: scenario_id(&arguments),
        mode: match arguments.mode {
            BenchmarkMode::Timing => "timing",
            BenchmarkMode::Mixed => "mixed",
            BenchmarkMode::Allocations => "allocations",
        },
        build_profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        target: format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
        allocator_instrumented: cfg!(feature = "allocation-stats"),
        lanes: arguments.lanes,
        pairs: arguments.pairs,
        batch_size: arguments.batch_size,
        concurrency: arguments.concurrency,
        warmup_calls: arguments.warmup_calls,
        measured_calls: calls,
        measured_quotes: calls.saturating_mul(arguments.batch_size),
        timing,
        allocations,
        event_load,
        rss_bytes: MemoryReport {
            before_fixture,
            ready: ready_memory,
            after,
            peak,
        },
        checksum,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn validate_arguments(arguments: &Arguments) -> Result<(), BenchmarkError> {
    if arguments.concurrency == 0
        || arguments.measured_quotes == 0
        || arguments.allocation_calls == 0
        || arguments.warmup_calls == 0
        || arguments.mixed_events_per_second == 0
    {
        return Err(BenchmarkError::Invalid(
            "concurrency, measured quotes, allocation calls, and warmup calls must be non-zero"
                .into(),
        ));
    }
    match arguments.mode {
        BenchmarkMode::Timing | BenchmarkMode::Mixed if cfg!(feature = "allocation-stats") => Err(
            BenchmarkError::Invalid("timing modes must run without allocation-stats".into()),
        ),
        BenchmarkMode::Allocations if !cfg!(feature = "allocation-stats") => Err(
            BenchmarkError::Invalid("allocation mode requires --features allocation-stats".into()),
        ),
        BenchmarkMode::Allocations if arguments.concurrency != 1 => Err(BenchmarkError::Invalid(
            "allocation mode is thread-local and requires --concurrency 1".into(),
        )),
        _ => Ok(()),
    }
}

fn validate_available(
    client: &ConnectedQuoteClient,
    requests: &[QuoteRequest],
) -> Result<(), BenchmarkError> {
    for request in requests {
        if !matches!(client.quote(request)?.outcome, QuoteOutcome::Available(_)) {
            return Err(BenchmarkError::Invalid(
                "synthetic quote fixture produced an unavailable request".into(),
            ));
        }
    }
    Ok(())
}

fn warm_up(
    client: &ConnectedQuoteClient,
    batches: &[Vec<QuoteRequest>],
    batch_size: usize,
    calls: usize,
) -> Result<(), BenchmarkError> {
    for index in 0..calls {
        black_box(evaluate(
            client,
            &batches[index % batches.len()],
            batch_size,
        )?);
    }
    Ok(())
}

fn run_timing(
    arguments: &Arguments,
    client: Arc<ConnectedQuoteClient>,
    batches: Arc<Vec<Vec<QuoteRequest>>>,
) -> Result<TimingResult, BenchmarkError> {
    let calls = measured_calls(arguments);
    let barrier = Arc::new(Barrier::new(arguments.concurrency + 1));
    let workers = (0..arguments.concurrency)
        .map(|worker| {
            let client = client.clone();
            let batches = batches.clone();
            let barrier = barrier.clone();
            let worker_calls =
                calls / arguments.concurrency + usize::from(worker < calls % arguments.concurrency);
            let concurrency = arguments.concurrency;
            let batch_size = arguments.batch_size;
            std::thread::spawn(move || -> Result<WorkerResult, BenchmarkError> {
                let mut latencies = Vec::with_capacity(worker_calls);
                let mut checksum = 0u64;
                barrier.wait();
                for iteration in 0..worker_calls {
                    let batch = &batches[(worker + iteration * concurrency) % batches.len()];
                    let request_started = Instant::now();
                    checksum = checksum.wrapping_add(evaluate(&client, batch, batch_size)?);
                    latencies.push(duration_ns(request_started.elapsed()));
                }
                Ok(WorkerResult {
                    latencies,
                    checksum,
                })
            })
        })
        .collect::<Vec<_>>();
    let measurement_started = Instant::now();
    barrier.wait();
    let mut latencies = Vec::with_capacity(calls);
    let mut checksum = 0u64;
    for worker in workers {
        let result = worker
            .join()
            .map_err(|_| BenchmarkError::WorkerPanicked)??;
        latencies.extend(result.latencies);
        checksum = checksum.wrapping_add(result.checksum);
    }
    let elapsed = measurement_started.elapsed();
    latencies.sort_unstable();
    let seconds = elapsed.as_secs_f64();
    Ok(TimingResult {
        report: TimingReport {
            duration_ns: duration_ns(elapsed),
            calls_per_second: calls as f64 / seconds,
            quotes_per_second: calls.saturating_mul(arguments.batch_size) as f64 / seconds,
            latency_ns: LatencyReport {
                p50: percentile(&latencies, 0.50),
                p95: percentile(&latencies, 0.95),
                p99: percentile(&latencies, 0.99),
            },
        },
        checksum,
    })
}

fn evaluate(
    client: &ConnectedQuoteClient,
    batch: &[QuoteRequest],
    batch_size: usize,
) -> Result<u64, BenchmarkError> {
    if batch_size == 1 {
        return Ok(outcome_checksum(&client.quote(&batch[0])?.outcome));
    }
    let quote = client.quote_many(batch)?;
    Ok(quote.outcomes.iter().fold(0u64, |sum, outcome| {
        sum.wrapping_add(outcome_checksum(outcome))
    }))
}

fn outcome_checksum(outcome: &QuoteOutcome) -> u64 {
    match outcome {
        QuoteOutcome::Available(result) => result
            .amount_in
            .to::<u64>()
            .wrapping_add(result.amount_out.to::<u64>().rotate_left(17))
            .wrapping_add(result.fee_amount.to::<u64>().rotate_left(31)),
        QuoteOutcome::Unavailable(_) => 0,
    }
}

#[cfg(feature = "allocation-stats")]
fn measure_allocations(
    client: Arc<ConnectedQuoteClient>,
    batches: &[Vec<QuoteRequest>],
    batch_size: usize,
    calls: usize,
) -> Result<(AllocationReport, u64), BenchmarkError> {
    let mut checksum = 0u64;
    let mut failure = None;
    let info = allocation_counter::measure(|| {
        for index in 0..calls {
            match evaluate(&client, &batches[index % batches.len()], batch_size) {
                Ok(value) => checksum = checksum.wrapping_add(value),
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }
    let quotes = calls.saturating_mul(batch_size);
    Ok((
        AllocationReport {
            count_total: info.count_total,
            count_current: info.count_current,
            count_max: info.count_max,
            bytes_total: info.bytes_total,
            bytes_current: info.bytes_current,
            bytes_max: info.bytes_max,
            count_per_call: info.count_total as f64 / calls as f64,
            bytes_per_call: info.bytes_total as f64 / calls as f64,
            count_per_quote: info.count_total as f64 / quotes as f64,
            bytes_per_quote: info.bytes_total as f64 / quotes as f64,
        },
        checksum,
    ))
}

#[cfg(not(feature = "allocation-stats"))]
fn measure_allocations(
    _client: Arc<ConnectedQuoteClient>,
    _batches: &[Vec<QuoteRequest>],
    _batch_size: usize,
    _calls: usize,
) -> Result<(AllocationReport, u64), BenchmarkError> {
    Err(BenchmarkError::Invalid(
        "allocation mode requires --features allocation-stats".into(),
    ))
}

fn measured_calls(arguments: &Arguments) -> usize {
    arguments.measured_quotes.div_ceil(arguments.batch_size)
}

fn scenario_id(arguments: &Arguments) -> String {
    let mode = match arguments.mode {
        BenchmarkMode::Timing => "timing",
        BenchmarkMode::Mixed => "mixed",
        BenchmarkMode::Allocations => "allocations",
    };
    format!(
        "{mode}-lanes{}-pairs{}-batch{}-c{}",
        arguments.lanes, arguments.pairs, arguments.batch_size, arguments.concurrency
    )
}

fn percentile(sorted: &[u64], quantile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (((sorted.len() - 1) as f64) * quantile).round() as usize;
    sorted[index]
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn process_memory() -> (Option<u64>, Option<u64>) {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return (None, None);
    };
    (
        status_bytes(&status, "VmRSS:"),
        status_bytes(&status, "VmHWM:"),
    )
}

fn status_bytes(status: &str, name: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        let value = line.strip_prefix(name)?.split_whitespace().next()?;
        value.parse::<u64>().ok()?.checked_mul(1_024)
    })
}

#[cfg(test)]
mod tests {
    use super::{duration_ns, percentile, status_bytes};
    use std::time::Duration;

    #[test]
    fn report_helpers_use_stable_integer_units() {
        assert_eq!(percentile(&[10, 20, 30, 40], 0.50), 30);
        assert_eq!(duration_ns(Duration::from_micros(7)), 7_000);
        assert_eq!(status_bytes("VmRSS:\t  42 kB\n", "VmRSS:"), Some(43_008));
    }
}
