//! Reproducible in-process benchmark for the connected quote hot path.

#[path = "lunarbase_quote_bench/model.rs"]
mod model;
#[path = "lunarbase_quote_bench/sampling.rs"]
mod sampling;

use clap::{Parser, ValueEnum};
use lunarbase_client::indexer::errors::IndexerError;
use lunarbase_client::prelude::ConnectedQuoteClient;
use lunarbase_math::{QuoteOutcome, QuoteRequest};
use lunarbase_tools::support::quote_benchmark::{fixture, rotating_batches};
use lunarbase_tools::support::quote_mixed::{
    SyntheticSource, UpdateBus, spawn_mixed_publisher, wait_for_reducer_sequence,
};
use model::{
    AllocationReport, BUILD_PROFILE, BUILD_TARGET, BenchmarkReport, LatencyReport,
    MeasurementPolicy, MemoryReport, REPORT_SCHEMA_VERSION, TimingReport, duration_ns,
    environment_report, process_memory, validate_mixed_load,
};
use sampling::{LatencySampler, distributed, percentile};
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
    #[arg(long, default_value_t = 2_000)]
    minimum_duration_ms: u64,
    #[arg(long, default_value_t = 32_768)]
    minimum_latency_samples: usize,
    #[arg(long, default_value_t = 65_536)]
    latency_sample_capacity: usize,
    #[arg(long, default_value_t = 4_096)]
    allocation_calls: usize,
    #[arg(long, default_value_t = 4_096)]
    warmup_calls: usize,
    #[arg(long, value_enum, default_value_t = BenchmarkMode::Timing)]
    mode: BenchmarkMode,
    #[arg(long, default_value_t = 1_000)]
    mixed_events_per_second: u64,
    #[arg(long, default_value_t = 1_500)]
    minimum_mixed_updates: u64,
    #[arg(long, default_value_t = 8_000)]
    minimum_mixed_rate_bps: u16,
    #[arg(long, default_value_t = 8_000)]
    minimum_mixed_applied_during_bps: u16,
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

#[derive(Debug)]
struct TimingResult {
    report: TimingReport,
    checksum: u64,
    measured_calls: usize,
}

#[derive(Debug)]
struct WorkerResult {
    latencies: Vec<u64>,
    checksum: u64,
    calls: usize,
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
                    timing.measured_calls,
                )
            })
        }
        BenchmarkMode::Mixed => {
            let initial_sequence = update_cursor.source_sequence.unwrap_or(0);
            let publisher_runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name("quote-bench-publisher")
                .enable_time()
                .build()
                .map_err(|error| BenchmarkError::Invalid(error.to_string()))?;
            let publisher = {
                let _entered = publisher_runtime.enter();
                spawn_mixed_publisher(
                    updates,
                    core_address,
                    lane_asset,
                    lane_slot0,
                    update_cursor,
                    arguments.mixed_events_per_second,
                )
            };
            if let Err(error) =
                wait_for_reducer_sequence(&client, initial_sequence.saturating_add(1)).await
            {
                let _ = publisher.finish().await;
                publisher_runtime.shutdown_background();
                return Err(BenchmarkError::Invalid(error));
            }
            let timing = run_timing(&arguments, client.clone(), batches.clone());
            let applied_during_measurement = client.health().map(|health| {
                health
                    .cursor
                    .and_then(|cursor| cursor.source_sequence)
                    .unwrap_or(0)
            });
            let published_during_measurement = publisher.published_updates();
            let mut event_load = publisher.finish().await;
            publisher_runtime.shutdown_background();
            event_load.published_during_measurement = published_during_measurement;
            event_load.applied_during_measurement = applied_during_measurement?
                .saturating_sub(initial_sequence)
                .min(published_during_measurement);
            let timing = timing?;
            let expected_sequence = initial_sequence.saturating_add(event_load.published_updates);
            let applied_sequence = wait_for_reducer_sequence(&client, expected_sequence)
                .await
                .map_err(BenchmarkError::Invalid)?;
            event_load.applied_updates = applied_sequence
                .saturating_sub(initial_sequence)
                .min(event_load.published_updates);
            validate_mixed_load(
                &event_load,
                arguments.minimum_duration_ms,
                arguments.minimum_mixed_updates,
                arguments.minimum_mixed_rate_bps,
                arguments.minimum_mixed_applied_during_bps,
            )
            .map_err(BenchmarkError::Invalid)?;
            Ok((
                Some(timing.report),
                None,
                Some(event_load),
                timing.checksum,
                timing.measured_calls,
            ))
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
        schema_version: REPORT_SCHEMA_VERSION,
        scenario_id: scenario_id(&arguments),
        mode: match arguments.mode {
            BenchmarkMode::Timing => "timing",
            BenchmarkMode::Mixed => "mixed",
            BenchmarkMode::Allocations => "allocations",
        },
        build_profile: BUILD_PROFILE,
        target: BUILD_TARGET.to_owned(),
        allocator_instrumented: cfg!(feature = "allocation-stats"),
        environment: environment_report(),
        measurement_policy: MeasurementPolicy {
            minimum_duration_ns: arguments.minimum_duration_ms.saturating_mul(1_000_000),
            minimum_latency_samples: arguments.minimum_latency_samples,
            latency_sample_capacity: arguments.latency_sample_capacity,
            minimum_measured_quotes: arguments.measured_quotes,
            allocation_calls: arguments.allocation_calls,
            minimum_mixed_updates: arguments.minimum_mixed_updates,
            minimum_mixed_rate_bps: arguments.minimum_mixed_rate_bps,
            minimum_mixed_applied_during_bps: arguments.minimum_mixed_applied_during_bps,
            mixed_publisher_runtime: "dedicated-deadline",
        },
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
        || arguments.batch_size == 0
        || arguments.measured_quotes == 0
        || arguments.allocation_calls == 0
        || arguments.warmup_calls == 0
        || arguments.mixed_events_per_second == 0
        || arguments.minimum_duration_ms == 0
        || arguments.minimum_latency_samples == 0
        || arguments.latency_sample_capacity < arguments.minimum_latency_samples
        || arguments.minimum_mixed_updates == 0
        || !(1..=10_000).contains(&arguments.minimum_mixed_rate_bps)
        || !(1..=10_000).contains(&arguments.minimum_mixed_applied_during_bps)
    {
        return Err(BenchmarkError::Invalid(
            "benchmark bounds must be non-zero, sample capacity must cover the minimum, and mixed rate bps must be in 1..=10000".into(),
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
    let minimum_duration = Duration::from_millis(arguments.minimum_duration_ms);
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
            let minimum_samples =
                distributed(arguments.minimum_latency_samples, concurrency, worker);
            let sample_capacity =
                distributed(arguments.latency_sample_capacity, concurrency, worker);
            std::thread::spawn(move || -> Result<WorkerResult, BenchmarkError> {
                let mut latencies = LatencySampler::new(sample_capacity);
                let mut checksum = 0u64;
                barrier.wait();
                let worker_started = Instant::now();
                let mut iteration = 0usize;
                while iteration < worker_calls
                    || iteration < minimum_samples
                    || worker_started.elapsed() < minimum_duration
                {
                    let batch = &batches[(worker + iteration * concurrency) % batches.len()];
                    let request_started = Instant::now();
                    checksum = checksum.wrapping_add(evaluate(&client, batch, batch_size)?);
                    latencies.push(duration_ns(request_started.elapsed()));
                    iteration = iteration.saturating_add(1);
                }
                Ok(WorkerResult {
                    latencies: latencies.into_vec(),
                    checksum,
                    calls: iteration,
                })
            })
        })
        .collect::<Vec<_>>();
    let measurement_started = Instant::now();
    barrier.wait();
    let mut latencies = Vec::with_capacity(arguments.latency_sample_capacity);
    let mut checksum = 0u64;
    let mut actual_calls = 0usize;
    for worker in workers {
        let result = worker
            .join()
            .map_err(|_| BenchmarkError::WorkerPanicked)??;
        latencies.extend(result.latencies);
        checksum = checksum.wrapping_add(result.checksum);
        actual_calls = actual_calls.saturating_add(result.calls);
    }
    let elapsed = measurement_started.elapsed();
    latencies.sort_unstable();
    let seconds = elapsed.as_secs_f64();
    Ok(TimingResult {
        report: TimingReport {
            duration_ns: duration_ns(elapsed),
            calls_per_second: actual_calls as f64 / seconds,
            quotes_per_second: actual_calls.saturating_mul(arguments.batch_size) as f64 / seconds,
            latency_ns: LatencyReport {
                p50: percentile(&latencies, 0.50),
                p95: percentile(&latencies, 0.95),
                p99: percentile(&latencies, 0.99),
                samples: latencies.len(),
            },
        },
        checksum,
        measured_calls: actual_calls,
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
