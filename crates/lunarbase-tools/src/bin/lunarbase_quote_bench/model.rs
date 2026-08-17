//! Stable report schema and host fingerprint for the quote benchmark.

use lunarbase_tools::support::quote_mixed::MixedLoadReport;
use serde::Serialize;
use std::time::Duration;

pub(super) const REPORT_SCHEMA_VERSION: u8 = 2;
pub(super) const HARNESS_ID: &str = "lunarbase-quote-hot-path-v2";
pub(super) const BUILD_TARGET: &str = env!("LUNARBASE_BUILD_TARGET");
pub(super) const BUILD_PROFILE: &str = env!("LUNARBASE_BUILD_PROFILE");
const BUILD_RUSTC_VERSION: &str = env!("LUNARBASE_BUILD_RUSTC_VERSION");
const BUILD_CARGO_VERSION: &str = env!("LUNARBASE_BUILD_CARGO_VERSION");

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct BenchmarkReport {
    pub(super) schema_version: u8,
    pub(super) scenario_id: String,
    pub(super) mode: &'static str,
    pub(super) build_profile: &'static str,
    pub(super) target: String,
    pub(super) allocator_instrumented: bool,
    pub(super) environment: EnvironmentReport,
    pub(super) measurement_policy: MeasurementPolicy,
    pub(super) lanes: usize,
    pub(super) pairs: usize,
    pub(super) batch_size: usize,
    pub(super) concurrency: usize,
    pub(super) warmup_calls: usize,
    pub(super) measured_calls: usize,
    pub(super) measured_quotes: usize,
    pub(super) timing: Option<TimingReport>,
    pub(super) allocations: Option<AllocationReport>,
    pub(super) event_load: Option<MixedLoadReport>,
    pub(super) rss_bytes: MemoryReport,
    pub(super) checksum: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TimingReport {
    pub(super) duration_ns: u64,
    pub(super) calls_per_second: f64,
    pub(super) quotes_per_second: f64,
    pub(super) latency_ns: LatencyReport,
}

#[derive(Debug, Serialize)]
pub(super) struct LatencyReport {
    pub(super) p50: u64,
    pub(super) p95: u64,
    pub(super) p99: u64,
    pub(super) samples: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EnvironmentReport {
    pub(super) cpu_model: String,
    pub(super) logical_cpus: usize,
    pub(super) rustc_version: String,
    pub(super) cargo_version: String,
    pub(super) harness_id: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MeasurementPolicy {
    pub(super) minimum_duration_ns: u64,
    pub(super) minimum_latency_samples: usize,
    pub(super) latency_sample_capacity: usize,
    pub(super) minimum_measured_quotes: usize,
    pub(super) allocation_calls: usize,
    pub(super) minimum_mixed_updates: u64,
    pub(super) minimum_mixed_rate_bps: u16,
    pub(super) minimum_mixed_applied_during_bps: u16,
    pub(super) mixed_publisher_runtime: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AllocationReport {
    pub(super) count_total: u64,
    pub(super) count_current: i64,
    pub(super) count_max: u64,
    pub(super) bytes_total: u64,
    pub(super) bytes_current: i64,
    pub(super) bytes_max: u64,
    pub(super) count_per_call: f64,
    pub(super) bytes_per_call: f64,
    pub(super) count_per_quote: f64,
    pub(super) bytes_per_quote: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MemoryReport {
    pub(super) before_fixture: Option<u64>,
    pub(super) ready: Option<u64>,
    pub(super) after: Option<u64>,
    pub(super) peak: Option<u64>,
}

pub(super) fn environment_report() -> EnvironmentReport {
    EnvironmentReport {
        cpu_model: std::env::var("LUNARBASE_BENCH_CPU_MODEL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(linux_cpu_model)
            .unwrap_or_else(|| "unknown".into()),
        logical_cpus: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(0),
        rustc_version: environment_value("LUNARBASE_BENCH_RUSTC_VERSION", BUILD_RUSTC_VERSION),
        cargo_version: environment_value("LUNARBASE_BENCH_CARGO_VERSION", BUILD_CARGO_VERSION),
        harness_id: HARNESS_ID,
    }
}

fn environment_value(name: &str, embedded: &str) -> String {
    runtime_or_embedded(std::env::var(name).ok(), embedded)
}

fn runtime_or_embedded(runtime: Option<String>, embedded: &str) -> String {
    runtime
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| embedded.to_owned())
}

fn linux_cpu_model() -> Option<String> {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    cpuinfo.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.trim() == "model name").then(|| value.trim().to_owned())
    })
}

pub(super) fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub(super) fn process_memory() -> (Option<u64>, Option<u64>) {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return (None, None);
    };
    (
        status_bytes(&status, "VmRSS:"),
        status_bytes(&status, "VmHWM:"),
    )
}

pub(super) fn validate_mixed_load(
    report: &MixedLoadReport,
    minimum_duration_ms: u64,
    minimum_updates: u64,
    minimum_rate_bps: u16,
    minimum_applied_during_bps: u16,
) -> Result<(), String> {
    let achieved = u128::from(report.published_updates)
        .saturating_mul(1_000_000_000)
        .saturating_mul(10_000);
    let required = u128::from(report.configured_events_per_second)
        .saturating_mul(u128::from(report.duration_ns))
        .saturating_mul(u128::from(minimum_rate_bps));
    let applied_during = u128::from(report.applied_during_measurement).saturating_mul(10_000);
    let required_during = u128::from(report.published_during_measurement)
        .saturating_mul(u128::from(minimum_applied_during_bps));
    if report.published_updates < minimum_updates
        || report.duration_ns < minimum_duration_ms.saturating_mul(1_000_000)
        || report.applied_updates != report.published_updates
        || achieved < required
        || report.published_during_measurement == 0
        || applied_during < required_during
    {
        return Err(format!(
            "mixed publisher missed its load contract: published={}, applied={}, published_during={}, applied_during={}, duration_ns={}, minimum_updates={}, minimum_rate_bps={}, minimum_applied_during_bps={}",
            report.published_updates,
            report.applied_updates,
            report.published_during_measurement,
            report.applied_during_measurement,
            report.duration_ns,
            minimum_updates,
            minimum_rate_bps,
            minimum_applied_during_bps,
        ));
    }
    Ok(())
}

fn status_bytes(status: &str, name: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        let value = line.strip_prefix(name)?.split_whitespace().next()?;
        value.parse::<u64>().ok()?.checked_mul(1_024)
    })
}

#[cfg(test)]
mod tests {
    use super::{duration_ns, runtime_or_embedded, status_bytes};
    use std::time::Duration;

    #[test]
    fn report_helpers_use_stable_integer_units() {
        assert_eq!(duration_ns(Duration::from_micros(7)), 7_000);
        assert_eq!(status_bytes("VmRSS:\t  42 kB\n", "VmRSS:"), Some(43_008));
    }

    #[test]
    fn runtime_toolchain_identity_overrides_the_embedded_fallback() {
        assert_eq!(
            runtime_or_embedded(Some("runtime".into()), "embedded"),
            "runtime"
        );
        assert_eq!(
            runtime_or_embedded(Some("  ".into()), "embedded"),
            "embedded"
        );
        assert_eq!(runtime_or_embedded(None, "embedded"), "embedded");
    }
}
