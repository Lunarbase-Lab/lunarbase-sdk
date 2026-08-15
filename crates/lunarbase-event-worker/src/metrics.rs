//! Lock-free health and Prometheus state owned only by the event worker.

use std::{
    fmt::Write,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug)]
pub(crate) struct Metrics {
    ready: AtomicBool,
    source_queue_depth: AtomicUsize,
    source_queue_capacity: usize,
    source_queue_bytes: AtomicUsize,
    source_queue_byte_capacity: usize,
    redis_queue_depth: AtomicUsize,
    redis_queue_capacity: usize,
    redis_queue_bytes: AtomicUsize,
    redis_queue_byte_capacity: usize,
    persisted: AtomicU64,
    duplicates: AtomicU64,
    redis_failures: AtomicU64,
    source_reconnects: AtomicU64,
    source_gaps: AtomicU64,
    queue_saturations: AtomicU64,
    recoveries: AtomicU64,
    recovery_failures: AtomicU64,
    redis_write_nanos: AtomicU64,
    last_source_update_millis: AtomicU64,
    source_head_block: AtomicU64,
    last_persisted_block: AtomicU64,
}

impl Metrics {
    pub(crate) fn new(
        source_queue_capacity: usize,
        source_queue_byte_capacity: usize,
        redis_queue_capacity: usize,
        redis_queue_byte_capacity: usize,
    ) -> Self {
        Self {
            ready: AtomicBool::new(false),
            source_queue_depth: AtomicUsize::new(0),
            source_queue_capacity,
            source_queue_bytes: AtomicUsize::new(0),
            source_queue_byte_capacity,
            redis_queue_depth: AtomicUsize::new(0),
            redis_queue_capacity,
            redis_queue_bytes: AtomicUsize::new(0),
            redis_queue_byte_capacity,
            persisted: AtomicU64::new(0),
            duplicates: AtomicU64::new(0),
            redis_failures: AtomicU64::new(0),
            source_reconnects: AtomicU64::new(0),
            source_gaps: AtomicU64::new(0),
            queue_saturations: AtomicU64::new(0),
            recoveries: AtomicU64::new(0),
            recovery_failures: AtomicU64::new(0),
            redis_write_nanos: AtomicU64::new(0),
            last_source_update_millis: AtomicU64::new(0),
            source_head_block: AtomicU64::new(0),
            last_persisted_block: AtomicU64::new(0),
        }
    }

    pub(crate) fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn source_enqueued(&self, bytes: usize) {
        self.source_queue_depth.fetch_add(1, Ordering::Relaxed);
        self.source_queue_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.last_source_update_millis
            .store(unix_millis(), Ordering::Relaxed);
    }

    pub(crate) fn source_dequeued(&self, bytes: usize) {
        self.source_queue_depth.fetch_sub(1, Ordering::Relaxed);
        self.source_queue_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub(crate) fn queue_saturated(&self) {
        self.set_ready(false);
        self.queue_saturations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn redis_started(&self, bytes: usize) {
        self.redis_queue_depth.fetch_add(1, Ordering::Relaxed);
        self.redis_queue_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn redis_finished(&self, bytes: usize) {
        self.redis_queue_depth.fetch_sub(1, Ordering::Relaxed);
        self.redis_queue_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub(crate) fn persisted(&self, block: u64, duplicate: bool, latency: Duration) {
        if duplicate {
            self.duplicates.fetch_add(1, Ordering::Relaxed);
        } else {
            self.persisted.fetch_add(1, Ordering::Relaxed);
        }
        self.redis_write_nanos
            .fetch_add(saturating_nanos(latency), Ordering::Relaxed);
        self.last_persisted_block.store(block, Ordering::Relaxed);
    }

    pub(crate) fn redis_failure(&self) {
        self.set_ready(false);
        self.redis_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn source_reconnect(&self) {
        self.source_reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn source_gap(&self) {
        self.set_ready(false);
        self.source_gaps.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn recovery(&self) {
        self.recoveries.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn recovery_failure(&self) {
        self.set_ready(false);
        self.recovery_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn observe_head(&self, block: u64) {
        self.source_head_block.fetch_max(block, Ordering::Relaxed);
    }

    pub(crate) fn last_persisted_block(&self) -> u64 {
        self.last_persisted_block.load(Ordering::Relaxed)
    }

    pub(crate) fn queues_empty(&self) -> bool {
        self.source_queue_depth.load(Ordering::Acquire) == 0
            && self.redis_queue_depth.load(Ordering::Acquire) == 0
    }

    pub(crate) fn render(&self) -> String {
        let persisted = self.persisted.load(Ordering::Relaxed);
        let duplicates = self.duplicates.load(Ordering::Relaxed);
        let successful_writes = persisted.saturating_add(duplicates);
        let average_write_seconds = if successful_writes == 0 {
            0.0
        } else {
            self.redis_write_nanos.load(Ordering::Relaxed) as f64 / successful_writes as f64 / 1e9
        };
        let mut output = String::new();
        gauge(
            &mut output,
            "lunarbase_event_worker_ready",
            self.is_ready() as u8,
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_source_queue_depth",
            self.source_queue_depth.load(Ordering::Relaxed),
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_source_queue_capacity",
            self.source_queue_capacity,
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_source_queue_bytes",
            self.source_queue_bytes.load(Ordering::Relaxed),
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_source_queue_byte_capacity",
            self.source_queue_byte_capacity,
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_redis_queue_depth",
            self.redis_queue_depth.load(Ordering::Relaxed),
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_redis_queue_capacity",
            self.redis_queue_capacity,
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_redis_queue_bytes",
            self.redis_queue_bytes.load(Ordering::Relaxed),
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_redis_queue_byte_capacity",
            self.redis_queue_byte_capacity,
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_source_head_block",
            self.source_head_block.load(Ordering::Relaxed),
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_last_persisted_block",
            self.last_persisted_block(),
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_source_update_age_seconds",
            update_age(self.last_source_update_millis.load(Ordering::Relaxed)),
        );
        gauge(
            &mut output,
            "lunarbase_event_worker_redis_write_seconds_average",
            average_write_seconds,
        );
        counter(
            &mut output,
            "lunarbase_event_worker_events_total",
            persisted,
        );
        counter(
            &mut output,
            "lunarbase_event_worker_duplicates_total",
            duplicates,
        );
        counter(
            &mut output,
            "lunarbase_event_worker_redis_failures_total",
            self.redis_failures.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_event_worker_source_reconnects_total",
            self.source_reconnects.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_event_worker_source_gaps_total",
            self.source_gaps.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_event_worker_queue_saturations_total",
            self.queue_saturations.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_event_worker_recoveries_total",
            self.recoveries.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_event_worker_recovery_failures_total",
            self.recovery_failures.load(Ordering::Relaxed),
        );
        output
    }
}

fn gauge(output: &mut String, name: &str, value: impl std::fmt::Display) {
    let _ = writeln!(output, "# TYPE {name} gauge");
    let _ = writeln!(output, "{name} {value}");
}

fn counter(output: &mut String, name: &str, value: impl std::fmt::Display) {
    let _ = writeln!(output, "# TYPE {name} counter");
    let _ = writeln!(output, "{name} {value}");
}

fn saturating_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn update_age(last_millis: u64) -> f64 {
    if last_millis == 0 {
        return f64::NAN;
    }
    unix_millis().saturating_sub(last_millis) as f64 / 1_000.0
}

#[cfg(test)]
mod tests {
    use super::update_age;

    #[test]
    fn missing_source_update_renders_as_prometheus_nan() {
        assert!(update_age(0).is_nan());
    }
}
