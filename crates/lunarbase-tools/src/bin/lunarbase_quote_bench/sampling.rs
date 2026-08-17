//! Bounded deterministic latency sampling for sustained timing runs.

/// Fixed-size per-worker ring retaining the deterministic tail of a warmed run.
#[derive(Debug)]
pub(super) struct LatencySampler {
    values: Vec<u64>,
    capacity: usize,
    cursor: usize,
}

impl LatencySampler {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            capacity,
            cursor: 0,
        }
    }

    pub(super) fn push(&mut self, value: u64) {
        if self.values.len() < self.capacity {
            self.values.push(value);
        } else if self.capacity > 0 {
            self.values[self.cursor] = value;
            self.cursor = (self.cursor + 1) % self.capacity;
        }
    }

    pub(super) fn into_vec(self) -> Vec<u64> {
        self.values
    }
}

pub(super) fn distributed(total: usize, workers: usize, worker: usize) -> usize {
    total / workers + usize::from(worker < total % workers)
}

pub(super) fn percentile(sorted: &[u64], quantile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (((sorted.len() - 1) as f64) * quantile).round() as usize;
    sorted[index]
}

#[cfg(test)]
mod tests {
    use super::{LatencySampler, distributed, percentile};

    #[test]
    fn samples_are_bounded_and_work_is_distributed() {
        let mut sampler = LatencySampler::new(3);
        for value in 1..=5 {
            sampler.push(value);
        }
        let mut values = sampler.into_vec();
        values.sort_unstable();
        assert_eq!(values, [3, 4, 5]);
        assert_eq!(percentile(&[10, 20, 30, 40], 0.50), 30);
        assert_eq!(
            (0..3)
                .map(|worker| distributed(8, 3, worker))
                .sum::<usize>(),
            8
        );
    }
}
