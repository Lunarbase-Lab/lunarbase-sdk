//! Exact-owned dynamic bytes at bounded ingestion and retention boundaries.

use super::{ChainCorrection, ChainUpdate, ContractLog};
use lunarbase_math::Bytes;

impl ContractLog {
    /// Detaches the visible payload from any larger or shared backing buffer.
    ///
    /// Non-empty data is rebuilt through an exact-length boxed slice. Moving a
    /// uniquely owned, exact-capacity buffer remains allocation-free; sliced,
    /// shared, static, and externally owned buffers become tightly owned.
    pub fn normalize_for_retention(&mut self) {
        let data = std::mem::take(&mut self.data);
        self.data = exact_owned_bytes(data);
    }
}

impl ChainCorrection {
    /// Normalizes every replacement-log payload before bounded retention.
    pub fn normalize_for_retention(&mut self) {
        for log in &mut self.replacement_logs {
            log.normalize_for_retention();
        }
    }
}

impl ChainUpdate {
    /// Normalizes every dynamic byte buffer before bounded retention.
    pub fn normalize_for_retention(&mut self) {
        match self {
            Self::Log(log) => log.normalize_for_retention(),
            Self::Correction(correction) => correction.normalize_for_retention(),
            Self::Head(_) | Self::Reorg { .. } | Self::Gap { .. } => {}
        }
    }
}

fn exact_owned_bytes(data: Bytes) -> Bytes {
    if data.is_empty() {
        return Bytes::new();
    }
    let data: Vec<u8> = data.into();
    Bytes::from(data.into_boxed_slice())
}

#[cfg(test)]
mod tests {
    use super::exact_owned_bytes;
    use lunarbase_math::Bytes;

    const BACKING_BYTES: usize = 1 << 20;

    #[test]
    fn unique_prefix_slice_is_reboxed_to_its_visible_length() {
        assert_tightly_owned(0..1, 0x41);
    }

    #[test]
    fn unique_tail_slice_is_reboxed_to_its_visible_length() {
        assert_tightly_owned(BACKING_BYTES - 1..BACKING_BYTES, 0x41);
    }

    #[test]
    fn shared_slice_is_detached_from_its_live_backing() {
        let backing = Bytes::from(vec![0x52; BACKING_BYTES]);
        let slice = backing.slice(BACKING_BYTES - 1..);
        let backing_ptr = backing.as_ptr();
        let normalized = exact_owned_bytes(slice);

        assert_eq!(normalized.as_ref(), [0x52]);
        assert_ne!(
            normalized.as_ptr(),
            backing_ptr.wrapping_add(BACKING_BYTES - 1)
        );
        drop(backing);
        assert_exact_capacity(normalized);
    }

    #[test]
    fn already_tight_unique_data_keeps_its_pointer_when_normalized_again() {
        let normalized = exact_owned_bytes(Bytes::from(vec![0x63; 64]));
        let pointer = normalized.as_ptr();
        let normalized = exact_owned_bytes(normalized);

        assert_eq!(normalized.as_ptr(), pointer);
        assert_exact_capacity(normalized);
    }

    fn assert_tightly_owned(range: std::ops::Range<usize>, expected: u8) {
        let backing = Bytes::from(vec![expected; BACKING_BYTES]);
        let slice = backing.slice(range);
        drop(backing);

        let normalized = exact_owned_bytes(slice);
        assert_eq!(normalized.as_ref(), [expected]);
        assert_exact_capacity(normalized);
    }

    fn assert_exact_capacity(data: Bytes) {
        let data: Vec<u8> = data.into();
        assert_eq!(data.capacity(), data.len());
    }
}
