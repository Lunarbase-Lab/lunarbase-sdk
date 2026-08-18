//! Core bounded cursor ordering for realtime source updates.

use crate::model::{ChainCursor, ChainUpdate, SourceError};
use std::collections::BTreeMap;

type CursorKey = (u64, u32, u32, u64, u32, u8);

const DEFAULT_BYTES_PER_UPDATE: usize = 64 * 1024;

/// Bounded single-writer reorder buffer for transport/decode work.
///
/// Network sources may decode messages concurrently, but the reducer must
/// see one deterministic order. The buffer never evicts silently: callers
/// receive a gap and must recover from a canonical source when its bound is
/// exceeded or conflicting updates share one event cursor.
#[derive(Clone, Debug)]
pub struct CursorReorderBuffer {
    /// Maximum number of updates retained before continuity fails closed.
    capacity: usize,
    /// Maximum retained memory charged across all pending updates.
    byte_capacity: usize,
    /// Current conservative retained-memory charge.
    pending_bytes: usize,
    /// Updates indexed by normalized deterministic source position.
    pending: BTreeMap<CursorKey, ChainUpdate>,
    /// Sticky continuity-failure flag cleared only by constructing a new buffer.
    poisoned: bool,
}

impl CursorReorderBuffer {
    /// Creates a bounded deterministic reorder buffer.
    pub fn new(capacity: usize) -> Result<Self, SourceError> {
        let byte_capacity = capacity.saturating_mul(DEFAULT_BYTES_PER_UPDATE);
        Self::with_limits(capacity, byte_capacity)
    }

    /// Creates a deterministic reorder buffer bounded by count and bytes.
    pub fn with_limits(capacity: usize, byte_capacity: usize) -> Result<Self, SourceError> {
        if capacity == 0 || byte_capacity == 0 {
            return Err(SourceError::Unavailable(
                "reorder buffer count and byte capacities must be non-zero".into(),
            ));
        }
        Ok(Self {
            capacity,
            byte_capacity,
            pending_bytes: 0,
            pending: BTreeMap::new(),
            poisoned: false,
        })
    }

    /// Returns the number of updates waiting for a watermark.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Returns whether no update is currently buffered.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Returns the conservative retained-memory charge of pending updates.
    pub fn retained_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Returns whether the buffer has entered fail-closed state.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Inserts one update. Any repeated cursor requires canonical recovery.
    ///
    /// # Errors
    ///
    /// Returns a gap for overflow, conflicting payloads at one cursor, or any
    /// insertion after the buffer has already been poisoned.
    pub fn push(&mut self, mut update: ChainUpdate) -> Result<(), SourceError> {
        if self.poisoned {
            return Err(SourceError::Gap(
                "reorder buffer is poisoned; resnapshot required".into(),
            ));
        }
        let key = update_key(&update);
        if self.pending.contains_key(&key) {
            self.poisoned = true;
            return Err(SourceError::Gap("multiple updates share one cursor".into()));
        }
        let update_bytes = update.retained_bytes();
        if self.pending.len() >= self.capacity
            || update_bytes > self.byte_capacity.saturating_sub(self.pending_bytes)
        {
            self.poisoned = true;
            return Err(SourceError::Gap(
                "reorder buffer count or byte budget exceeded; resnapshot required".into(),
            ));
        }
        update.normalize_for_retention();
        debug_assert_eq!(update_bytes, update.retained_bytes());
        self.pending_bytes += update_bytes;
        self.pending.insert(key, update);
        Ok(())
    }

    /// Drain updates through a watermark. A head without transaction/log
    /// coordinates is treated as the end of its block, which is the safe
    /// boundary for block-level head notifications.
    /// Releases all updates at or before a block/log watermark in cursor order.
    pub fn drain_through(&mut self, watermark: &ChainCursor) -> Vec<ChainUpdate> {
        let key = watermark_key(watermark);
        let keys: Vec<_> = self.pending.range(..=key).map(|(key, _)| *key).collect();
        keys.into_iter()
            .filter_map(|key| self.remove(&key))
            .collect()
    }

    /// Releases every buffered update in deterministic key order.
    pub fn drain_all(&mut self) -> Vec<ChainUpdate> {
        self.pending_bytes = 0;
        std::mem::take(&mut self.pending).into_values().collect()
    }

    fn remove(&mut self, key: &CursorKey) -> Option<ChainUpdate> {
        let update = self.pending.remove(key)?;
        self.pending_bytes = self.pending_bytes.saturating_sub(update.retained_bytes());
        Some(update)
    }
}

fn update_key(update: &ChainUpdate) -> CursorKey {
    match update {
        ChainUpdate::Head(head) => cursor_key(&head.cursor, 0),
        ChainUpdate::Log(log) => cursor_key(&log.cursor, 1),
        ChainUpdate::Correction(correction) => branch_end_key(&correction.new_tip.cursor, 2),
        ChainUpdate::Reorg { new_head, .. } => branch_end_key(&new_head.cursor, 3),
        ChainUpdate::Gap { cursor, .. } => cursor
            .as_ref()
            .map_or((u64::MAX, 0, 0, 0, 0, 4), |cursor| cursor_key(cursor, 4)),
    }
}

fn watermark_key(cursor: &ChainCursor) -> CursorKey {
    let (block, transaction, log, source_sequence, source_sub_index) = cursor.event_order();
    if cursor.transaction_index.is_none() && cursor.log_index.is_none() {
        (block, u32::MAX, u32::MAX, u64::MAX, u32::MAX, u8::MAX)
    } else {
        (
            block,
            transaction,
            log,
            source_sequence,
            source_sub_index,
            u8::MAX,
        )
    }
}

fn branch_end_key(cursor: &ChainCursor, rank: u8) -> CursorKey {
    (
        cursor.block_number,
        u32::MAX,
        u32::MAX,
        u64::MAX,
        u32::MAX,
        rank,
    )
}

fn cursor_key(cursor: &ChainCursor, rank: u8) -> CursorKey {
    let (block, transaction, log, source_sequence, source_sub_index) = cursor.event_order();
    (
        block,
        transaction,
        log,
        source_sequence,
        source_sub_index,
        rank,
    )
}

#[cfg(test)]
mod tests {
    use crate::model::{BlockRef, ChainCursor, ChainUpdate, Commitment, ContractLog};
    use crate::state::ordering::CursorReorderBuffer;
    use lunarbase_math::{Address, Bytes};

    fn cursor(block: u64, tx: Option<u32>, log: Option<u32>) -> ChainCursor {
        ChainCursor {
            chain_id: 143,
            block_number: block,
            execution_block_number: block,
            block_hash: None,
            transaction_index: tx,
            log_index: log,
            source_sequence: None,
            source_sub_index: None,
            commitment: Commitment::Realtime,
        }
    }

    fn head(cursor: ChainCursor) -> ChainUpdate {
        ChainUpdate::Head(BlockRef::new(cursor, None))
    }

    #[test]
    fn reorders_unique_updates_without_eviction() {
        let mut buffer = CursorReorderBuffer::new(3).unwrap();
        let later = head(cursor(11, None, None));
        let earlier = head(cursor(10, None, None));
        buffer.push(later.clone()).unwrap();
        buffer.push(earlier.clone()).unwrap();
        assert_eq!(
            buffer.drain_all(),
            vec![head(cursor(10, None, None)), later,]
        );
    }

    #[test]
    fn repeated_cursor_requires_canonical_recovery() {
        let mut buffer = CursorReorderBuffer::new(3).unwrap();
        let update = head(cursor(10, None, None));
        buffer.push(update.clone()).unwrap();
        assert!(buffer.push(update).is_err());
        assert!(buffer.is_poisoned());
    }

    #[test]
    fn block_head_watermark_drains_all_events_in_that_block() {
        let mut buffer = CursorReorderBuffer::new(4).unwrap();
        let log = ChainUpdate::Log(ContractLog {
            address: Address::ZERO,
            transaction_hash: None,
            topics: vec![],
            data: Bytes::new(),
            removed: false,
            cursor: cursor(10, Some(3), Some(7)),
        });
        buffer.push(log.clone()).unwrap();
        buffer.push(head(cursor(10, None, None))).unwrap();
        assert_eq!(
            buffer.drain_through(&cursor(10, None, None)),
            vec![head(cursor(10, None, None)), log]
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn overflow_poison_is_sticky() {
        let mut buffer = CursorReorderBuffer::new(1).unwrap();
        buffer.push(head(cursor(1, None, None))).unwrap();
        assert!(buffer.push(head(cursor(2, None, None))).is_err());
        assert!(buffer.is_poisoned());
        assert!(buffer.push(head(cursor(3, None, None))).is_err());
    }

    #[test]
    fn byte_budget_is_released_on_drain_and_overflow_fails_closed() {
        let first = head(cursor(1, None, None));
        let bytes = first.retained_bytes();
        let mut buffer = CursorReorderBuffer::with_limits(10, bytes).unwrap();
        buffer.push(first.clone()).unwrap();
        assert_eq!(buffer.retained_bytes(), bytes);
        assert_eq!(buffer.drain_all(), vec![first]);
        assert_eq!(buffer.retained_bytes(), 0);

        buffer.push(head(cursor(2, None, None))).unwrap();
        assert!(buffer.push(head(cursor(3, None, None))).is_err());
        assert!(buffer.is_poisoned());
    }

    #[test]
    fn byte_budget_retains_only_the_visible_tail_slice() {
        let backing = Bytes::from(vec![0x5a; 1 << 20]);
        let data = backing.slice(backing.len() - 1..);
        drop(backing);
        let update = ChainUpdate::Log(ContractLog {
            address: Address::ZERO,
            transaction_hash: None,
            topics: Vec::new(),
            data,
            removed: false,
            cursor: cursor(10, Some(0), Some(0)),
        });
        let logical_bytes = update.retained_bytes();
        let mut buffer = CursorReorderBuffer::with_limits(1, logical_bytes).unwrap();

        buffer.push(update).unwrap();
        assert_eq!(buffer.retained_bytes(), logical_bytes);
        let ChainUpdate::Log(log) = buffer.drain_all().pop().unwrap() else {
            panic!("queued update must remain a log");
        };
        assert_eq!(log.data.as_ref(), [0x5a]);
        let data: Vec<u8> = log.data.into();
        assert_eq!(data.capacity(), data.len());
    }
}
