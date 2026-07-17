//! Core bounded cursor ordering for realtime source updates.

use crate::{ChainCursor, ChainUpdate, SourceError};
use std::collections::BTreeMap;

type CursorKey = (u64, u32, u32, u8);

/// Bounded single-writer reorder buffer for transport/decode work.
///
/// Network adapters may decode messages concurrently, but the reducer must
/// see one deterministic order. The buffer never evicts silently: callers
/// receive a gap and must recover from a canonical source when its bound is
/// exceeded or conflicting updates share one event cursor.
#[derive(Clone, Debug)]
pub struct CursorReorderBuffer {
    capacity: usize,
    pending: BTreeMap<CursorKey, ChainUpdate>,
    poisoned: bool,
}

impl CursorReorderBuffer {
    /// Creates a bounded deterministic reorder buffer.
    pub fn new(capacity: usize) -> Result<Self, SourceError> {
        if capacity == 0 {
            return Err(SourceError::Unavailable(
                "reorder buffer capacity must be non-zero".into(),
            ));
        }
        Ok(Self {
            capacity,
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

    /// Returns whether the buffer has entered fail-closed state.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Returns `true` when the update was inserted and `false` for an exact
    /// duplicate. A different payload at the same event cursor is a
    /// continuity failure, not something that can be resolved by ordering.
    /// Inserts an update, deduplicating an identical cursor/payload.
    ///
    /// # Errors
    ///
    /// Returns a gap for overflow, conflicting payloads at one cursor, or any
    /// insertion after the buffer has already been poisoned.
    pub fn push(&mut self, update: ChainUpdate) -> Result<bool, SourceError> {
        if self.poisoned {
            return Err(SourceError::Gap(
                "reorder buffer is poisoned; resnapshot required".into(),
            ));
        }
        let key = update_key(&update);
        if let Some(existing) = self.pending.get(&key) {
            if existing == &update {
                return Ok(false);
            }
            self.poisoned = true;
            return Err(SourceError::Gap(
                "conflicting updates share one cursor".into(),
            ));
        }
        if self.pending.len() >= self.capacity {
            self.poisoned = true;
            return Err(SourceError::Gap(
                "reorder buffer overflow; resnapshot required".into(),
            ));
        }
        self.pending.insert(key, update);
        Ok(true)
    }

    /// Drain updates through a watermark. A head without transaction/log
    /// coordinates is treated as the end of its block, which is the safe
    /// boundary for block-level head notifications.
    /// Releases all updates at or before a block/log watermark in cursor order.
    pub fn drain_through(&mut self, watermark: &ChainCursor) -> Vec<ChainUpdate> {
        let key = watermark_key(watermark);
        let keys: Vec<_> = self.pending.range(..=key).map(|(key, _)| *key).collect();
        keys.into_iter()
            .filter_map(|key| self.pending.remove(&key))
            .collect()
    }

    /// Releases every buffered update in deterministic key order.
    pub fn drain_all(&mut self) -> Vec<ChainUpdate> {
        std::mem::take(&mut self.pending).into_values().collect()
    }
}

fn update_key(update: &ChainUpdate) -> CursorKey {
    match update {
        ChainUpdate::Head(cursor) => cursor_key(cursor, 0),
        ChainUpdate::Log(log) => cursor_key(&log.cursor, 1),
        ChainUpdate::Reorg { new_head, .. } => cursor_key(new_head, 2),
        ChainUpdate::Gap { cursor, .. } => cursor
            .as_ref()
            .map_or((u64::MAX, 0, 0, 3), |cursor| cursor_key(cursor, 3)),
        ChainUpdate::SourceHealth { .. } => (0, 0, 0, 4),
    }
}

fn watermark_key(cursor: &ChainCursor) -> CursorKey {
    let (block, transaction, log) = cursor.event_order();
    if cursor.transaction_index.is_none() && cursor.log_index.is_none() {
        (block, u32::MAX, u32::MAX, u8::MAX)
    } else {
        (block, transaction, log, u8::MAX)
    }
}

fn cursor_key(cursor: &ChainCursor, rank: u8) -> CursorKey {
    let (block, transaction, log) = cursor.event_order();
    (block, transaction, log, rank)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChainCursor, Commitment};

    fn cursor(block: u64, tx: Option<u32>, log: Option<u32>) -> ChainCursor {
        ChainCursor {
            chain_id: 143,
            block_number: block,
            block_hash: None,
            transaction_index: tx,
            log_index: log,
            source_sequence: None,
            source_sub_index: None,
            commitment: Commitment::Realtime,
        }
    }

    #[test]
    fn reorders_and_deduplicates_logs_without_eviction() {
        let mut buffer = CursorReorderBuffer::new(3).unwrap();
        let later = ChainUpdate::Head(cursor(11, None, None));
        let earlier = ChainUpdate::Head(cursor(10, None, None));
        assert!(buffer.push(later.clone()).unwrap());
        assert!(buffer.push(earlier.clone()).unwrap());
        assert!(!buffer.push(earlier).unwrap());
        assert_eq!(
            buffer.drain_all(),
            vec![ChainUpdate::Head(cursor(10, None, None)), later,]
        );
    }

    #[test]
    fn block_head_watermark_drains_all_events_in_that_block() {
        let mut buffer = CursorReorderBuffer::new(4).unwrap();
        let log = ChainUpdate::Log(crate::ContractLog {
            address: lunarbase_math::Address::ZERO,
            topics: vec![],
            data: vec![],
            removed: false,
            cursor: cursor(10, Some(3), Some(7)),
        });
        buffer.push(log.clone()).unwrap();
        buffer
            .push(ChainUpdate::Head(cursor(10, None, None)))
            .unwrap();
        assert_eq!(
            buffer.drain_through(&cursor(10, None, None)),
            vec![ChainUpdate::Head(cursor(10, None, None)), log]
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn overflow_poison_is_sticky() {
        let mut buffer = CursorReorderBuffer::new(1).unwrap();
        buffer
            .push(ChainUpdate::Head(cursor(1, None, None)))
            .unwrap();
        assert!(buffer
            .push(ChainUpdate::Head(cursor(2, None, None)))
            .is_err());
        assert!(buffer.is_poisoned());
        assert!(buffer
            .push(ChainUpdate::Head(cursor(3, None, None)))
            .is_err());
    }
}
