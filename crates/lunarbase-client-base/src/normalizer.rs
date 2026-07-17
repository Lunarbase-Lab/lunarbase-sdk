//! Base Flashblocks payload normalization.

use lunarbase_client_core::{ChainCursor, ChainUpdate, Commitment, ContractLog, SourceError};
use lunarbase_math::{Address, U256};

/// Tracks one provisional Flashblocks payload and its monotonic index.
#[derive(Clone, Debug, Default)]
pub struct BaseFlashblocksTracker {
    payload_id: Option<[u8; 32]>,
    block_number: Option<u64>,
    latest_index: Option<u64>,
}

impl BaseFlashblocksTracker {
    /// Records a payload/index and reports whether it starts a new payload.
    pub fn observe(
        &mut self,
        payload_id: [u8; 32],
        block_number: u64,
        index: u64,
    ) -> Result<bool, SourceError> {
        if self.payload_id == Some(payload_id) {
            if self.block_number != Some(block_number) {
                return Err(SourceError::Gap("payload changed block context".into()));
            }
            if self.latest_index.is_some_and(|latest| index < latest) {
                return Err(SourceError::Gap("Flashblocks index regression".into()));
            }
            if self.latest_index == Some(index) {
                return Ok(false);
            }
            self.latest_index = Some(index);
            return Ok(false);
        }
        self.payload_id = Some(payload_id);
        self.block_number = Some(block_number);
        self.latest_index = Some(index);
        Ok(true)
    }

    /// Clears payload history after canonical recovery.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Provider-independent Flashblocks header fields used by the normalizer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlashblockHeader {
    pub payload_id: [u8; 32],
    pub block_number: u64,
    pub block_hash: Option<[u8; 32]>,
    pub index: u64,
}

/// Provider-independent pending log attached to a Flashblocks header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlashblockLog {
    pub header: FlashblockHeader,
    pub transaction_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<U256>,
    pub data: Vec<u8>,
    pub removed: bool,
}

/// Converts Flashblocks payloads into common runtime updates.
#[derive(Clone, Debug, Default)]
pub struct BaseFlashblocksNormalizer {
    tracker: BaseFlashblocksTracker,
    chain_id: u64,
}

impl BaseFlashblocksNormalizer {
    /// Creates a normalizer for one configured Base chain id.
    pub fn new(chain_id: u64) -> Self {
        Self {
            tracker: BaseFlashblocksTracker::default(),
            chain_id,
        }
    }

    /// Converts a Flashblocks header into a realtime block-head update.
    pub fn normalize_header(
        &mut self,
        header: FlashblockHeader,
    ) -> Result<Option<ChainUpdate>, SourceError> {
        if !self
            .tracker
            .observe(header.payload_id, header.block_number, header.index)?
        {
            return Ok(None);
        }
        Ok(Some(ChainUpdate::Head(ChainCursor {
            chain_id: self.chain_id,
            block_number: header.block_number,
            block_hash: header.block_hash,
            transaction_index: None,
            log_index: None,
            source_sequence: Some(header.index),
            source_sub_index: None,
            commitment: Commitment::Realtime,
        })))
    }

    /// Converts one pending log into an ordered head/log update sequence.
    pub fn normalize_log(&mut self, log: FlashblockLog) -> Result<Vec<ChainUpdate>, SourceError> {
        let header_update = self.normalize_header(log.header.clone())?;
        let mut updates = Vec::with_capacity(2);
        if let Some(update) = header_update {
            updates.push(update);
        }
        updates.push(ChainUpdate::Log(ContractLog {
            address: log.address,
            topics: log.topics,
            data: log.data,
            removed: log.removed,
            cursor: ChainCursor {
                chain_id: self.chain_id,
                block_number: log.header.block_number,
                block_hash: log.header.block_hash,
                transaction_index: Some(log.transaction_index),
                log_index: Some(log.log_index),
                source_sequence: Some(log.header.index),
                source_sub_index: Some(log.log_index),
                commitment: Commitment::Realtime,
            },
        }));
        Ok(updates)
    }

    /// Clears provisional payload tracking after recovery.
    pub fn reset(&mut self) {
        self.tracker.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(value: u8) -> Address {
        let mut bytes = [0; 20];
        bytes[19] = value;
        Address(bytes)
    }

    #[test]
    fn multiple_logs_at_one_flashblock_index_remain_valid() {
        let mut normalizer = BaseFlashblocksNormalizer::new(8453);
        let header = FlashblockHeader {
            payload_id: [1; 32],
            block_number: 42,
            block_hash: Some([2; 32]),
            index: 0,
        };
        let log = |address| FlashblockLog {
            header: header.clone(),
            transaction_index: 0,
            log_index: 0,
            address,
            topics: Vec::new(),
            data: Vec::new(),
            removed: false,
        };
        assert_eq!(normalizer.normalize_log(log(address(1))).unwrap().len(), 2);
        assert_eq!(normalizer.normalize_log(log(address(2))).unwrap().len(), 1);
    }
}
