use crate::{ChainCursor, ChainUpdate, DeploymentConfig, SourceError};
use async_trait::async_trait;
use lunarbase_math::{Address, QuoteState};
use std::collections::VecDeque;
/// A block-tagged, fully materialized quote snapshot returned by a chain
/// backend. The code hash is checked before it can become ready state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapSnapshot {
    pub state: QuoteState,
    pub cursor: ChainCursor,
    pub runtime_code_hash: [u8; 32],
}

#[async_trait]
pub trait SnapshotProvider: Send + Sync {
    async fn snapshot(
        &self,
        config: &DeploymentConfig,
        lane_assets: &[Address],
        routers: &[Address],
    ) -> Result<BootstrapSnapshot, SourceError>;
}

/// Bounded handoff queue used while a block-tagged snapshot is being fetched.
/// Once capacity is exceeded it stays poisoned until the caller resnapshots;
/// silently dropping an update would invalidate quote freshness.
#[derive(Clone, Debug)]
pub struct BufferedUpdateQueue {
    capacity: usize,
    updates: VecDeque<ChainUpdate>,
    poisoned: bool,
}

impl BufferedUpdateQueue {
    pub fn new(capacity: usize) -> Result<Self, SourceError> {
        if capacity == 0 {
            return Err(SourceError::Unavailable(
                "buffer capacity must be non-zero".into(),
            ));
        }
        Ok(Self {
            capacity,
            updates: VecDeque::with_capacity(capacity.min(1024)),
            poisoned: false,
        })
    }

    pub fn push(&mut self, update: ChainUpdate) -> Result<(), SourceError> {
        if self.poisoned || self.updates.len() >= self.capacity {
            self.poisoned = true;
            return Err(SourceError::Gap(
                "snapshot handoff buffer overflow; resnapshot required".into(),
            ));
        }
        self.updates.push_back(update);
        Ok(())
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn drain(&mut self) -> Result<Vec<ChainUpdate>, SourceError> {
        if self.poisoned {
            return Err(SourceError::Gap(
                "snapshot handoff buffer is poisoned".into(),
            ));
        }
        Ok(self.updates.drain(..).collect())
    }
}
