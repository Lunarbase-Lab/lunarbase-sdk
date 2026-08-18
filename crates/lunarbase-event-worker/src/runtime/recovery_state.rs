//! Sticky recovery target and coverage state kept across retry loops.

use super::recovery_coverage;
use lunarbase_client::model::{BlockRef, ChainCursor};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Transition {
    Continue,
    Recover(Option<BlockRef>),
    RecoverRequired(ChainCursor),
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RecoveryAction {
    Live,
    Retry,
    Shutdown,
}

#[derive(Default)]
pub(super) struct RecoveryState {
    target: Option<BlockRef>,
    required: Option<ChainCursor>,
    conflict: Option<String>,
}

impl RecoveryState {
    pub(super) fn target(&self) -> Option<&BlockRef> {
        self.target.as_ref()
    }

    pub(super) fn take_target(&mut self) -> Option<BlockRef> {
        self.target.take()
    }

    pub(super) fn restore_target(&mut self, target: Option<BlockRef>) {
        self.target = target;
    }

    pub(super) fn clear_target(&mut self) {
        self.target = None;
    }

    pub(super) fn required(&self) -> Option<&ChainCursor> {
        self.required.as_ref()
    }

    pub(super) fn conflict(&self) -> Option<&str> {
        self.conflict.as_deref()
    }

    pub(super) fn apply(&mut self, transition: Transition, has_forks: bool) -> RecoveryAction {
        match transition {
            Transition::Continue => {
                self.required = None;
                self.conflict = None;
                RecoveryAction::Live
            }
            Transition::Shutdown => RecoveryAction::Shutdown,
            Transition::Recover(target) => {
                if has_forks {
                    self.target = target;
                } else {
                    self.target = None;
                    if let Some(target) = target {
                        self.merge_required(target.cursor);
                    }
                }
                RecoveryAction::Retry
            }
            Transition::RecoverRequired(required) => {
                self.target = None;
                self.merge_required(required);
                RecoveryAction::Retry
            }
        }
    }

    fn merge_required(&mut self, candidate: ChainCursor) {
        if let Err(error) = recovery_coverage::merge(&mut self.required, candidate) {
            self.conflict.get_or_insert_with(|| error.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use lunarbase_client::model::{BlockRef, Commitment};

    fn cursor(block: u64, hash: u8, commitment: Commitment) -> ChainCursor {
        ChainCursor::block(8453, block, Some(B256::new([hash; 32])), commitment)
    }

    #[test]
    fn no_fork_target_becomes_a_sticky_coverage_watermark() {
        let mut state = RecoveryState::default();
        let target = BlockRef::new(cursor(105, 1, Commitment::Realtime), None);
        assert_eq!(
            state.apply(Transition::Recover(Some(target)), false),
            RecoveryAction::Retry
        );
        assert_eq!(state.target(), None);
        assert_eq!(state.required().unwrap().block_number, 105);
    }

    #[test]
    fn conflicting_finalized_requirements_do_not_exit_or_replace_the_first() {
        let mut state = RecoveryState::default();
        state.apply(
            Transition::RecoverRequired(cursor(105, 1, Commitment::Finalized)),
            false,
        );
        state.apply(
            Transition::RecoverRequired(cursor(105, 2, Commitment::Canonical)),
            false,
        );
        assert!(state.conflict().is_some());
        assert_eq!(
            state.required().unwrap().block_hash,
            Some(B256::new([1; 32]))
        );
    }
}
