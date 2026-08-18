//! Bounded, ownership-preserving bootstrap-to-reducer handoff.

use crate::indexer::client_types::{
    ClientRuntimeStats, CoreEventSink, QueuedChainUpdate, SharedQuoteState,
};
use crate::indexer::engine::{QuoteIndexer, sort_chain_update_refs_with_indices};
use crate::indexer::errors::IndexerError;
use crate::indexer::event_delivery::{same_core_event_identity, try_observe_core_event};
use crate::model::{ChainUpdate, SourceError};
use tokio::sync::mpsc;

pub(super) struct BootstrapHandoff {
    queued: Vec<QueuedChainUpdate>,
    observer_order: Vec<usize>,
}

impl BootstrapHandoff {
    /// Captures one fixed queue prefix. Concurrent refills remain queued for
    /// the reducer and cannot extend this non-awaiting bootstrap operation.
    pub(super) fn capture(
        receiver: &mut mpsc::Receiver<QueuedChainUpdate>,
        queue_capacity: usize,
    ) -> Self {
        let captured = captured_prefix_len(receiver, queue_capacity);
        Self {
            queued: take_prefix(receiver, captured),
            observer_order: Vec::new(),
        }
    }

    /// Applies borrowed payloads while their accounting and byte permits stay
    /// owned by this handoff. Observer indices contain no payload copies.
    pub(super) fn apply(
        mut self,
        indexer: &mut QuoteIndexer,
        event_sink: Option<&CoreEventSink>,
        skip_canonical_covered: bool,
    ) -> Result<Self, IndexerError> {
        let mut apply_order = self
            .queued
            .iter()
            .enumerate()
            .map(|(index, queued)| (index, queued.update()))
            .collect::<Vec<_>>();
        sort_chain_update_refs_with_indices(&mut apply_order);
        indexer.apply_handoff_borrowed_ordered(apply_order.iter().map(|(_, update)| *update))?;
        drop(apply_order);

        let Some(event_sink) = event_sink else {
            return Ok(self);
        };
        let mut observer_order = self
            .queued
            .iter()
            .enumerate()
            .filter_map(|(index, queued)| match queued.update() {
                ChainUpdate::Log(log) if event_sink.accepts(log.cursor.commitment) => Some(index),
                _ => None,
            })
            .collect::<Vec<_>>();
        observer_order.sort_by_key(|index| {
            let ChainUpdate::Log(log) = self.queued[*index].update() else {
                unreachable!("observer order contains only logs")
            };
            log.cursor.event_order()
        });
        observer_order.dedup_by(|right, left| {
            let ChainUpdate::Log(left) = self.queued[*left].update() else {
                unreachable!("observer order contains only logs")
            };
            let ChainUpdate::Log(right) = self.queued[*right].update() else {
                unreachable!("observer order contains only logs")
            };
            same_core_event_identity(left, right)
        });
        if skip_canonical_covered {
            let mut uncovered = Vec::with_capacity(observer_order.len());
            for index in observer_order {
                let ChainUpdate::Log(log) = self.queued[index].update() else {
                    unreachable!("observer order contains only logs")
                };
                if !indexer.canonical_floor_covers_core_log(log)? {
                    uncovered.push(index);
                }
            }
            self.observer_order = uncovered;
        } else {
            self.observer_order = observer_order;
        }
        Ok(self)
    }

    /// Releases queued ownership only after the verified candidate is both
    /// installed and published Ready, moving observer logs without cloning.
    pub(super) fn publish_events(
        self,
        event_sink: Option<&CoreEventSink>,
        stats: &ClientRuntimeStats,
    ) {
        let mut queued = self.queued.into_iter().map(Some).collect::<Vec<_>>();
        for index in self.observer_order {
            let update = queued[index]
                .take()
                .expect("each observer update is published at most once")
                .dequeue();
            let ChainUpdate::Log(log) = update else {
                unreachable!("observer order contains only logs")
            };
            try_observe_core_event(event_sink, log, stats);
        }
    }

    /// Installs and admits only the source generation that began bootstrap.
    /// A reconnect cannot lend its newer activity to an older snapshot.
    pub(super) fn install_and_publish(
        self,
        shared: &SharedQuoteState,
        indexer: QuoteIndexer,
        source_lease: u64,
        event_sink: Option<&CoreEventSink>,
        stats: &ClientRuntimeStats,
    ) -> Result<(), IndexerError> {
        let retired = shared.publish_indexer(indexer)?;
        drop(retired);
        stats.record_state_update();
        if !shared.publish_available_if(source_lease) {
            return Err(SourceError::Unavailable(
                "source generation changed while bootstrap state was being installed".into(),
            )
            .into());
        }
        self.publish_events(event_sink, stats);
        Ok(())
    }
}

fn take_prefix(
    receiver: &mut mpsc::Receiver<QueuedChainUpdate>,
    captured: usize,
) -> Vec<QueuedChainUpdate> {
    let mut queued = Vec::with_capacity(captured);
    for _ in 0..captured {
        let Ok(update) = receiver.try_recv() else {
            break;
        };
        queued.push(update);
    }
    queued
}

fn captured_prefix_len(
    receiver: &mpsc::Receiver<QueuedChainUpdate>,
    queue_capacity: usize,
) -> usize {
    receiver.len().min(queue_capacity)
}

#[cfg(test)]
#[path = "bootstrap_handoff_tests.rs"]
mod tests;
