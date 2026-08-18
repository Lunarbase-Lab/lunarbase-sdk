//! Generation-checked coherent state publication.

use super::SharedQuoteState;
use crate::indexer::engine::QuoteIndexer;
use crate::indexer::errors::IndexerError;
#[cfg(feature = "perf-trace")]
use crate::indexer::perf_trace::PerfTracePublicationTiming;
use arc_swap::Guard;
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl SharedQuoteState {
    /// Loads one immutable quote snapshot without contending with publishers.
    pub(in crate::indexer) fn load_indexer(
        &self,
    ) -> Result<Guard<Arc<QuoteIndexer>>, IndexerError> {
        if self.publication_writer.is_poisoned() {
            return Err(IndexerError::LockPoisoned);
        }
        let published = self.indexer.load();
        if self.publication_writer.is_poisoned() {
            return Err(IndexerError::LockPoisoned);
        }
        Ok(published)
    }

    /// Clones a private candidate paired with its exact publication generation.
    pub(in crate::indexer) fn indexer_candidate(
        &self,
    ) -> Result<(u64, QuoteIndexer), IndexerError> {
        let writer = self
            .publication_writer
            .lock()
            .map_err(|_| IndexerError::LockPoisoned)?;
        let generation = self.publication_generation.load(Ordering::Relaxed);
        let published = self.indexer.load_full();
        drop(writer);
        let candidate = published.as_ref().clone();
        Ok((generation, candidate))
    }

    /// Unconditionally publishes one coherent immutable candidate.
    pub(in crate::indexer) fn publish_indexer(
        &self,
        candidate: QuoteIndexer,
    ) -> Result<Arc<QuoteIndexer>, IndexerError> {
        let candidate = Arc::new(candidate);
        let writer = self
            .publication_writer
            .lock()
            .map_err(|_| IndexerError::LockPoisoned)?;
        let publication = self.availability.begin_publication();
        let retired = self.indexer.swap(candidate);
        self.publication_generation.fetch_add(1, Ordering::Release);
        drop(writer);
        if let Some(publication) = publication {
            self.availability.complete_publication(publication);
        }
        Ok(retired)
    }

    /// Applies a rare control transition to a private clone and publishes it.
    pub(in crate::indexer) fn mutate_indexer<T>(
        &self,
        mutate: impl FnOnce(&mut QuoteIndexer) -> T,
    ) -> Result<T, IndexerError> {
        let writer = self
            .publication_writer
            .lock()
            .map_err(|_| IndexerError::LockPoisoned)?;
        let mut candidate = self.indexer.load().as_ref().clone();
        let result = mutate(&mut candidate);
        let publication = self.availability.begin_publication();
        let retired = self.indexer.swap(Arc::new(candidate));
        self.publication_generation.fetch_add(1, Ordering::Release);
        drop(writer);
        if let Some(publication) = publication {
            self.availability.complete_publication(publication);
        }
        drop(retired);
        Ok(result)
    }

    /// Installs a candidate only if no other writer changed its captured base.
    ///
    /// The retired snapshot is returned so allocator/destructor work can happen
    /// after the writer gate has been released.
    pub(in crate::indexer) fn publish_indexer_if_generation(
        &self,
        expected_generation: u64,
        candidate: QuoteIndexer,
    ) -> Result<Option<Arc<QuoteIndexer>>, IndexerError> {
        #[cfg(feature = "perf-trace")]
        let published =
            self.publish_indexer_if_generation_inner(expected_generation, candidate, None);
        #[cfg(not(feature = "perf-trace"))]
        let published = self.publish_indexer_if_generation_inner(expected_generation, candidate);
        published
    }

    #[cfg(feature = "perf-trace")]
    pub(in crate::indexer) fn publish_indexer_if_generation_traced(
        &self,
        expected_generation: u64,
        candidate: QuoteIndexer,
    ) -> Result<(Option<Arc<QuoteIndexer>>, PerfTracePublicationTiming), IndexerError> {
        let mut timing = PerfTracePublicationTiming::default();
        let published = self.publish_indexer_if_generation_inner(
            expected_generation,
            candidate,
            Some(&mut timing),
        )?;
        Ok((published, timing))
    }

    fn publish_indexer_if_generation_inner(
        &self,
        expected_generation: u64,
        candidate: QuoteIndexer,
        #[cfg(feature = "perf-trace")] mut timing: Option<&mut PerfTracePublicationTiming>,
    ) -> Result<Option<Arc<QuoteIndexer>>, IndexerError> {
        let candidate = Arc::new(candidate);
        let writer = self
            .publication_writer
            .lock()
            .map_err(|_| IndexerError::LockPoisoned)?;
        #[cfg(feature = "perf-trace")]
        if let Some(timing) = timing.as_deref_mut() {
            timing.writer_gate_acquired_at = Some(std::time::Instant::now());
        }
        if self.publication_generation.load(Ordering::Relaxed) != expected_generation {
            drop(writer);
            #[cfg(feature = "perf-trace")]
            if let Some(timing) = timing.as_deref_mut() {
                timing.writer_gate_released_at = Some(std::time::Instant::now());
            }
            drop(candidate);
            return Ok(None);
        }
        #[cfg(feature = "perf-trace")]
        if let Some(timing) = timing.as_deref_mut() {
            timing.pre_store_at = Some(std::time::Instant::now());
        }
        let publication = self.availability.begin_publication();
        let retired = self.indexer.swap(candidate);
        #[cfg(feature = "perf-trace")]
        if let Some(timing) = timing.as_deref_mut() {
            timing.store_returned_at = Some(std::time::Instant::now());
        }
        self.publication_generation.fetch_add(1, Ordering::Release);
        drop(writer);
        if let Some(publication) = publication {
            self.availability.complete_publication(publication);
        }
        #[cfg(feature = "perf-trace")]
        if let Some(timing) = timing {
            timing.writer_gate_released_at = Some(std::time::Instant::now());
        }
        Ok(Some(retired))
    }
}
