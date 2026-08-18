//! Versioned lock-free quote-admission state machine.

use std::sync::atomic::{AtomicU64, Ordering};

const STATE_MASK: u64 = 0b111;
const INVALID: u64 = 0;
const READY: u64 = 1;
const PUBLISHING: u64 = 2;
const EXPIRING: u64 = 3;
const STOPPING: u64 = 4;

#[derive(Debug)]
pub(super) struct Availability(AtomicU64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum QuoteAdmission {
    Unavailable,
    Ready,
    Publishing,
}

impl Availability {
    pub(super) const fn new() -> Self {
        Self(AtomicU64::new(INVALID))
    }

    /// Publishes only if no source disconnect advanced the captured lease.
    pub(super) fn publish_if(&self, expected: u64) -> bool {
        if state(expected) == STOPPING {
            return false;
        }
        // Normal reducer progress keeps the read-mostly readiness cacheline
        // stable. A disconnect after this linearization point revokes it.
        // Ordinary progress may validate the source while a queued correction
        // owns publication priority. It must not complete that correction's
        // exact lease.
        if matches!(state(expected), READY | PUBLISHING) {
            return self.0.load(Ordering::Acquire) == expected;
        }
        self.0
            .compare_exchange(
                expected,
                next(expected, READY),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(super) fn is_available(&self) -> bool {
        matches!(
            state(self.0.load(Ordering::Acquire)),
            READY | PUBLISHING | EXPIRING
        )
    }

    /// Samples quote admission and publication progress from one cacheline read.
    pub(super) fn quote_admission(&self) -> QuoteAdmission {
        match state(self.0.load(Ordering::Acquire)) {
            READY | EXPIRING => QuoteAdmission::Ready,
            PUBLISHING => QuoteAdmission::Publishing,
            _ => QuoteAdmission::Unavailable,
        }
    }

    pub(super) fn token(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    pub(super) const fn token_is_correcting(token: u64) -> bool {
        state(token) == PUBLISHING
    }

    pub(super) fn begin_correction(&self) -> Option<u64> {
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            if !matches!(state(current), READY | EXPIRING) {
                return None;
            }
            let correcting = next(current, PUBLISHING);
            match self.0.compare_exchange_weak(
                current,
                correcting,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(correcting),
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn complete_correction(&self, correcting: u64) -> bool {
        self.0
            .compare_exchange(
                correcting,
                next(correcting, READY),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Briefly prioritizes any coherent snapshot store over hot quote readers.
    ///
    /// A correction already owns the same publishing state for its full
    /// private-build/install interval, so its store does not acquire a nested
    /// token.
    pub(super) fn begin_publication(&self) -> Option<u64> {
        self.begin_correction()
    }

    /// Restores Ready only when this publication still owns its exact token.
    ///
    /// Source disconnect, failure, and shutdown transitions win the CAS.
    pub(super) fn complete_publication(&self, publishing: u64) -> bool {
        self.complete_correction(publishing)
    }

    pub(super) fn fail_correction(&self, correcting: u64) -> bool {
        self.0
            .compare_exchange(
                correcting,
                next(correcting, INVALID),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(super) fn revoke(&self) -> bool {
        self.transition_to_terminal(INVALID)
    }

    /// Invalidates a source-activity lease even when admission is already
    /// invalid, so recovery cannot republish across a disconnect race.
    pub(super) fn invalidate_source_lease(&self) -> bool {
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            if state(current) == STOPPING {
                return false;
            }
            match self.0.compare_exchange_weak(
                current,
                next(current, INVALID),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn stop(&self) -> bool {
        self.transition_to_terminal(STOPPING)
    }

    fn transition_to_terminal(&self, target: u64) -> bool {
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            let current_state = state(current);
            if current_state == target || current_state == STOPPING {
                return false;
            }
            match self.0.compare_exchange_weak(
                current,
                next(current, target),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn begin_expiration(&self, ready: u64) -> Option<u64> {
        if state(ready) != READY {
            return None;
        }
        let expiring = next(ready, EXPIRING);
        self.0
            .compare_exchange(ready, expiring, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| expiring)
    }

    pub(super) fn finish_expiration(&self, expiring: u64, unchanged: bool) -> bool {
        let target = if unchanged { INVALID } else { READY };
        self.0
            .compare_exchange(
                expiring,
                next(expiring, target),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
            && unchanged
    }
}

const fn state(token: u64) -> u64 {
    token & STATE_MASK
}

const fn next(token: u64, state: u64) -> u64 {
    token
        .wrapping_add(STATE_MASK + 1)
        .wrapping_sub(token & STATE_MASK)
        | state
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_disconnect_invalidates_a_recovery_publication_lease() {
        let availability = Availability::new();
        let lease = availability.token();
        assert!(availability.invalidate_source_lease());
        assert!(!availability.publish_if(lease));
        assert!(!availability.is_available());
    }

    #[test]
    fn normal_ready_publication_does_not_advance_the_cacheline() {
        let availability = Availability::new();
        assert!(availability.publish_if(availability.token()));
        let ready = availability.token();
        assert!(availability.publish_if(ready));
        assert_eq!(availability.token(), ready);
    }

    #[test]
    fn quote_admission_distinguishes_publication_progress() {
        let availability = Availability::new();
        assert_eq!(availability.quote_admission(), QuoteAdmission::Unavailable);
        assert!(availability.publish_if(availability.token()));
        assert_eq!(availability.quote_admission(), QuoteAdmission::Ready);
        let correction = availability.begin_correction().unwrap();
        assert!(availability.publish_if(correction));
        assert_eq!(availability.token(), correction);
        assert_eq!(availability.quote_admission(), QuoteAdmission::Publishing);
        assert!(availability.begin_publication().is_none());
        assert!(availability.complete_correction(correction));
        assert_eq!(availability.quote_admission(), QuoteAdmission::Ready);

        let publication = availability.begin_publication().unwrap();
        assert_eq!(availability.quote_admission(), QuoteAdmission::Publishing);
        assert!(availability.complete_publication(publication));
        assert_eq!(availability.quote_admission(), QuoteAdmission::Ready);
    }
}
