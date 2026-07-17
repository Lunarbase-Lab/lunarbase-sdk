#[cfg(test)]
mod lease_tests {
    use super::{CheckpointStore, InMemoryRedisStore};
    use crate::{ChainCursor, Checkpoint, Commitment, MATH_COMPATIBILITY_VERSION, SCHEMA_VERSION};
    use lunarbase_math::QuoteState;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn writer_lease_is_owner_checked_and_expires() {
        let mut store = InMemoryRedisStore::new(8);
        assert!(store
            .acquire_writer_lease("writer-a", Duration::from_millis(20))
            .unwrap());
        assert!(!store
            .acquire_writer_lease("writer-b", Duration::from_millis(20))
            .unwrap());
        assert!(!store
            .renew_writer_lease("writer-b", Duration::from_millis(20))
            .unwrap());
        assert!(!store
            .acquire_writer_lease("writer-b", Duration::from_millis(20))
            .unwrap());
        store.release_writer_lease("writer-a").unwrap();
        assert!(store
            .acquire_writer_lease("writer-b", Duration::from_millis(20))
            .unwrap());
        store.release_writer_lease("writer-a").unwrap();
        assert!(!store
            .acquire_writer_lease("writer-c", Duration::from_millis(20))
            .unwrap());
        sleep(Duration::from_millis(25));
        assert!(store
            .acquire_writer_lease("writer-c", Duration::from_millis(20))
            .unwrap());
    }

    #[test]
    fn writer_can_renew_and_release_its_own_lease() {
        let mut store = InMemoryRedisStore::new(8);
        assert!(store
            .acquire_writer_lease("writer", Duration::from_secs(1))
            .unwrap());
        assert!(store
            .renew_writer_lease("writer", Duration::from_secs(1))
            .unwrap());
        store.release_writer_lease("writer").unwrap();
        assert!(store
            .acquire_writer_lease("standby", Duration::from_secs(1))
            .unwrap());
    }

    #[test]
    fn checkpoint_commit_is_fenced_by_current_lease_owner() {
        let mut store = InMemoryRedisStore::new(8);
        assert!(store
            .acquire_writer_lease("writer-a", Duration::from_secs(1))
            .unwrap());
        store.configure_writer_lease(Some("writer-a"));
        assert!(store.commit(checkpoint(), Vec::new()).is_ok());
        store.release_writer_lease("writer-a").unwrap();
        assert!(store.commit(checkpoint(), Vec::new()).is_err());
        assert!(store
            .acquire_writer_lease("writer-b", Duration::from_secs(1))
            .unwrap());
        assert!(store.commit(checkpoint(), Vec::new()).is_err());
        store.configure_writer_lease(Some("writer-b"));
        assert!(store.commit(checkpoint(), Vec::new()).is_ok());
    }

    fn checkpoint() -> Checkpoint {
        Checkpoint {
            schema_version: SCHEMA_VERSION,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            expected_runtime_code_hash: [0; 32],
            cursor: ChainCursor::block(1, 1, Some([1; 32]), Commitment::Canonical),
            state: QuoteState::default(),
        }
    }
}

