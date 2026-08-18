use super::{MAX_POLL_INTERVAL, MonadEventRingConfig};
use lunarbase_client::model::MIN_UPDATE_QUEUE_BYTE_CAPACITY;
use lunarbase_math::Address;
use lunarbase_source_monad::execution::MonadDeliveryMode;
use std::{path::PathBuf, time::Duration};

fn config() -> MonadEventRingConfig {
    MonadEventRingConfig {
        event_ring_path: PathBuf::from("unused"),
        core: Address::new([1; 20]),
        chain_id: 143,
        queue_bound: 1,
        queue_byte_bound: MIN_UPDATE_QUEUE_BYTE_CAPACITY,
        poll_interval: Duration::from_micros(100),
        delivery_mode: MonadDeliveryMode::Realtime,
        emit_removed_logs: false,
    }
}

#[test]
fn poll_interval_is_bounded_for_prompt_shutdown() {
    let mut value = config();
    value.poll_interval = Duration::ZERO;
    assert!(value.validate().is_err());

    value.poll_interval = MAX_POLL_INTERVAL + Duration::from_nanos(1);
    assert!(value.validate().is_err());

    value.poll_interval = MAX_POLL_INTERVAL;
    assert!(value.validate().is_ok());
}
