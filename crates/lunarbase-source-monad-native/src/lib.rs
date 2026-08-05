//! Linux adapter for the official Monad execution-event ring.

#[cfg(all(feature = "native-event-ring", target_os = "linux"))]
mod native;

#[cfg(all(feature = "native-event-ring", target_os = "linux"))]
pub use native::{MonadEventRingConfig, MonadEventRingSource, connect_event_ring};
