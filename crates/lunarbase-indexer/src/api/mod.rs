//! HTTP API composition.

mod handlers;
mod model;

use crate::metrics::ServiceMetrics;
use crate::runtime::RuntimeHandle;
use axum::routing::{get, post};
use axum::Router;
use std::future::Future;
use std::net::SocketAddr;

#[derive(Clone)]
struct ApiState {
    runtime: RuntimeHandle,
    metrics: ServiceMetrics,
}

/// Serves liveness, readiness, and quote endpoints until shutdown resolves.
pub async fn serve(
    bind: SocketAddr,
    runtime: RuntimeHandle,
    metrics: ServiceMetrics,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), std::io::Error> {
    let router = Router::new()
        .route("/health/live", get(handlers::live))
        .route("/health/ready", get(handlers::ready))
        .route("/metrics", get(handlers::metrics))
        .route("/v1/quote", post(handlers::quote))
        .with_state(ApiState { runtime, metrics });
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}
