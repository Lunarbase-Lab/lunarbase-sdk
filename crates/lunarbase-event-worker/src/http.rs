//! Independent operational endpoints for the durable event pipeline.

use crate::metrics::Metrics;
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::json;
use std::{future::Future, net::SocketAddr, sync::Arc};

#[derive(Clone)]
struct ApiState {
    metrics: Arc<Metrics>,
}

pub(crate) async fn serve(
    bind: SocketAddr,
    metrics: Arc<Metrics>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), std::io::Error> {
    let router = Router::new()
        .route("/livez", get(live))
        .route("/readyz", get(ready))
        .route("/metrics", get(prometheus))
        .with_state(ApiState { metrics });
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}

async fn live() -> impl IntoResponse {
    Json(json!({"live": true}))
}

async fn ready(State(state): State<ApiState>) -> Response {
    let ready = state.metrics.is_ready();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "ready": ready,
            "lastPersistedBlock": state.metrics.last_persisted_block(),
        })),
    )
        .into_response()
}

async fn prometheus(State(state): State<ApiState>) -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "text/plain; version=0.0.4"
            .parse()
            .expect("static content type"),
    );
    (headers, state.metrics.render())
}
