//! Composition root: build the engine, the schema, the HTTP app, and run the server.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::Extension;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::json;
use tower_http::trace::TraceLayer;

use agavelens_api::{build_schema, AgaveLensSchema, ApiContext};
use agavelens_core::{AnalyticsConfig, AnalyticsEngine, EngineDeps, SystemClock};
use agavelens_infra::MemorySampleRepository;

use crate::config::ServeArgs;
use crate::telemetry;

/// Wire a fresh engine backed by a bounded in-memory store of `capacity` samples.
pub fn build_engine(capacity: usize) -> Arc<AnalyticsEngine> {
    let repo = Arc::new(MemorySampleRepository::new(capacity));
    let clock = Arc::new(SystemClock);
    let config = AnalyticsConfig {
        max_samples: capacity,
        ..AnalyticsConfig::default()
    };
    Arc::new(AnalyticsEngine::new(EngineDeps { repo, clock }, config))
}

/// Build the GraphQL schema around a shared engine.
pub fn build_schema_from_engine(engine: Arc<AnalyticsEngine>) -> AgaveLensSchema {
    build_schema(ApiContext::new(engine))
}

/// Assemble the axum application: GraphQL endpoint, health probes, and metrics.
pub fn build_app(schema: AgaveLensSchema, metrics: PrometheusHandle) -> Router {
    Router::new()
        .route("/graphql", get(graphiql).post(graphql_handler))
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health_ready))
        .route("/metrics", get(metrics_handler))
        .layer(Extension(schema))
        .layer(Extension(metrics))
        .layer(TraceLayer::new_for_http())
}

/// Build engine + schema + app and serve until a shutdown signal arrives.
pub async fn run_server(args: ServeArgs) -> Result<()> {
    let metrics = telemetry::init_metrics()?;
    let engine = build_engine(args.samples_capacity);
    let schema = build_schema_from_engine(engine);
    let app = build_app(schema, metrics);

    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .with_context(|| format!("invalid bind address {}:{}", args.host, args.port))?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    tracing::info!(%addr, "agavelens analytics API listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;
    tracing::info!("shutdown complete");
    Ok(())
}

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

async fn graphql_handler(
    Extension(schema): Extension<AgaveLensSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    metrics::counter!("agavelens_graphql_requests_total").increment(1);
    schema.execute(req.into_inner()).await.into()
}

async fn health_live() -> impl IntoResponse {
    Json(json!({ "status": "live" }))
}

async fn health_ready() -> impl IntoResponse {
    Json(json!({ "status": "ready" }))
}

async fn metrics_handler(Extension(handle): Extension<PrometheusHandle>) -> impl IntoResponse {
    handle.render()
}

/// Resolve when the process receives Ctrl-C or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl-C, shutting down"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use metrics_exporter_prometheus::PrometheusBuilder;
    use tower::ServiceExt;

    fn test_app() -> Router {
        let metrics = PrometheusBuilder::new().build_recorder().handle();
        let engine = build_engine(1_000);
        let schema = build_schema_from_engine(engine);
        build_app(schema, metrics)
    }

    #[tokio::test]
    async fn health_ready_returns_ok() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_endpoint_renders() {
        let app = test_app();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn graphql_query_over_http() {
        let app = test_app();
        let body = serde_json::to_vec(&json!({ "query": "{ apiVersion }" })).unwrap();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/graphql")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
