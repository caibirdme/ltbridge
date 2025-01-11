use crate::{logquery, metrics, state};
use axum::{
	extract::{Json, Request},
	http::StatusCode,
	middleware::from_fn_with_state,
	routing::{any, get, on, MethodFilter},
	Router,
};
use http::Request as HttpRequest;
use serde::Serialize;
use tower::ServiceBuilder;
use tower_http::trace::DefaultOnResponse;
use tower_http::{
	compression::CompressionLayer, decompression::RequestDecompressionLayer,
	timeout::TimeoutLayer, trace::TraceLayer,
};
use tracing::{info, Span};

static SKIP_LOGGING_PATHS: [&str; 3] = ["/ready", "/metrics", "/api/echo"];

// Loki HTTP API, see https://grafana.com/docs/loki/latest/reference/api/#query-endpoints
pub fn new_router(state: state::AppState) -> Router {
	let cfg = state.config.clone();
	let app = Router::new()
		.route("/ready", any(ok))
		.route("/metrics", get(metrics::export_metrics))
		// loki API
		// /loki/api/v1/query grafana use this endpoint to check if the datasource is working
		.route("/loki/api/v1/query", get(logquery::loki_is_working))
		.route("/loki/api/v1/labels", get(logquery::query_labels))
		.route(
			"/loki/api/v1/label/{label}/values",
			get(logquery::query_label_values),
		)
		.route("/loki/api/v1/query_range", get(logquery::query_range))
		.route(
			"/loki/api/v1/series",
			on(
				MethodFilter::GET.or(MethodFilter::POST),
				logquery::query_series,
			),
		)
		// collector API for ingesting traces, just for test
		// tempo API
		.route("/api/status/buildinfo", get(build_info))
		.route(
			"/api/traces/{trace_id}",
			get(crate::trace::get_trace_by_id),
		)
		.route("/api/search", get(crate::trace::search_trace_v2))
		.route("/api/v2/search", get(crate::trace::search_trace_v2))
		.route("/api/v2/search/tags", get(crate::trace::search_tags))
		.route("/api/v2/search/tag/{tag_name}/values", get(crate::trace::search_tag_values))
		// https://grafana.com/docs/tempo/latest/api_docs/#query-echo-endpoint
		.route("/api/echo", get(|| async { "echo" }))
		.fallback(handler_404)
		.with_state(state.clone())
		.layer(
			ServiceBuilder::new()
				.layer(
					TraceLayer::new_for_http()
						.on_request(
							|req: &HttpRequest<_>, _: &Span| {
								let p = req.uri().path();
								if SKIP_LOGGING_PATHS.contains(&p) {
									return;
								}
								info!(method = ?req.method(), path = p, query = req.uri().query(), "request received");
							}
						)
						.on_response(
							DefaultOnResponse::new()
								.level(tracing::Level::INFO),
						),
				)
				.layer(from_fn_with_state(state, metrics::record_middleware))
				.layer(TimeoutLayer::new(cfg.server.timeout))
				.layer(CompressionLayer::new())
				.layer(RequestDecompressionLayer::new()),
		);
	app
}

async fn ok() -> StatusCode {
	StatusCode::OK
}

async fn handler_404(req: Request) -> StatusCode {
	dbg!(req.uri());
	StatusCode::NOT_FOUND
}

// https://grafana.com/docs/tempo/latest/api_docs/#list-build-information
async fn build_info() -> Json<BuildInfo> {
	Json(BuildInfo {
		version: env!("CARGO_PKG_VERSION").to_string(),
		revision: "master".to_string(),
		branch: "master".to_string(),
		build_date: "2024-04-30".to_string(),
		build_user: env!("CARGO_PKG_AUTHORS").to_string(),
		go_version: "rust-".to_string() + env!("CARGO_PKG_RUST_VERSION"),
	})
}

#[derive(Debug, Serialize)]
struct BuildInfo {
	version: String,
	revision: String,
	branch: String,
	#[serde(rename = "buildDate")]
	build_date: String,
	#[serde(rename = "buildUser")]
	build_user: String,
	#[serde(rename = "goVersion")]
	go_version: String,
}
