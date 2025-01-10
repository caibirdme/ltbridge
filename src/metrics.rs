use crate::state::AppState;
use axum::{
	extract::{Request, State},
	http::StatusCode,
	middleware::Next,
	response::{IntoResponse, Response},
};
use chrono::{TimeDelta, Utc};
use opentelemetry::{
	metrics::{Counter, Histogram, MeterProvider},
	KeyValue,
};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::{Encoder, Registry, TextEncoder};

const HTTP_REQUEST_TOTAL_NAME: &str = "http_requests_total";
const HTTP_REQUEST_DURATION_SECONDS: &str = "http_request_duration_seconds";

#[derive(Clone)]
pub struct Instrumentations {
	registry: Registry,
	_provider: SdkMeterProvider,
	pub http_request_total: Counter<u64>,
	pub http_request_duration: Histogram<f64>,
}

#[derive(Clone)]
pub struct AddTotalTag {
	pub service: String,
	pub method: String,
	pub status: i64,
	pub uri: String,
}

impl Instrumentations {
	pub fn add_req_total(&self, req: &AddTotalTag) {
		self.http_request_total.add(
			1,
			&[
				KeyValue::new("service", req.service.clone()),
				KeyValue::new("method", req.method.clone()),
				KeyValue::new("status", req.status),
				KeyValue::new("uri", req.uri.clone()),
			],
		)
	}
	pub fn observe_req_duration(&self, seconds: f64, req: &AddTotalTag) {
		self.http_request_duration.record(
			seconds,
			&[
				KeyValue::new("service", req.service.clone()),
				KeyValue::new("method", req.method.clone()),
				KeyValue::new("status", req.status),
				KeyValue::new("uri", req.uri.clone()),
			],
		)
	}
}

pub fn setup_metrcis() -> Instrumentations {
	let registry = Registry::new();
	let exporter = opentelemetry_prometheus::exporter()
		.with_registry(registry.clone())
		.build()
		.unwrap();

	let provider = SdkMeterProvider::builder().with_reader(exporter).build();

	let meter = provider.meter(env!("CARGO_PKG_NAME"));

	let http_request_total = meter
		.u64_counter(HTTP_REQUEST_TOTAL_NAME)
		.with_description("Total number of http requests")
		.with_unit("")
		.build();

	let http_request_duration = meter
		.f64_histogram(HTTP_REQUEST_DURATION_SECONDS)
		.with_unit("s")
		.with_description("The HTTP request latencies in seconds")
		.build();

	Instrumentations {
		registry,
		_provider: provider,
		http_request_total,
		http_request_duration,
	}
}

pub async fn record_middleware(
	State(state): State<AppState>,
	request: Request,
	next: Next,
) -> Response {
	let start = Utc::now();
	let mut tags = AddTotalTag {
		service: "lgtmrs".to_string(),
		method: request.method().to_string(),
		status: 200,
		uri: request.uri().path().to_string(),
	};
	let response = next.run(request).await;

	tags.status = response.status().as_u16() as i64;
	state.metrics.add_req_total(&tags);
	let duration = Utc::now() - start;
	state
		.metrics
		.observe_req_duration(delta_to_seconds(duration), &tags);
	response
}

fn delta_to_seconds(d: TimeDelta) -> f64 {
	(d.num_nanoseconds().unwrap() as f64) / 1_000_000_000.0
}

pub async fn export_metrics(State(state): State<AppState>) -> Response {
	let encoder = TextEncoder::new();
	let metric_families = state.metrics.registry.gather();
	let mut buffer = vec![];
	match encoder.encode(&metric_families, &mut buffer) {
		Ok(()) => {
			let resp = String::from_utf8(buffer).unwrap();
			resp.into_response()
		}
		Err(e) => {
			(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
		}
	}
}
