use super::{sdk, *};
use crate::storage::{trace::*, *};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JSONValue;
use std::collections::HashMap;
use traceql::*;

#[derive(Clone)]
pub struct QuickwitTrace {
	cli: sdk::QuickwitSdk,
}

impl QuickwitTrace {
	pub fn new(cfg: QuickwitServerConfig) -> Self {
		let cli = sdk::QuickwitSdk::new(cfg);
		QuickwitTrace { cli }
	}
}

#[async_trait]
impl TraceStorage for QuickwitTrace {
	async fn query_trace(
		&self,
		trace_id: &str,
		opt: QueryLimits,
	) -> Result<Vec<SpanItem>> {
		let query = sdk::SearcgRequest {
			query: format!("trace_id:{}", trace_id),
			start_timestamp: opt.range.start.map(|v| v.and_utc().timestamp()),
			end_timestamp: opt.range.end.map(|v| v.and_utc().timestamp()),
			..Default::default()
		};
		let sps: Vec<SpanItem> = self
			.cli
			.search_records(&query)
			.await?
			.hits
			.into_iter()
			.filter_map(|v| {
				let sp: Option<QuickwitSpan> =
					serde_json::from_value(v).map_err(|e| anyhow!(e)).ok();
				sp
			})
			.map(Into::into)
			.collect_vec();
		Ok(sps)
	}
	async fn search_span(
		&self,
		_expr: &Expression,
		_opt: QueryLimits,
	) -> Result<Vec<SpanItem>> {
		Ok(vec![])
	}
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct QuickwitSpan {
	#[serde(
		rename = "span_start_timestamp_nanos",
		deserialize_with = "deserialize_timestamp"
	)]
	pub ts: DateTime<Utc>,
	pub trace_id: String,
	pub span_id: String,
	pub parent_span_id: Option<String>,
	pub trace_state: Option<String>,
	pub span_name: String,
	pub span_kind: i32,
	pub service_name: String,
	#[serde(default)]
	pub resource_attributes: HashMap<String, JSONValue>,
	pub scope_name: Option<String>,
	pub scope_version: Option<String>,
	#[serde(default)]
	pub span_attributes: HashMap<String, JSONValue>,
	#[serde(rename = "span_duration_millis")]
	pub duration: i64,
	#[serde(
		rename = "span_status",
		deserialize_with = "deserialize_status_code",
		default = "default_status_code"
	)]
	pub status_code: Option<i32>,
	#[serde(rename = "status_message")]
	pub status_message: Option<String>,
	#[serde(rename = "events", default)]
	pub span_events: Vec<QuickwitSpanEvent>,
	#[serde(default)]
	pub link: Vec<QuickwitLinks>,
}

impl From<QuickwitSpan> for SpanItem {
	fn from(val: QuickwitSpan) -> SpanItem {
		let mut sp = SpanItem {
			ts: val.ts,
			trace_id: val.trace_id,
			span_id: val.span_id,
			parent_span_id: val.parent_span_id.unwrap_or_default(),
			trace_state: val.trace_state.unwrap_or_default(),
			span_name: val.span_name,
			span_kind: val.span_kind,
			service_name: val.service_name,
			resource_attributes: val.resource_attributes,
			scope_name: val.scope_name,
			scope_version: val.scope_version,
			span_attributes: val.span_attributes,
			duration: val.duration * 1_000_000,
			status_code: val.status_code,
			status_message: {
				if let Some(code) = val.status_code {
					let v = match code {
						1 => "OK".to_string(),
						2 => "ERROR".to_string(),
						_ => "UNKNOWN".to_string(),
					};
					Some(v)
				} else {
					None
				}
			},
			span_events: val
				.span_events
				.into_iter()
				.map(|v| SpanEvent {
					ts: v.event_timestamp,
					dropped_attributes_count: 0,
					name: v.event_name,
					attributes: v.event_attributes,
				})
				.collect_vec(),
			link: val
				.link
				.into_iter()
				.map(|v| Links {
					trace_id: v.trace_id,
					span_id: v.span_id,
					trace_state: v.trace_state,
					attributes: v.attributes,
				})
				.collect_vec(),
		};
		// Add service name to resource attributes(OTLP convention)
		sp.resource_attributes.insert(
			"service.name".to_string(),
			JSONValue::String(sp.service_name.clone()),
		);
		sp
	}
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct QuickwitSpanEvent {
	pub event_name: String,
	#[serde(
		rename = "event_timestamp_nanos",
		deserialize_with = "deserialize_timestamp"
	)]
	pub event_timestamp: DateTime<Utc>,
	#[serde(default)]
	pub event_attributes: HashMap<String, JSONValue>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct QuickwitLinks {
	pub trace_id: String,
	pub span_id: String,
	pub trace_state: String,
	pub attributes: HashMap<String, JSONValue>,
}

// Function to deserialize timestamp from nanoseconds
fn deserialize_timestamp<'de, D>(
	deserializer: D,
) -> Result<DateTime<Utc>, D::Error>
where
	D: Deserializer<'de>,
{
	let nanos: i64 = Deserialize::deserialize(deserializer)?;
	Ok(DateTime::from_timestamp_nanos(nanos))
}

const fn default_status_code() -> Option<i32> {
	Some(0)
}

// Function to deserialize status code
fn deserialize_status_code<'de, D>(
	deserializer: D,
) -> Result<Option<i32>, D::Error>
where
	D: Deserializer<'de>,
{
	#[derive(Deserialize)]
	struct Status {
		code: String,
	}

	let status: Status = Deserialize::deserialize(deserializer)?;
	let code = match status.code.to_uppercase().as_str() {
		"OK" => 1,
		"ERROR" => 2,
		_ => 0,
	};
	Ok(Some(code))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_der_qw_trace_json() {
		let j = serde_json::json!(        {
			"events": [
				{
					"event_attributes": {
						"ctx.deadline": "999.873302ms",
						"message.detail": "{\"msg\":\"caibirdme\"}",
						"message.uncompressed_size": 19
					},
					"event_name": "SENT",
					"event_timestamp_nanos": 1716190734199699272 as i64
				}
			],
			"parent_span_id": "e5d183b642cd6bbf",
			"resource_attributes": {
				"server.owner": "",
				"telemetry.sdk.language": "go",
			},
			"scope_name": "go.opentelemetry.io/otel/sdk/tracer",
			"service_name": "test.front",
			"span_attributes": {
				"baggage": "",
				"net.host.ip": "127.0.0.1",
				"trpc.status_type": 0
			},
			"span_duration_millis": 0,
			"span_end_timestamp_nanos": 1716190734200402000 as i64,
			"span_fingerprint": "x",
			"span_id": "7735303533a98def",
			"span_kind": 3,
			"span_name": "trpc.test.helloworld.Greeter/SayHi",
			"span_start_timestamp_nanos": 1716190734199689000 as i64,
			"span_status": {
				"code": "ok"
			},
			"trace_id": "ec1857b46bdc76e4a56ef6258077340b"
		});
		let sp: QuickwitSpan = serde_json::from_value(j).unwrap();
		assert_eq!(sp.service_name, "test.front");
		assert_eq!(sp.span_id, "7735303533a98def");
		assert_eq!(sp.trace_id, "ec1857b46bdc76e4a56ef6258077340b");
		assert_eq!(sp.span_name, "trpc.test.helloworld.Greeter/SayHi");
		assert_eq!(sp.span_kind, 3);
		assert_eq!(sp.status_code, Some(1));
		assert_eq!(
			sp.span_attributes
				.get("net.host.ip")
				.unwrap()
				.as_str()
				.unwrap(),
			"127.0.0.1"
		);
	}
}
