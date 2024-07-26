use super::QueryLimits;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, offset::Utc};
use dyn_clone::DynClone;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use traceql::Expression;

#[async_trait]
pub trait TraceStorage: DynClone + Send + Sync {
	async fn query_trace(
		&self,
		trace_id: &str,
		opt: QueryLimits,
	) -> Result<Vec<SpanItem>>;
	async fn search_span(
		&self,
		expr: &Expression,
		opt: QueryLimits,
	) -> Result<Vec<SpanItem>>;
	async fn span_tags(&self, _opt: QueryLimits) -> Result<Vec<String>> {
		Ok(vec![])
	}
	async fn span_tag_values(
		&self,
		_tag: &str,
		_opt: QueryLimits,
	) -> Result<Vec<String>> {
		Ok(vec![])
	}
}

dyn_clone::clone_trait_object!(TraceStorage);

#[derive(Debug, Default, Clone)]
pub struct SpanItem {
	pub ts: DateTime<Utc>,
	pub trace_id: String,
	pub span_id: String,
	pub parent_span_id: String,
	pub trace_state: String,
	pub span_name: String,
	pub span_kind: i32,
	pub service_name: String,
	pub resource_attributes: HashMap<String, serde_json::Value>,
	pub scope_name: Option<String>,
	pub scope_version: Option<String>,
	pub span_attributes: HashMap<String, serde_json::Value>,
	pub duration: i64,
	pub status_code: Option<i32>,
	pub status_message: Option<String>,
	pub span_events: Vec<SpanEvent>,
	pub link: Vec<Links>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
	#[serde(rename = "time_unix_nano")]
	#[serde(deserialize_with = "deserialize_naivedatetime")]
	pub ts: DateTime<Utc>,
	pub dropped_attributes_count: u32,
	pub name: String,
	pub attributes: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Links {
	pub trace_id: String,
	pub span_id: String,
	pub trace_state: String,
	pub attributes: HashMap<String, serde_json::Value>,
}

fn deserialize_naivedatetime<'de, D>(
	deserializer: D,
) -> Result<DateTime<Utc>, D::Error>
where
	D: Deserializer<'de>,
{
	let s = String::deserialize(deserializer)?;
	parse_datetime(&s).map_err(serde::de::Error::custom)
}

static DATETIME_FORMATS: [&str; 3] = ["%+", "%c", "%s"];

fn parse_datetime(s: &str) -> Result<DateTime<Utc>> {
	// try best effort to parse datetime
	// https://docs.rs/chrono/latest/chrono/format/strftime/index.html#specifiers
	for format in DATETIME_FORMATS {
		if let Ok(d) = DateTime::parse_from_str(s, format) {
			return Ok(d.to_utc());
		}
	}
	Err(anyhow::anyhow!("invalid datetime format"))
}

#[cfg(test)]
mod tests {
	use crate::storage::trace::parse_datetime;
	use itertools::Itertools;
	use std::time::Duration;

	#[test]
	fn test_parse_naivedatetime() {
		let test_cases = vec![
			"2021-08-01T12:00:00.123000Z",
			"2021-08-01T12:00:00.123Z",
			"2021-08-01T12:00:00.123000000Z",
		];
		let actual = test_cases
			.iter()
			.filter_map(|s| parse_datetime(s).ok())
			.collect_vec();
		assert!(actual.iter().all_equal());
	}
	#[test]
	fn test_parse_naivedatetime_v2() {
		let test_cases = vec!["2024-05-04T17:38:07Z", "1714815487"];
		let actual = test_cases
			.iter()
			.filter_map(|s| parse_datetime(s).ok())
			.collect_vec();
		assert!(actual.len() == test_cases.len());
		assert_eq!(actual[0], actual[1] + Duration::from_secs(8 * 60 * 60));
	}
}
