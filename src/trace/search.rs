use std::collections::HashMap;

use super::json_value_to_opt_pb_any_value;
use crate::{
	errors::AppError,
	proto::tempopb::{
		SearchResponse, Span as TempoSpan, SpanSet, TraceSearchMetadata,
	},
	state::AppState,
	storage::{trace::SpanItem, QueryLimits},
};
use axum::{
	extract::{Query, State},
	Json,
};
use axum_valid::Valid;
use chrono::DateTime;
use common::TimeRange;
use itertools::Itertools;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Deserialize, Debug, Validate)]
pub struct SearchTraceRequest {
	#[validate(length(min = 1))]
	pub q: String,
	pub limit: Option<u32>,
	#[validate(custom(function = "crate::utils::validate::unix_timestamp"))]
	pub start: Option<u64>,
	#[validate(custom(function = "crate::utils::validate::unix_timestamp"))]
	pub end: Option<u64>,
}

impl From<SearchTraceRequest> for QueryLimits {
	fn from(value: SearchTraceRequest) -> Self {
		Self {
			limit: value.limit,
			range: TimeRange {
				start: value.start.map(|v| {
					DateTime::from_timestamp(v as i64, 0)
						.map(|d| d.naive_utc())
						.unwrap()
				}),
				end: value.end.map(|v| {
					DateTime::from_timestamp(v as i64, 0)
						.map(|d| d.naive_utc())
						.unwrap()
				}),
			},
			direction: None,
			step: None,
		}
	}
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct SearchTagsRequest {
	#[serde(default = "default_scope")]
	pub scope: Option<ScopeType>,
	pub start: Option<u64>,
	pub end: Option<u64>,
}

const fn default_scope() -> Option<ScopeType> {
	Some(ScopeType::All)
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ScopeType {
	Span,
	Resource,
	Intrinsic,
	All,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SearchTagsResponse {
	#[serde(rename = "tagNames")]
	pub tag_names: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ScopeTag {
	pub name: String,
	pub tags: Vec<String>,
}

pub async fn search_tags(
	State(_state): State<AppState>,
	Query(_req): Query<SearchTagsRequest>,
) -> Result<Json<SearchTagsResponse>, AppError> {
	Ok(Json(SearchTagsResponse { tag_names: vec![] }))
}

pub async fn search_trace_v2(
	Valid(Query(req)): Valid<Query<SearchTraceRequest>>,
	State(state): State<AppState>,
) -> Result<Json<SearchResponse>, AppError> {
	let expr = traceql::parse_traceql(&req.q)?;
	let handle = state.trace_handle;
	let spans = handle.search_span(&expr, req.into()).await?;

	// convert to tempo required format
	let root_name = get_root_name_map(&spans);
	let traces = spans
		.iter()
		.into_group_map_by(|sp| &sp.trace_id)
		.into_iter()
		.map(|(trace_id, sps)| {
			let spsset: Vec<TempoSpan> = sps
				.iter()
				.map(|v| TempoSpan {
					span_id: v.span_id.clone(),
					name: v.span_name.clone(),
					start_time_unix_nano: v.ts.timestamp_nanos_opt().unwrap()
						as u64,
					duration_nanos: v.duration as u64,
					attributes: v
						.span_attributes
						.iter()
						.map(|(k, v)| KeyValue {
							key: k.clone(),
							value: json_value_to_opt_pb_any_value(v.clone()),
						})
						.collect(),
				})
				.collect();
			let matched = spsset.len() as u32;
			let start_time_nano = root_name
				.get(trace_id)
				.map(|(_, _, start, _)| *start)
				.unwrap_or_default();
			TraceSearchMetadata {
				trace_id: trace_id.clone(),
				root_service_name: root_name
					.get(trace_id)
					.map(|(_, service_name, _, _)| service_name.clone())
					.unwrap_or_default(),
				root_trace_name: root_name
					.get(trace_id)
					.map(|(span_name, _, _, _)| span_name.clone())
					.unwrap_or_default(),
				start_time_unix_nano: start_time_nano,
				duration_ms: root_name
					.get(trace_id)
					.map(|(_, _, _, end)| (*end - start_time_nano) / 1000000)
					.unwrap_or_default() as u32,
				span_set: None,
				span_sets: vec![SpanSet {
					spans: spsset,
					matched,
				}],
			}
		})
		.collect::<Vec<TraceSearchMetadata>>();
	let resp = SearchResponse {
		traces,
		metrics: None,
	};
	Ok(Json(resp))
}

// get all root span's name,service name, start_unix_nano and duration
fn get_root_name_map(
	spans: &[SpanItem],
) -> HashMap<String, (String, String, u64, u64)> {
	// find each trace's last span endtime
	let endtime_map = spans
		.iter()
		.into_group_map_by(|v| v.trace_id.clone())
		.into_iter()
		.map(|(k, arr)| {
			let w = arr
				.iter()
				.max_by(|x, y| {
					let x_time =
						x.ts.timestamp_nanos_opt().unwrap() + x.duration;
					let y_time =
						y.ts.timestamp_nanos_opt().unwrap() + y.duration;
					x_time.cmp(&y_time)
				})
				.unwrap();
			(k, (w.ts.timestamp_nanos_opt().unwrap() + w.duration) as u64)
		})
		.collect::<HashMap<String, u64>>();

	let mut root_name = HashMap::new();
	spans.iter().for_each(|sp| {
		if sp.parent_span_id.is_empty() {
			root_name.insert(
				sp.trace_id.clone(),
				(
					sp.span_name.clone(),
					sp.service_name.clone(),
					sp.ts.timestamp_nanos_opt().unwrap() as u64,
					endtime_map.get(&sp.trace_id).copied().unwrap_or(100000000),
				),
			);
		}
	});
	root_name
}

#[derive(Serialize, Debug)]
pub struct TagValuesResponse {
	#[serde(rename = "tagValues")]
	pub tag_values: Vec<String>,
}

pub async fn search_tag_values() -> Result<Json<TagValuesResponse>, AppError> {
	Ok(Json(TagValuesResponse { tag_values: vec![] }))
}
