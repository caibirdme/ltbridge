use super::*;
use crate::{
	errors::AppError, proto::tempopb::Trace, state::AppState,
	storage::QueryLimits,
};
use anyhow::anyhow;
use axum::{
	body::Bytes,
	extract::{Path, Query, State},
	http::header::{self, HeaderMap},
	response::{IntoResponse, Response},
	Json,
};
use axum_extra::protobuf::Protobuf;
use chrono::DateTime;
use common::TimeRange;
use itertools::Itertools;
use moka::sync::Cache;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_semantic_conventions::SCHEMA_URL;
use prost::Message;
use serde::Deserialize;
use validator::Validate;

const HEADER_ENCODING_PROTOBUF: &str = "application/protobuf";

#[derive(Deserialize, Debug, Validate)]
pub struct GetTraceByIDRequest {
	#[serde(rename = "start")]
	#[validate(custom(function = "crate::utils::validate::unix_timestamp"))]
	start_seconds: Option<u64>,
	#[serde(rename = "end")]
	#[validate(custom(function = "crate::utils::validate::unix_timestamp"))]
	end_seconds: Option<u64>,
}

impl From<GetTraceByIDRequest> for QueryLimits {
	fn from(value: GetTraceByIDRequest) -> Self {
		Self {
			limit: None,
			range: TimeRange {
				start: value.start_seconds.map(|v| {
					DateTime::from_timestamp(v as i64, 0)
						.map(|d| d.naive_utc())
						.unwrap()
				}),
				end: value.end_seconds.map(|v| {
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

pub async fn get_trace_by_id(
	Path(trace_id): Path<String>,
	header: HeaderMap,
	State(state): State<AppState>,
	Query(req): Query<GetTraceByIDRequest>,
) -> Result<GetTraceByIDResponse, AppError> {
	macro_rules! output_trace {
		($v:ident) => {
			match header.get(header::ACCEPT) {
				Some(enconding) if enconding == HEADER_ENCODING_PROTOBUF => {
					GetTraceByIDResponse::Proto(Protobuf($v))
				}
				_ => GetTraceByIDResponse::Json(Json($v)),
			}
		};
	}
	if let Ok(Some(tr)) = get_cached_trace(&trace_id, state.cache.clone()) {
		let val = output_trace!(tr);
		return Ok(val);
	}
	let handle = state.trace_handle;
	let spans = handle
		.query_trace(&trace_id, req.into())
		.await?
		.into_iter()
		.map(|span| spanitem_into_resourcespans(&span))
		.collect_vec();
	let resp = Trace {
		batches: reorder_spans(spans),
	};
	cache_trace(&trace_id, &resp, state.cache.clone());
	let val = output_trace!(resp);
	Ok(val)
}

fn cache_trace(trace_id: &str, trace: &Trace, cache: Cache<String, Vec<u8>>) {
	let d = trace.encode_to_vec();
	let key = get_trace_cache_key(trace_id);
	cache.insert(key.clone(), d.clone());
}

fn get_cached_trace(
	trace_id: &str,
	cache: Cache<String, Vec<u8>>,
) -> Result<Option<Trace>, AppError> {
	let data = cache.get(get_trace_cache_key(trace_id).as_str());
	match data {
		Some(data) => {
			let trace =
				Message::decode(Bytes::from(data)).map_err(|e| anyhow!(e))?;
			Ok(Some(trace))
		}
		None => Ok(None),
	}
}

fn get_trace_cache_key(trace_id: &str) -> String {
	format!("cc:tr:{}", trace_id)
}

fn reorder_spans(spans: Vec<ResourceSpans>) -> Vec<ResourceSpans> {
	spans
		.into_iter()
		.into_group_map_by(|sps| match &sps.resource {
			Some(res) => {
				let mut buf = vec![];
				res.encode(&mut buf).unwrap();
				buf
			}
			None => vec![],
		})
		.into_iter()
		.map(|(k, arr)| {
			let mut spss = ResourceSpans::default();
			if !k.is_empty() {
				let res: Resource = Message::decode(Bytes::from(k)).unwrap();
				spss.resource = Some(res);
			}
			spss.schema_url = SCHEMA_URL.to_string();
			spss.scope_spans =
				arr.into_iter().flat_map(|x| x.scope_spans).collect();
			spss
		})
		.collect()
}

#[derive(Debug)]
pub enum GetTraceByIDResponse {
	Proto(Protobuf<Trace>),
	Json(Json<Trace>),
}

impl IntoResponse for GetTraceByIDResponse {
	fn into_response(self) -> Response {
		match self {
			GetTraceByIDResponse::Proto(proto) => {
				([(header::CONTENT_TYPE, "application/protobuf")], proto)
					.into_response()
			}
			GetTraceByIDResponse::Json(json) => {
				([(header::CONTENT_TYPE, "application/json")], json)
					.into_response()
			}
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use pretty_assertions::assert_eq;
	#[test]
	fn it_works() {
		let s = serde_json::to_string(&Trace { batches: vec![] }).unwrap();
		assert_eq!(s, r#"{"batches":[]}"#);
	}
}
