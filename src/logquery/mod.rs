use crate::{errors::AppError, storage::QueryLimits};
use axum::{
	http::{HeaderMap, StatusCode},
	response::{IntoResponse, Json, Response},
};
use chrono::{DateTime, NaiveDateTime, Utc};
use common::TimeRange as StorageTimeRange;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::str::FromStr;
use std::{collections::HashMap, time::Duration};
use validator::Validate;

pub(crate) mod labels;
pub(crate) mod query_range;

pub(crate) use labels::query_label_values;
pub(crate) use labels::query_labels;
pub(crate) use labels::query_series;
pub(crate) use query_range::{loki_is_working, query_range};

const TENANT_KEY: &str = "tenant";
const DEFAULT_TENANT: &str = "default";

#[derive(Serialize, Deserialize, Hash, Debug, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
	Forward,
	Backward,
}

#[derive(Deserialize, Serialize, Hash, Debug, Clone, Validate)]
pub struct QueryRangeRequest {
	// at least 6 characters --> {a=""}
	#[validate(length(min = 6))]
	pub query: String,
	pub start: Option<LokiDate>,
	pub end: Option<LokiDate>,
	pub limit: Option<u32>,
	#[serde(default = "default_direction")]
	pub direction: Direction,
	#[serde(with = "humantime_serde")]
	pub step: Option<Duration>,
}

const fn default_direction() -> Direction {
	Direction::Backward
}

impl From<QueryRangeRequest> for QueryLimits {
	fn from(value: QueryRangeRequest) -> Self {
		Self {
			limit: value.limit,
			range: StorageTimeRange {
				start: value.start.map(|v| v.0.naive_utc()),
				end: value.end.map(|v| v.0.naive_utc()),
			},
			direction: Some(match value.direction {
				Direction::Forward => crate::storage::Direction::Forward,
				Direction::Backward => crate::storage::Direction::Backward,
			}),
			step: value.step,
		}
	}
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ResponseStatus {
	Success,
	Error,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryRangeResponse {
	pub status: ResponseStatus,
	pub data: QueryResult,
}

impl IntoResponse for QueryRangeResponse {
	fn into_response(self) -> Response {
		let status = match self.status {
			ResponseStatus::Success => StatusCode::OK,
			ResponseStatus::Error => StatusCode::INTERNAL_SERVER_ERROR,
		};
		(status, Json(self)).into_response()
	}
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum QueryResult {
	Streams(StreamResponse),
	Matrix(MatrixResponse),
	Vector(VectorResponse),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ResultType {
	Streams,
	Matrix,
	Vector,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MatrixResponse {
	#[serde(rename = "resultType")]
	pub result_type: ResultType,
	pub result: Vec<MatrixValue>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StreamResponse {
	#[serde(rename = "resultType")]
	pub result_type: ResultType,
	pub result: Vec<StreamValue>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VectorResponse {
	#[serde(rename = "resultType")]
	pub result_type: ResultType,
	pub result: Vec<VectorValue>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StreamValue {
	pub stream: HashMap<String, String>,
	pub values: Vec<[String; 2]>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VectorValue {
	pub metric: HashMap<String, String>,
	pub value: [serde_json::Value; 2],
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MatrixValue {
	pub metric: HashMap<String, String>,
	pub values: Vec<[serde_json::Value; 2]>,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct LokiDate(DateTime<Utc>);

impl<'de> Deserialize<'de> for LokiDate {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let value = String::deserialize(deserializer)?;
		parse_timestamp(&value)
			.map(LokiDate)
			.map_err(de::Error::custom)
	}
}

impl Serialize for LokiDate {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		self.0.to_rfc3339().serialize(serializer)
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeRange {
	StartFrom(i64),
	EndAt(i64),
	BetweenInclude(i64, i64),
	UnLimited,
}

impl From<(Option<LokiDate>, Option<LokiDate>)> for TimeRange {
	fn from(value: (Option<LokiDate>, Option<LokiDate>)) -> Self {
		match value {
			(None, None) => TimeRange::UnLimited,
			(None, Some(end)) => TimeRange::EndAt(end.0.timestamp()),
			(Some(start), None) => TimeRange::StartFrom(start.0.timestamp()),
			(Some(start), Some(end)) => TimeRange::BetweenInclude(
				start.0.timestamp(),
				end.0.timestamp(),
			),
		}
	}
}

// support different loki time format
fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, AppError> {
	if let Ok(seconds) = value.parse::<i64>() {
		if seconds.to_string().len() <= 10 {
			if let Some(dt) = DateTime::from_timestamp(seconds, 0) {
				return Ok(dt);
			}
			return Err(AppError::InvalidTimeFormat(value.to_string()));
		} else {
			let nanos = (seconds % 1_000_000_000) as u32;
			let secs = seconds / 1_000_000_000;
			if let Some(dt) = DateTime::from_timestamp(secs, nanos) {
				return Ok(dt);
			}
			return Err(AppError::InvalidTimeFormat(value.to_string()));
		}
	} else if let Ok(timestamp) = value.parse::<f64>() {
		let secs = timestamp.trunc() as i64;
		let nanos = (timestamp.fract() * 1_000_000_000.0) as u32;
		if let Some(dt) = DateTime::from_timestamp(secs, nanos) {
			return Ok(dt);
		}
		return Err(AppError::InvalidTimeFormat(value.to_string()));
	} else if let Ok(dt) = DateTime::<Utc>::from_str(value) {
		return Ok(dt);
	}
	Err(AppError::InvalidTimeFormat(value.to_string()))
}

pub struct MetricTable {
	pub level: String,
	pub total: u64,
	pub ts: NaiveDateTime,
}

#[derive(Deserialize, Debug)]
pub struct QuerySeriesRequest {
	pub start: Option<LokiDate>,
	pub end: Option<LokiDate>,
	#[serde(rename = "match[]")]
	pub matches: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QuerySeriesResponse {
	pub status: ResponseStatus,
	pub data: Vec<HashMap<String, String>>,
}

#[derive(Deserialize, Debug)]
pub struct QueryLabelsRequest {
	start: Option<LokiDate>,
	end: Option<LokiDate>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryLabelsResponse {
	pub status: ResponseStatus,
	pub data: Vec<String>,
}

impl IntoResponse for QueryLabelsResponse {
	fn into_response(self) -> Response {
		(StatusCode::OK, Json(self)).into_response()
	}
}

#[derive(Deserialize, Debug)]
pub struct QueryLabelValuesRequest {
	start: Option<LokiDate>,
	end: Option<LokiDate>,
	_query: Option<String>,
}

pub fn get_tenant(header: &HeaderMap) -> &str {
	header
		.get(TENANT_KEY)
		.map(|v| v.to_str().unwrap())
		.unwrap_or(DEFAULT_TENANT)
}

#[cfg(test)]
mod tests {
	use super::*;
	use pretty_assertions::assert_eq;
	use std::collections::HashMap;
	#[test]
	fn it_works() {
		let rsp = QueryRangeResponse {
			status: ResponseStatus::Success,
			data: QueryResult::Streams(StreamResponse {
				result_type: ResultType::Streams,
				result: vec![StreamValue {
					stream: HashMap::from([
						("k1".to_string(), "v1".to_string()),
						("k2".to_string(), "v2".to_string()),
					]),
					values: vec![
						["1".to_string(), "2".to_string()],
						["3".to_string(), "4".to_string()],
					],
				}],
			}),
		};
		let expect = serde_json::json!(
			{
				"status": "success",
				"data": {
					"resultType": "streams",
					"result": [{
						"stream": {
							"k1": "v1",
							"k2": "v2"
						},
						"values": [
							["1", "2"],
							["3", "4"]
						]
					}]
				}
			}
		);
		let actual: serde_json::Value =
			serde_json::from_str(serde_json::to_string(&rsp).unwrap().as_str())
				.unwrap();
		assert_eq!(expect, actual);
	}
}
