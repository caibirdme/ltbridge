use super::*;
use crate::{errors::AppError, state::AppState};
use axum::{
	extract::{Path, Query, State},
	Json,
};
use common::TimeRange;

pub async fn query_labels(
	State(state): State<AppState>,
	Query(req): Query<QueryLabelsRequest>,
) -> Result<QueryLabelsResponse, AppError> {
	let cache = state.cache;
	if let Some(c) = cache.get(label_cache_key()) {
		return Ok(serde_json::from_slice(&c).unwrap());
	}
	let labels = state
		.log_handle
		.labels(QueryLimits {
			limit: None,
			range: time_range_less_in_a_day(req.start, req.end),
			direction: None,
			step: None,
		})
		.await?;
	let should_cache = !labels.is_empty();
	let resp = QueryLabelsResponse {
		status: ResponseStatus::Success,
		data: labels,
	};
	if should_cache {
		let d = serde_json::to_vec(&resp).unwrap();
		cache.insert(label_cache_key().to_string(), d);
	}
	Ok(resp)
}

fn time_range_less_in_a_day(
	start: Option<LokiDate>,
	_: Option<LokiDate>,
) -> TimeRange {
	let two_hour_before = Utc::now() - Duration::from_secs(2 * 60 * 60);
	let start = start.or(Some(LokiDate(two_hour_before))).map(|d| {
		let d = d.0;
		if d > two_hour_before {
			d
		} else {
			two_hour_before
		}
	});
	TimeRange {
		start: start.map(|v| v.naive_utc()),
		end: None,
	}
}

const fn label_cache_key() -> &'static str {
	"cc:labels"
}

fn label_values_cache_key(k: &str) -> String {
	format!("cc:label_values:{}", k)
}

pub async fn query_label_values(
	State(state): State<AppState>,
	Path(label): Path<String>,
	Query(req): Query<QueryLabelValuesRequest>,
) -> Result<QueryLabelsResponse, AppError> {
	let cache = state.cache;
	let cache_key = label_values_cache_key(&label);
	if let Some(c) = cache.get(&cache_key) {
		return Ok(serde_json::from_slice(&c).unwrap());
	}
	let values = state
		.log_handle
		.label_values(
			&label,
			QueryLimits {
				limit: None,
				range: time_range_less_in_a_day(req.start, req.end),
				direction: None,
				step: None,
			},
		)
		.await?;
	let should_cache = !values.is_empty();
	let resp = QueryLabelsResponse {
		status: ResponseStatus::Success,
		data: values,
	};
	if should_cache {
		let d = serde_json::to_vec(&resp).unwrap();
		cache.insert(cache_key, d);
	}
	Ok(resp)
}

pub async fn query_series() -> Json<QuerySeriesResponse> {
	Json(QuerySeriesResponse {
		status: ResponseStatus::Success,
		data: vec![],
	})
}
