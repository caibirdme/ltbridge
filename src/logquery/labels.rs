use std::sync::Arc;

use super::*;
use crate::{errors::AppError, state::AppState};
use axum::{
	extract::{rejection::QueryRejection, Path, Query, State},
	Json,
};
use common::TimeRange;
use logql::parser;

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
		cache.insert(label_cache_key().to_string(), Arc::new(d));
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

fn series_cache_key() -> String {
	"cc:series".to_string()
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
		cache.insert(cache_key, Arc::new(d));
	}
	Ok(resp)
}

pub async fn query_series(
	State(state): State<AppState>,
	req: Result<Query<QuerySeriesRequest>, QueryRejection>,
) -> Result<Json<QuerySeriesResponse>, AppError> {
	let req = req
		.map_err(|e| AppError::InvalidQueryString(e.to_string()))?
		.0;
	if let Some(cached) = state.cache.get(&series_cache_key()) {
		return Ok(Json(QuerySeriesResponse {
			status: ResponseStatus::Success,
			data: serde_json::from_slice(&cached)?,
		}));
	}
	if req.matches.len() > 1 {
		return Err(AppError::MultiMatch(req.matches.len()));
	}
	let matches = if req.matches.is_empty() {
		None
	} else if let parser::Query::LogQuery(ql) =
		parser::parse_logql_query(req.matches[0].as_str())?
	{
		Some(ql)
	} else {
		None
	};
	let values = state
		.log_handle
		.series(
			matches,
			QueryLimits {
				limit: None,
				range: time_range_less_in_a_day(req.start, req.end),
				direction: None,
				step: None,
			},
		)
		.await?;
	if !values.is_empty() {
		let d = serde_json::to_vec(&values).unwrap();
		state.cache.insert(series_cache_key(), Arc::new(d));
	}
	Ok(Json(QuerySeriesResponse {
		status: ResponseStatus::Success,
		data: values,
	}))
}
