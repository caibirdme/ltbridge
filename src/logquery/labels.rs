use std::time::Duration;

use crate::{errors::AppError, state::AppState, storage::QueryLimits};
use axum::{
	extract::{rejection::QueryRejection, Path, Query, State},
	Json,
};
use chrono::Utc;
use common::TimeRange;
use logql::parser;

use super::{
	QueryLabelValuesRequest, QueryLabelsRequest, QueryLabelsResponse,
	QuerySeriesRequest, QuerySeriesResponse, ResponseStatus,
};

pub async fn query_labels(
	State(state): State<AppState>,
	_: Query<QueryLabelsRequest>,
) -> Result<QueryLabelsResponse, AppError> {
	let labels = state
		.log_handle
		.labels(QueryLimits {
			limit: None,
			range: t_hours_before(2),
			direction: None,
			step: None,
		})
		.await?;
	let resp = QueryLabelsResponse {
		status: ResponseStatus::Success,
		data: labels,
	};
	Ok(resp)
}

pub async fn query_label_values(
	State(state): State<AppState>,
	Path(label): Path<String>,
	_: Query<QueryLabelValuesRequest>,
) -> Result<QueryLabelsResponse, AppError> {
	let values = state
		.log_handle
		.label_values(
			&label,
			QueryLimits {
				limit: None,
				range: t_hours_before(2),
				direction: None,
				step: None,
			},
		)
		.await?;
	let resp = QueryLabelsResponse {
		status: ResponseStatus::Success,
		data: values,
	};
	Ok(resp)
}

pub async fn query_series(
	State(state): State<AppState>,
	req: Result<Query<QuerySeriesRequest>, QueryRejection>,
) -> Result<Json<QuerySeriesResponse>, AppError> {
	let req = req
		.map_err(|e| AppError::InvalidQueryString(e.to_string()))?
		.0;
	let matches = if let parser::Query::LogQuery(lq) =
		parser::parse_logql_query(req.matches.as_str())?
	{
		lq
	} else {
		return Err(AppError::InvalidQueryString(req.matches));
	};
	// if no label pairs, client should not call this api
	// instead, it should call query_labels
	if matches.selector.label_paris.is_empty() {
		return Err(AppError::InvalidQueryString(
			req.matches.as_str().to_string(),
		));
	}

	let series = state
		.log_handle
		.series(
			Some(matches),
			QueryLimits {
				limit: None,
				range: t_hours_before(2),
				direction: None,
				step: None,
			},
		)
		.await?;

	Ok(Json(QuerySeriesResponse {
		status: ResponseStatus::Success,
		data: series,
	}))
}

fn t_hours_before(hours: u64) -> TimeRange {
	let start = Utc::now() - Duration::from_secs(hours * 60 * 60);
	TimeRange {
		start: Some(start.naive_utc()),
		end: None,
	}
}
