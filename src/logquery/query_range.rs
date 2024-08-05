use super::*;
use crate::{
	errors::AppError,
	state::AppState,
	storage::log::{LogItem, MetricItem},
};
use axum::extract::{Query, State};
use axum_valid::Valid;
use common::LogLevel;
use itertools::Itertools;
use logql::parser;
use moka::sync::Cache;
use std::collections::HashMap;

pub async fn query_range(
	State(state): State<AppState>,
	Valid(Query(req)): Valid<Query<QueryRangeRequest>>,
) -> Result<QueryRangeResponse, AppError> {
	let cache_key = serde_json::to_string(&req).unwrap();
	if let Some(resp) = get_cached_query(&cache_key, state.cache.clone()) {
		return Ok(resp);
	}
	// parse the logql query and convert the logql query to databend sql
	let ql = parser::parse_logql_query(req.query.as_str())?;
	let resp = match ql {
		parser::Query::LogQuery(ql) => {
			handle_log_query(ql, req, state.clone()).await
		}
		parser::Query::MetricQuery(mq) => {
			handle_metric_query(mq, req, state.clone()).await
		}
	};
	if let Ok(inner) = &resp {
		let d = serde_json::to_vec(inner).unwrap();
		state.cache.insert(cache_key, d);
	}
	resp
}

pub async fn loki_is_working() -> Result<QueryRangeResponse, AppError> {
	let now = Utc::now().timestamp();
	Ok(QueryRangeResponse {
		status: ResponseStatus::Success,
		data: QueryResult::Vector(VectorResponse {
			result_type: ResultType::Vector,
			result: vec![VectorValue {
				metric: HashMap::new(),
				value: [now.into(), "2".to_string().into()],
			}],
		}),
	})
}

fn get_cached_query(
	key: &str,
	cache: Cache<String, Vec<u8>>,
) -> Option<QueryRangeResponse> {
	if let Some(v) = cache.get(key) {
		if let Ok(d) = serde_json::from_slice(&v) {
			return Some(d);
		}
	}
	None
}

/*
SELECT
  LEVEL,
  TO_START_OF_DAY(ts) AS nts,
  count(*) AS total
FROM
  LOGS
GROUP BY
  LEVEL,
  nts
ORDER BY
  nts
*/
async fn handle_metric_query(
	mq: parser::MetricQuery,
	req: QueryRangeRequest,
	state: AppState,
) -> Result<QueryRangeResponse, AppError> {
	let handle = state.log_handle;
	let rows = handle.query_metrics(&mq, req.into()).await?;
	Ok(to_metric_query_range_response(&rows))
}

async fn handle_log_query(
	ql: parser::LogQuery,
	mut req: QueryRangeRequest,
	state: AppState,
) -> Result<QueryRangeResponse, AppError> {
	const DEFAULT_LIMIT: u32 = 1000;
	let handle = state.log_handle;
	if req.limit.is_none() {
		req.limit = Some(DEFAULT_LIMIT);
	}
	let rows = handle.query_stream(&ql, req.into()).await?;
	let (resp, _) = to_log_query_range_response(&rows);
	Ok(resp)
}

fn to_metric_query_range_response(value: &[MetricItem]) -> QueryRangeResponse {
	let matrix = value
		.iter()
		.into_group_map_by(|v| v.level)
		.iter()
		.map(|(level, elements)| MatrixValue {
			metric: HashMap::from_iter(vec![(
				"level".to_string(),
				(*level).into(),
			)]),
			values: elements
				.iter()
				.map(|e| [e.ts.timestamp().into(), e.total.to_string().into()])
				.collect(),
		})
		.collect();
	QueryRangeResponse {
		status: ResponseStatus::Success,
		data: QueryResult::Matrix(MatrixResponse {
			result_type: ResultType::Matrix,
			result: matrix,
		}),
	}
}

fn to_log_query_range_response(
	value: &[LogItem],
) -> (QueryRangeResponse, Vec<HashMap<String, String>>) {
	let mut tag_list = vec![];
	let streams = value
		.iter()
		.filter_map(|r| {
			let lvl = LogLevel::try_from(r.level.clone());
			if lvl.is_err() {
				return None;
			}
			let mut tags = HashMap::from_iter(vec![
				("ServiceName".to_string(), r.service_name.clone()),
				("TraceId".to_string(), r.trace_id.clone()),
				("SpanId".to_string(), r.span_id.clone()),
				("SeverityText".to_string(), r.level.clone()),
				// fix: https://github.com/grafana/loki/pull/12651
				("level".to_string(), lvl.unwrap().into()),
			]);
			if !r.scope_name.is_empty() {
				tags.insert("scope_name".to_string(), r.scope_name.clone());
			}
			r.resource_attributes.iter().for_each(|(k, v)| {
				tags.insert(format!("resources.{}", k), v.clone());
			});
			r.scope_attributes.iter().for_each(|(k, v)| {
				tags.insert(format!("scopes.{}", k), v.clone());
			});
			r.log_attributes.iter().for_each(|(k, v)| {
				tags.insert(format!("attributes.{}", k), v.clone());
			});
			tag_list.push(tags.clone());
			Some(StreamValue {
				stream: tags,
				values: vec![[
					r.ts.timestamp_nanos_opt().unwrap().to_string(),
					r.message.clone(),
				]],
			})
		})
		.collect();
	(
		QueryRangeResponse {
			status: ResponseStatus::Success,
			data: QueryResult::Streams(StreamResponse {
				result_type: ResultType::Streams,
				result: streams,
			}),
		},
		tag_list,
	)
}
