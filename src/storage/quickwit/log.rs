use super::{
	qwdsl::{self, Clause, Query, TermCtx, Unary},
	sdk::{self, *},
	QuickwitServerConfig,
};
use crate::storage::{log::*, *};
use anyhow::Result;
use async_trait::async_trait;
use chrono::DateTime;
use common::LogLevel;
use itertools::Itertools;
use lazy_static::lazy_static;
use logql::parser::{
	Filter, FilterType, LabelPair, LogLineFilter, LogQuery, MetricQuery,
	Operator,
};
use serde_json::Value as JSONValue;
use std::collections::HashMap;

static LABEL_ALIAS: [(&str, &str); 1] = [("severity_text", "level")];

lazy_static! {
	static ref LABEL_ALIAS_KV: HashMap<&'static str, &'static str> =
		LABEL_ALIAS.iter().cloned().collect();
	static ref LABEL_ALIAS_VK: HashMap<&'static str, &'static str> =
		LABEL_ALIAS.iter().map(|(k, v)| (*v, *k)).collect();
}

#[derive(Clone)]
pub struct QuickwitLog {
	schema: LogIndexMapping,
	cli: QuickwitSdk,
}

impl QuickwitLog {
	pub fn new(cfg: QuickwitServerConfig) -> Self {
		let cli = QuickwitSdk::new(cfg);
		QuickwitLog {
			schema: LogIndexMapping::default(),
			cli,
		}
	}
	fn log_query_to_dsl(&self, q: &LogQuery) -> Option<Query> {
		let query =
			q.selector
				.label_paris
				.iter()
				.fold(None, |acc, p| match acc {
					None => Some(Query::C(label_pair_to_unary(p))),
					Some(l) => {
						let r = qwdsl::Query::C(label_pair_to_unary(p));
						Some(Query::And(Box::new(l), Box::new(r)))
					}
				});
		match &q.filters {
			None => query,
			Some(filters) => filters
				.iter()
				.filter_map(|f| match f {
					Filter::Drop => None,
					Filter::LogLine(l) => Some(l),
				})
				.fold(query, |acc, p| match acc {
					None => Some(Query::C(loglinefilter_to_unary(p))),
					Some(l) => {
						let r = Query::C(loglinefilter_to_unary(p));
						Some(Query::And(Box::new(l), Box::new(r)))
					}
				}),
		}
	}
}

#[async_trait]
impl LogStorage for QuickwitLog {
	async fn query_stream(
		&self,
		q: &LogQuery,
		opt: QueryLimits,
	) -> Result<Vec<LogItem>> {
		let query = self.log_query_to_dsl(q);
		let query = build_search_query(query, opt, &self.schema.ts_key());
		let res = self.cli.search_records(&query).await?;
		if res.hits.is_empty() {
			return Ok(vec![]);
		}
		let records = res
			.hits
			.iter()
			.filter_map(|h| serde_json::from_value::<LogRecord>(h.clone()).ok())
			.map(record_to_logitem)
			.collect::<Vec<LogItem>>();
		Ok(records)
	}
	async fn query_metrics(
		&self,
		q: &MetricQuery,
		opt: QueryLimits,
	) -> Result<Vec<MetricItem>> {
		let query = self.log_query_to_dsl(&q.log_query);
		let interval = step_to_interval(q.range);
		let query = build_metric_query(query, opt);
		let resp = self
			.cli
			.level_aggregation(query, self.schema.ts_key(), interval)
			.await?;
		Ok(flatten_volume_agg_response(resp))
	}
	async fn labels(&self, opt: QueryLimits) -> Result<Vec<String>> {
		self.cli
			.field_caps(sdk::TimeRange {
				start: opt.range.start,
				end: opt.range.end,
			})
			.await
			.map(|labels| {
				labels
					.into_iter()
					.map(|k| field_alias_k_2_v(&k))
					.collect_vec()
			})
	}
	async fn label_values(
		&self,
		label: &str,
		opt: QueryLimits,
	) -> Result<Vec<String>> {
		let aliased_label = field_alias_v_2_k(label);
		self.cli
			.field_terms(
				&aliased_label,
				sdk::TimeRange {
					start: opt.range.start,
					end: opt.range.end,
				},
			)
			.await
	}
}

fn flatten_volume_agg_response(
	resp: sdk::VolumeAggrResponse,
) -> Vec<MetricItem> {
	resp.aggregations
		.volume
		.buckets
		.into_iter()
		.flat_map(|b| {
			let ts =
				DateTime::from_timestamp_millis(b.key.floor() as i64).unwrap();
			b.levels
				.buckets
				.into_iter()
				.map(|ib| MetricItem {
					ts,
					level: ib.key.try_into().unwrap_or(LogLevel::Trace),
					total: ib.doc_count as u64,
				})
				.collect_vec()
		})
		.collect()
}

#[derive(Debug, Clone)]
struct LogIndexMapping {
	ts: String,
}

impl LogIndexMapping {
	fn ts_key(&self) -> String {
		self.ts.clone()
	}
}

impl Default for LogIndexMapping {
	fn default() -> Self {
		LogIndexMapping {
			ts: "timestamp_nanos".to_string(),
		}
	}
}

fn build_metric_query(
	q: Option<qwdsl::Query>,
	limit: QueryLimits,
) -> sdk::SearcgRequest {
	SearcgRequest {
		query: q.unwrap_or_default().to_string(),
		max_hits: Some(0),
		start_timestamp: limit.range.start.map(|v| v.and_utc().timestamp()),
		end_timestamp: limit.range.end.map(|v| v.and_utc().timestamp()),
		sort_by: None,
		aggs: None,
	}
}

fn step_to_interval(step: Duration) -> String {
	let secs = step.as_secs();
	match secs {
		..=4 => "1s".to_string(),
		5..=9 => "5s".to_string(),
		10..=14 => "10s".to_string(),
		15..=29 => "15s".to_string(),
		30..=59 => "30s".to_string(),
		60..=299 => "1m".to_string(),
		300..=599 => "5m".to_string(),
		600..=899 => "10m".to_string(),
		900..=1799 => "15m".to_string(),
		1800..=3599 => "30m".to_string(),
		3600..=7199 => "1h".to_string(),
		7200..=10799 => "2h".to_string(),
		10800..=43199 => "3h".to_string(),
		43200..=86399 => "12h".to_string(),
		86400..=604799 => "1d".to_string(),
		604800.. => "7d".to_string(),
	}
}

fn build_search_query(
	q: Option<qwdsl::Query>,
	limit: QueryLimits,
	ts_key: &str,
) -> sdk::SearcgRequest {
	sdk::SearcgRequest {
		query: q.unwrap_or_default().to_string(),
		max_hits: limit.limit.map(Into::into),
		start_timestamp: limit.range.start.map(|v| v.and_utc().timestamp()),
		end_timestamp: limit.range.end.map(|v| v.and_utc().timestamp()),
		sort_by: limit.direction.map(|d| match d {
			Direction::Forward => SortBy {
				fields: vec![SortField {
					field: ts_key.to_string(),
					order: SortOrder::Asc,
				}],
			},
			Direction::Backward => SortBy {
				fields: vec![SortField {
					field: ts_key.to_string(),
					order: SortOrder::Desc,
				}],
			},
		}),
		..Default::default()
	}
}

fn record_to_logitem(r: LogRecord) -> LogItem {
	let level = get_level(&r);
	LogItem {
		ts: DateTime::from_timestamp_nanos(r.timestamp_nanos as i64),
		trace_id: r.trace_id.unwrap_or("".to_string()),
		span_id: r.span_id.unwrap_or("".to_string()),
		level: level.into(),
		service_name: r.service_name,
		resource_attributes: jsonmap_to_stringmap(r.resource_attributes),
		log_attributes: jsonmap_to_stringmap(r.attributes),
		message: r
			.body
			.map(|v| {
				v.get("message")
					.map(|v| v.to_string())
					.unwrap_or("".to_string())
			})
			.unwrap_or_default(),
		scope_name: r.scope_name.unwrap_or("".to_string()),
		scope_attributes: jsonmap_to_stringmap(r.scope_attributes),
	}
}

fn get_level(r: &LogRecord) -> LogLevel {
	if let Some(level_text) = &r.severity_text {
		if !level_text.is_empty() {
			return level_text.clone().try_into().unwrap_or(LogLevel::Trace);
		}
	}
	(r.severity_number as u32).into()
}

fn jsonmap_to_stringmap(
	m: HashMap<String, JSONValue>,
) -> HashMap<String, String> {
	m.into_iter()
		.map(|(k, v)| (k, v.to_string().trim_matches('"').to_owned()))
		.collect()
}

fn field_alias_k_2_v(f: &str) -> String {
	LABEL_ALIAS_KV
		.get(f)
		.map(|v| v.to_string())
		.unwrap_or(f.to_string())
}

fn field_alias_v_2_k(f: &str) -> String {
	LABEL_ALIAS_VK
		.get(f)
		.map(|v| v.to_string())
		.unwrap_or(f.to_string())
}

fn label_pair_to_unary(p: &LabelPair) -> Unary {
	match p.op {
		Operator::Equal => Unary::Pos(Clause::Term(TermCtx {
			field: field_alias_v_2_k(&p.label),
			value: JSONValue::String(p.value.clone()),
		})),
		Operator::NotEqual => Unary::Neg(Clause::Term(TermCtx {
			field: field_alias_v_2_k(&p.label),
			value: JSONValue::String(p.value.clone()),
		})),
		_ => unimplemented!("regexp is not supported yet"),
	}
}

fn loglinefilter_to_unary(p: &LogLineFilter) -> Unary {
	match p.op {
		FilterType::Contain => {
			Unary::Pos(Clause::Defaultable(p.expression.clone()))
		}
		FilterType::NotContain => {
			Unary::Neg(Clause::Defaultable(p.expression.clone()))
		}
		_ => unimplemented!("regexp is not supported yet"),
	}
}
