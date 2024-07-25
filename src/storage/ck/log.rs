use super::{common::*, converter::CKLogConverter};
use crate::config::Clickhouse;
use crate::storage::{log::*, *};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime};
use common::LogLevel;
use logql::parser::{LogQuery, MetricQuery};
use reqwest::Client;
use serde_json::Value as JSONValue;
use sqlbuilder::{
	builder::{time_range_into_timing, QueryPlan, TableSchema},
	visit::{DefaultIRVisitor, LogQLVisitor},
};
use std::collections::HashMap;

#[derive(Clone)]
pub struct CKLogQuerier {
	cli: Client,
	schema: LogTable,
	ck_cfg: Clickhouse,
}

impl CKLogQuerier {
	pub fn new(cli: Client, table: String, ck_cfg: Clickhouse) -> Self {
		Self {
			cli,
			// since we use http, we should use the full table name(database.table)
			schema: LogTable::new(format!("{}.{}", ck_cfg.database, table)),
			ck_cfg,
		}
	}
}

#[async_trait]
impl LogStorage for CKLogQuerier {
	async fn query_stream(
		&self,
		q: &LogQuery,
		opt: QueryLimits,
	) -> Result<Vec<LogItem>> {
		let sql = logql_to_sql(q, opt, &self.schema);
		let mut results = vec![];
		let rows =
			send_query(self.cli.clone(), self.ck_cfg.clone(), sql).await?;
		for row in rows {
			let record = LogRecod::try_from(row)?;
			results.push(record.into());
		}
		Ok(results)
	}
	async fn query_metrics(
		&self,
		q: &MetricQuery,
		opt: QueryLimits,
	) -> Result<Vec<MetricItem>> {
		let sql = new_from_metricquery(q, opt, self.schema.clone());
		let mut results = vec![];
		let rows =
			send_query(self.cli.clone(), self.ck_cfg.clone(), sql).await?;
		for row in rows {
			let record = MetricRecord::try_from(row)?;
			results.push(record.into());
		}
		Ok(results)
	}
}

#[derive(Debug)]
struct MetricRecord {
	ts: i64,
	severity_text: String,
	total: u64,
}

impl TryFrom<Vec<JSONValue>> for MetricRecord {
	type Error = CKConvertErr;
	fn try_from(
		value: Vec<JSONValue>,
	) -> std::result::Result<Self, Self::Error> {
		if value.len() != 3 {
			return Err(CKConvertErr::Length);
		}
		let ts = value[0].as_str().ok_or(CKConvertErr::Timestamp)?;
		let tts = NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S")
			.map_err(|e| {
				dbg!(e);
				CKConvertErr::Timestamp
			})?;

		let record = Self {
			ts: tts
				.and_utc()
				.timestamp_nanos_opt()
				.ok_or(CKConvertErr::Timestamp)?,
			severity_text: value[1].as_str().unwrap_or("").to_string(),
			total: value[2].as_str().unwrap_or("0").parse().unwrap_or(0),
		};
		Ok(record)
	}
}

impl From<MetricRecord> for MetricItem {
	fn from(r: MetricRecord) -> Self {
		Self {
			level: LogLevel::try_from(r.severity_text)
				.unwrap_or(LogLevel::Trace),
			total: r.total,
			ts: DateTime::from_timestamp_nanos(r.ts).naive_utc(),
		}
	}
}

fn new_from_metricquery(
	q: &MetricQuery,
	limits: QueryLimits,
	schema: LogTable,
) -> String {
	let v = LogQLVisitor::new(DefaultIRVisitor {});
	let selection = v.visit(&q.log_query);
	let step = limits.step.unwrap_or(DEFAULT_STEP);
	let qp = QueryPlan::new(
		CKLogConverter::new(schema.clone()),
		schema.clone(),
		vec![
			to_start_interval(step).to_string(),
			"SeverityText".to_string(),
			"count(*) as Total".to_string(),
		],
		selection,
		vec!["SeverityText".to_string(), "Tts".to_string()],
		vec![],
		time_range_into_timing(&limits.range),
		limits.limit,
	);
	qp.as_sql()
}

fn logql_to_sql(
	q: &LogQuery,
	limits: QueryLimits,
	schema: &LogTable,
) -> String {
	let v = LogQLVisitor::new(DefaultIRVisitor {});
	let selection = v.visit(q);
	let qp = QueryPlan::new(
		CKLogConverter::new(schema.clone()),
		schema.clone(),
		schema.projection(),
		selection,
		vec![],
		direction_to_sorting(&limits.direction, schema),
		time_range_into_timing(&limits.range),
		limits.limit,
	);
	qp.as_sql()
}

#[derive(Debug, Clone)]
pub(crate) struct LogTable {
	table: String,
}

impl LogTable {
	pub fn new(name: String) -> Self {
		Self { table: name }
	}
	fn projection(&self) -> Vec<String> {
		LOG_TABLE_COLS.iter().map(|s| s.to_string()).collect()
	}
}

static LOG_TABLE_COLS: [&str; 11] = [
	"Timestamp",
	"TraceId",
	"SpanId",
	"SeverityText",
	"SeverityNumber",
	"ServiceName",
	"Body",
	"ResourceAttributes",
	"ScopeName",
	"ScopeAttributes",
	"LogAttributes",
];

/*
	`Timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
	`TraceId` String CODEC(ZSTD(1)),
	`SpanId` String CODEC(ZSTD(1)),
	`SeverityText` LowCardinality(String) CODEC(ZSTD(1)),
	`SeverityNumber` Int32 CODEC(ZSTD(1)),
	`ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
	`Body` String CODEC(ZSTD(1)),
	`ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	`ScopeName` String CODEC(ZSTD(1)),
	`ScopeAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	`LogAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
*/
#[derive(Debug, Clone)]
struct LogRecod {
	timestamp: i64,
	trace_id: String,
	span_id: String,
	severity_text: String,
	serverity_number: u32,
	service_name: String,
	body: String,
	resource_attr: HashMap<String, String>,
	scope_name: String,
	scope_attributes: HashMap<String, String>,
	log_attributes: HashMap<String, String>,
}

impl TryFrom<Vec<JSONValue>> for LogRecod {
	type Error = CKConvertErr;
	fn try_from(
		value: Vec<JSONValue>,
	) -> std::result::Result<Self, Self::Error> {
		if value.len() != 11 {
			return Err(CKConvertErr::Length);
		}
		let ts = value[0].as_str().ok_or(CKConvertErr::Timestamp)?;
		let tts = NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S.%9f")
			.map_err(|_| CKConvertErr::Timestamp)?;

		let record = Self {
			timestamp: tts
				.and_utc()
				.timestamp_nanos_opt()
				.ok_or(CKConvertErr::Timestamp)?,
			trace_id: value[1].as_str().unwrap_or("").to_string(),
			span_id: value[2].as_str().unwrap_or("").to_string(),
			severity_text: value[3].as_str().unwrap_or("").to_string(),
			serverity_number: value[4].as_u64().unwrap_or(0) as u32,
			service_name: value[5].as_str().unwrap_or("").to_string(),
			body: value[6].as_str().unwrap_or("").to_string(),
			resource_attr: json_object_to_map_s_s(&value[7])?,
			scope_name: value[8].as_str().unwrap_or("").to_string(),
			scope_attributes: json_object_to_map_s_s(&value[9])?,
			log_attributes: json_object_to_map_s_s(&value[10])?,
		};
		Ok(record)
	}
}

impl From<LogRecod> for LogItem {
	fn from(r: LogRecod) -> Self {
		Self {
			ts: DateTime::from_timestamp_nanos(r.timestamp).naive_utc(),
			trace_id: r.trace_id,
			span_id: r.span_id,
			level: if r.severity_text.is_empty() {
				r.serverity_number.into()
			} else {
				LogLevel::try_from(r.severity_text).unwrap_or(LogLevel::Info)
			},
			service_name: r.service_name,
			message: r.body,
			resource_attributes: r.resource_attr,
			scope_name: r.scope_name,
			scope_attributes: r.scope_attributes,
			log_attributes: r.log_attributes,
		}
	}
}

impl TableSchema for LogTable {
	fn msg_key(&self) -> &str {
		"Body"
	}
	fn ts_key(&self) -> &str {
		"Timestamp"
	}
	fn table(&self) -> &str {
		self.table.as_str()
	}
	fn level_key(&self) -> &str {
		"SeverityNumber"
	}
	fn trace_key(&self) -> &str {
		"TraceId"
	}
	fn attributes_key(&self) -> &str {
		"SpanAttributes"
	}
	fn resources_key(&self) -> &str {
		"ResourceAttributes"
	}
}