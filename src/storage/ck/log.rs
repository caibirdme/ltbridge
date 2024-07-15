use std::{collections::HashMap, vec};
use chrono::DateTime;
use ::clickhouse::{Client, Row};
use logql::parser::{LogQuery, MetricQuery};
use crate::storage::{log::*, *};
use async_trait::async_trait;
use sqlbuilder::{
	builder::{QueryPlan, TableSchema, time_range_into_timing},
	visit::{DefaultIRVisitor,LogQLVisitor},
};
use super::{
	converter::CKLogConverter,
	common::*,
};
use serde::{Deserialize, Serialize};
use common::LogLevel;

#[derive(Clone)]
pub struct CKLogQuerier {
    cli: Client,
	schema: LogTable,
}

impl CKLogQuerier {
    pub fn new(cli: Client, table: String) -> Self {
        Self { cli, schema: LogTable::new(table)}
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
		let mut cursor = self.cli.query(sql.as_str()).fetch::<LogRecod>()?;
		while let Some(r) = cursor.next().await? {
			results.push(r.into());
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
		let mut cursor = self.cli.query(sql.as_str()).fetch::<MetricRecord>()?;
		while let Some(r) = cursor.next().await? {
			results.push(r.into());
		}
        Ok(results)
	}
}

#[derive(Row, Debug, Deserialize)]
struct MetricRecord {
	#[serde(rename = "Tts")]
	ts: i64,
	#[serde(rename = "SeverityText")]
	severity_text: String,
	#[serde(rename = "Total")]
	total: u64,
}

impl From<MetricRecord> for MetricItem {
	fn from(r: MetricRecord) -> Self {
		Self {
			level: LogLevel::try_from(r.severity_text).unwrap_or(LogLevel::Trace),
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
	let v = LogQLVisitor::new(DefaultIRVisitor{});
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
		direction_to_sorting(&limits.direction, &schema),
		time_range_into_timing(&limits.range),
		limits.limit,
	);
	qp.as_sql()
}

fn logql_to_sql(q: &LogQuery, limits: QueryLimits, schema: &LogTable) -> String {
	let v = LogQLVisitor::new(DefaultIRVisitor{});
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

static LOG_TABLE_COLS: [&str; 13] = [
	"Timestamp",
	"TraceId",
	"SpanId",
	"SeverityText",
	"ServiceName",
	"Body",
	"ResourceSchemaUrl",
	"ResourceAttributes",
	"ScopeSchemaUrl",
	"ScopeName",
	"ScopeVersion",
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
#[derive(Row, Debug, Clone, Serialize, Deserialize)]
struct LogRecod {
	#[serde(rename = "Timestamp")]
	timestamp: i64,
	#[serde(rename = "TraceId")]
	trace_id: String,
	#[serde(rename = "SpanId")]
	span_id: String,
	#[serde(rename = "SeverityText")]
	severity_text: String,
	#[serde(rename = "SeverityNumber")]
	serverity_number: u32,
	#[serde(rename = "ServiceName")]
	service_name: String,
	#[serde(rename = "Body")]
	body: String,
	#[serde(rename = "ResourceAttributes")]
	resource_attr: HashMap<String, String>,
	#[serde(rename = "ScopeName")]
	scope_name: String,
	#[serde(rename = "ScopeAttributes")]
	scope_attributes: HashMap<String, String>,
	#[serde(rename = "LogAttributes")]
	log_attributes: HashMap<String, String>,

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
