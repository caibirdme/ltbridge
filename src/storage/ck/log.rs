use super::{common::*, converter::CKLogConverter};
use crate::config::ClickhouseLog;
use crate::storage::{log::*, *};
use async_trait::async_trait;
use chrono::DateTime;
use common::LogLevel;
use logql::parser::{LogQuery, MetricQuery, Operator};
use reqwest::Client;
use serde_json::Value as JSONValue;
use sqlbuilder::{
	builder::{time_range_into_timing, QueryConverter, QueryPlan, TableSchema},
	visit::{DefaultIRVisitor, LogQLVisitor},
};
use std::{
	collections::HashMap,
	sync::{Arc, OnceLock},
};
use streamstore::{SeriesStore, StreamStore};
use tracing::error;

const TRACE_ID_NAME: &str = "trace_id";

static DEFAULT_LEVEL: OnceLock<String> = OnceLock::new();

#[derive(Clone)]
pub struct CKLogQuerier {
	cli: Client,
	schema: LogTable,
	ck_cfg: ClickhouseLog,
	meta: Arc<StreamStore>,
}

impl CKLogQuerier {
	pub fn new(cli: Client, table: String, ck_cfg: ClickhouseLog) -> Self {
		let lvl = ck_cfg.default_log_level.clone();
		_ = DEFAULT_LEVEL.set(lvl);
		Self {
			cli,
			schema: LogTable::new(format!(
				"{}.{}",
				ck_cfg.common.database, table
			)),
			ck_cfg,
			meta: StreamStore::new(),
		}
	}
	fn new_converter(&self) -> CKLogConverter<LogTable> {
		CKLogConverter::new(
			self.schema.clone(),
			self.ck_cfg.replace_dash_to_dot.unwrap_or(false),
			!self.ck_cfg.level_case_sensitive.unwrap_or(false),
		)
	}
}

#[async_trait]
impl LogStorage for CKLogQuerier {
	async fn query_stream(
		&self,
		q: &LogQuery,
		opt: QueryLimits,
	) -> Result<Vec<LogItem>> {
		let sql = logql_to_sql(q, opt, &self.schema, self.new_converter());
		let mut results = vec![];
		let rows =
			send_query(self.cli.clone(), self.ck_cfg.common.clone(), sql)
				.await
				.map_err(|e| {
					error!("Query log error: {:?}", e);
					e
				})?;
		for row in rows {
			let record = LogRecod::try_from(row).map_err(|e| {
				error!("Convert log record error: {:?}", e);
				e
			})?;
			results.push(record.into());
		}
		self.record_label(&results);
		Ok(results)
	}
	async fn query_metrics(
		&self,
		q: &MetricQuery,
		opt: QueryLimits,
	) -> Result<Vec<MetricItem>> {
		let sql = new_from_metricquery(
			q,
			opt,
			self.schema.clone(),
			self.new_converter(),
		);
		let mut results = vec![];
		let rows =
			send_query(self.cli.clone(), self.ck_cfg.common.clone(), sql)
				.await?;
		for row in rows {
			let record = MetricRecord::try_from(row)?;
			results.push(record.into());
		}
		Ok(results)
	}
	async fn labels(&self, _: QueryLimits) -> Result<Vec<String>> {
		let mut labels = self.meta.labels().unwrap_or_default();
		labels.push(TRACE_ID_NAME.to_string());
		Ok(labels)
	}
	async fn label_values(
		&self,
		label: &str,
		_: QueryLimits,
	) -> Result<Vec<String>> {
		if matches!(label.to_lowercase().as_str(), TRACE_ID_NAME | "traceid") {
			return Ok(vec!["your_trace_id".to_string()]);
		}
		Ok(self.meta.label_values(label).unwrap_or_default())
	}
	async fn series(
		&self,
		query: Option<LogQuery>,
		_opt: QueryLimits,
	) -> Result<Vec<HashMap<String, String>>> {
		let labels = match query {
			Some(q) => self.selector_to_labels(&q),
			None => HashMap::new(),
		};
		let mut series = self.meta.query(labels);
		for map in &mut series {
			map.insert(TRACE_ID_NAME.to_string(), "your_trace_id".into());
		}
		Ok(series)
	}
}

impl CKLogQuerier {
	pub async fn init_labels(&self) {
		let sql = format!(
			"SELECT {} FROM {} WHERE {} >= now() - INTERVAL 5 MINUTE LIMIT 3000",
			self.schema.projection().join(","),
			self.schema.table(),
			"TimestampTime".to_string(),
		);
		let rows =
			send_query(self.cli.clone(), self.ck_cfg.common.clone(), sql)
				.await
				.unwrap_or_default();
		let mut records = vec![];
		for row in rows {
			if let Ok(record) = LogRecod::try_from(row) {
				records.push(record.into());
			}
		}
		self.record_label(&records);
	}
	fn record_label(&self, records: &[LogItem]) {
		let cfg = self.ck_cfg.label.clone();
		let mut label_records = Vec::new();

		for record in records {
			let mut labels = HashMap::new();
			// Add service name
			labels
				.insert("ServiceName".to_string(), record.service_name.clone());
			// Add level
			labels.insert("level".to_string(), record.level.clone());

			// Add resource attributes
			if !cfg.resource_attributes.is_empty() {
				for key in &cfg.resource_attributes {
					if let Some(value) = record.resource_attributes.get(key) {
						labels.insert(
							format!("resources_{}", key),
							value.clone(),
						);
					}
				}
			}

			// Add log attributes
			if !cfg.log_attributes.is_empty() {
				for key in &cfg.log_attributes {
					if let Some(value) = record.log_attributes.get(key) {
						labels.insert(
							format!("attributes_{}", key),
							value.clone(),
						);
					}
				}
			}

			label_records.push(labels);
		}

		// Batch add all records to StreamStore
		self.meta.add(label_records);
	}

	fn selector_to_labels(&self, q: &LogQuery) -> HashMap<String, String> {
		q.selector
			.label_paris
			.iter()
			.filter_map(|v| {
				if v.op != Operator::Equal || !self.concerned_labels(&v.label) {
					return None;
				}
				Some((v.label.clone(), v.value.clone()))
			})
			.collect()
	}
	fn concerned_labels(&self, label: &String) -> bool {
		if matches!(label.to_lowercase().as_str(), "service_name" | "level") {
			return true;
		}
		self.ck_cfg.label.resource_attributes.contains(label)
			|| self.ck_cfg.label.log_attributes.contains(label)
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
		let tts = parse_timestamp_try_best(ts)
			.map_err(|_| CKConvertErr::Timestamp)?;

		let record = Self {
			ts: tts.timestamp_nanos_opt().ok_or(CKConvertErr::Timestamp)?,
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
			ts: DateTime::from_timestamp_nanos(r.ts),
		}
	}
}

fn new_from_metricquery(
	q: &MetricQuery,
	limits: QueryLimits,
	schema: LogTable,
	converter: impl QueryConverter,
) -> String {
	let v = LogQLVisitor::new(DefaultIRVisitor {});
	let selection = v.visit(&q.log_query);
	let step = limits.step.unwrap_or(DEFAULT_STEP);
	let qp = QueryPlan::new(
		converter,
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
	converter: impl QueryConverter,
) -> String {
	let v = LogQLVisitor::new(DefaultIRVisitor {});
	let selection = v.visit(q);
	let qp = QueryPlan::new(
		converter,
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
		let tts = parse_timestamp_try_best(ts)
			.map_err(|_| CKConvertErr::Timestamp)?;
		let record = Self {
			timestamp: tts
				.timestamp_nanos_opt()
				.ok_or(CKConvertErr::Timestamp)?,
			trace_id: value[1].as_str().unwrap_or("").to_string(),
			span_id: value[2].as_str().unwrap_or("").to_string(),
			severity_text: consistent_level(value[3].as_str().unwrap_or("")),
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

fn consistent_level(lvl: &str) -> String {
	match LogLevel::try_from(lvl) {
		Ok(l) => l.into(),
		Err(_) => DEFAULT_LEVEL.get().unwrap().clone(),
	}
}

impl From<LogRecod> for LogItem {
	fn from(r: LogRecod) -> Self {
		Self {
			ts: DateTime::from_timestamp_nanos(r.timestamp),
			trace_id: r.trace_id,
			span_id: r.span_id,
			level: r.severity_text,
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
		"SeverityText"
	}
	fn trace_key(&self) -> &str {
		"TraceId"
	}
	fn span_id_key(&self) -> &str {
		"SpanId"
	}
	fn attributes_key(&self) -> &str {
		"LogAttributes"
	}
	fn resources_key(&self) -> &str {
		"ResourceAttributes"
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use anyhow::Result;
	use pretty_assertions::assert_eq;
	#[test]
	fn test_decode_log_resp() -> Result<()> {
		// read json file from "./testdata/log.json"
		use std::fs;
		let v = fs::read_to_string("./testdata/ck/log_resp.json")?;
		let resp: RecordWarpper = serde_json::from_str(&v)?;
		assert_eq!(resp.data.len(), 1);
		for d in resp.data {
			let w: LogRecod = d.try_into()?;
			assert_eq!(w.trace_id, "2a4aa700ea743a8ffb5b1d1dde88fbe8");
		}
		Ok(())
	}
}
