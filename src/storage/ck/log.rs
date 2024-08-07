use super::{common::*, converter::CKLogConverter, labels::SeriesStore};
use crate::config::ClickhouseLog;
use crate::storage::{log::*, *};
use async_trait::async_trait;
use chrono::DateTime;
use common::LogLevel;
use logql::parser::{LogQuery, MetricQuery};
use reqwest::Client;
use serde_json::Value as JSONValue;
use sqlbuilder::{
	builder::{time_range_into_timing, QueryPlan, TableSchema},
	visit::{DefaultIRVisitor, LogQLVisitor},
};
use std::{
	collections::{HashMap, HashSet},
	sync::OnceLock,
};
use tokio::sync::mpsc::Sender;
use tracing::error;

const TRACE_ID_NAME: &str = "trace_id";

static DEFAULT_LEVEL: OnceLock<String> = OnceLock::new();

#[derive(Clone)]
pub struct CKLogQuerier {
	cli: Client,
	schema: LogTable,
	ck_cfg: ClickhouseLog,
	meta: SeriesStore,
	tx: Sender<(LabelType, String)>,
}

impl CKLogQuerier {
	pub fn new(cli: Client, table: String, ck_cfg: ClickhouseLog) -> Self {
		let lvl = ck_cfg.default_log_level.clone();
		_ = DEFAULT_LEVEL.set(lvl);
		let (meta, tx) = SeriesStore::new();
		Self {
			cli,
			// since we use http, we should use the full table name(database.table)
			schema: LogTable::new(format!(
				"{}.{}",
				ck_cfg.common.database, table
			)),
			ck_cfg,
			meta,
			tx,
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
		let sql = logql_to_sql(
			q,
			opt,
			&self.schema,
			self.ck_cfg.replace_dash_to_dot.unwrap_or(false),
		);
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
		self.record_label(&results).await;
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
			send_query(self.cli.clone(), self.ck_cfg.common.clone(), sql)
				.await?;
		for row in rows {
			let record = MetricRecord::try_from(row)?;
			results.push(record.into());
		}
		Ok(results)
	}
	async fn labels(&self, _: QueryLimits) -> Result<Vec<String>> {
		let mut arr: Vec<String> =
			self.meta.labels().into_iter().map(Into::into).collect();
		arr.push(TRACE_ID_NAME.to_string());
		Ok(arr)
	}
	async fn label_values(
		&self,
		label: &str,
		_: QueryLimits,
	) -> Result<Vec<String>> {
		if matches!(label.to_lowercase().as_str(), TRACE_ID_NAME | "traceid") {
			return Ok(vec!["your_trace_id".to_string()]);
		}
		if let Some(v) = self.meta.get(&label.into()) {
			Ok(v)
		} else {
			Ok(vec![])
		}
	}
	async fn series(
		&self,
		_match: Option<LogQuery>,
		_opt: QueryLimits,
	) -> Result<Vec<HashMap<String, String>>> {
		Ok(self
			.meta
			.series()
			.into_iter()
			.map(|v| {
				v.into_iter()
					.map(|(k, v)| (k.into(), v))
					.collect::<HashMap<String, String>>()
			})
			.map(|mut map| {
				map.insert(TRACE_ID_NAME.to_string(), "your_trace_id".into());
				map
			})
			.collect())
	}
}

impl CKLogQuerier {
	pub async fn init_labels(&self) {
		let sql = format!(
			"SELECT {} FROM {} WHERE {} >= now() - INTERVAL 5 MINUTE LIMIT 3000",
			self.schema.projection().join(","),
			self.schema.table(),
			self.schema.ts_key(),
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
		self.record_label(&records).await;
	}
	async fn record_label(&self, records: &[LogItem]) {
		let cfg = self.ck_cfg.label.clone();
		for name in Self::collect_svcname(records) {
			let _ = self.tx.send((LabelType::ServiceName, name.clone())).await;
		}
		for level in Self::collect_level(records) {
			let _ = self.tx.send((LabelType::Level, level)).await;
		}
		if !cfg.resource_attributes.is_empty() {
			for (k, vs) in Self::collect_attrs(
				&records
					.iter()
					.map(|r| &r.resource_attributes)
					.collect::<Vec<_>>(),
				&cfg.resource_attributes,
			) {
				for v in vs {
					let _ = self
						.tx
						.send((LabelType::ResourceAttr(k.clone()), v))
						.await;
				}
			}
		}
		if !cfg.log_attributes.is_empty() {
			for (k, vs) in Self::collect_attrs(
				&records
					.iter()
					.map(|r| &r.log_attributes)
					.collect::<Vec<_>>(),
				&cfg.log_attributes,
			) {
				for v in vs {
					let _ =
						self.tx.send((LabelType::LogAttr(k.clone()), v)).await;
				}
			}
		}
	}
	fn collect_svcname(records: &[LogItem]) -> Vec<String> {
		let set: HashSet<String> =
			records.iter().map(|r| r.service_name.clone()).collect();
		set.into_iter().collect()
	}
	fn collect_level(records: &[LogItem]) -> Vec<String> {
		let set: HashSet<String> =
			records.iter().map(|r| r.level.clone()).collect();
		set.into_iter().collect()
	}
	fn collect_attrs(
		records: &[&HashMap<String, String>],
		want_keys: &[String],
	) -> HashMap<String, Vec<String>> {
		let mut m = HashMap::new();
		for r in records {
			for k in want_keys {
				if let Some(v) = r.get(k) {
					m.entry(k).or_insert(HashSet::new()).insert(v);
				}
			}
		}
		m.into_iter()
			.map(|(k, v)| {
				(
					k.clone(),
					v.iter().cloned().cloned().collect::<Vec<String>>(),
				)
			})
			.collect()
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
) -> String {
	let v = LogQLVisitor::new(DefaultIRVisitor {});
	let selection = v.visit(&q.log_query);
	let step = limits.step.unwrap_or(DEFAULT_STEP);
	let qp = QueryPlan::new(
		CKLogConverter::new(schema.clone(), false),
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
	replace_dash: bool,
) -> String {
	let v = LogQLVisitor::new(DefaultIRVisitor {});
	let selection = v.visit(q);
	let qp = QueryPlan::new(
		CKLogConverter::new(schema.clone(), replace_dash),
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
		"SeverityNumber"
	}
	fn trace_key(&self) -> &str {
		"TraceId"
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
