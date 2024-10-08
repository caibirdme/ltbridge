use super::{common::*, converter::CKLogConverter};
use crate::config::ClickhouseTrace;
use crate::storage::trace::{Links, SpanEvent};
use crate::storage::{trace::*, *};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use itertools::izip;
use opentelemetry_proto::tonic::trace::v1::{
	span::SpanKind, status::StatusCode,
};
use reqwest::Client;
use serde_json::Value as JSONValue;
use sqlbuilder::{builder::TableSchema, trace::single_spanset_query};
use std::collections::HashMap;
use traceql::*;
use tracing::{error, warn};

#[derive(Clone)]
pub struct CKTraceQuerier {
	client: Client,
	ck_cfg: ClickhouseTrace,
	schema: TraceTable,
}

impl CKTraceQuerier {
	pub fn new(client: Client, table: String, ck_cfg: ClickhouseTrace) -> Self {
		Self {
			client,
			ck_cfg: ck_cfg.clone(),
			schema: TraceTable::new(
				table,
				ck_cfg.common.database,
				ck_cfg.trace_ts_table,
			),
		}
	}
}

#[async_trait]
impl TraceStorage for CKTraceQuerier {
	async fn query_trace(
		&self,
		trace_id: &str,
		opt: QueryLimits,
	) -> Result<Vec<SpanItem>> {
		let sql = traceid_query_sql(trace_id, opt, self.schema.clone());
		let mut results = vec![];
		let rows =
			send_query(self.client.clone(), self.ck_cfg.common.clone(), sql)
				.await
				.map_err(|e| {
					error!("Query trace error: {:?}", e);
					e
				})?;
		for row in rows {
			let record = TraceRecord::try_from(row).map_err(|e| {
				error!("Convert trace record error: {:?}", e);
				e
			})?;
			results.push(record.into());
		}
		Ok(results)
	}
	async fn search_span(
		&self,
		expr: &Expression,
		opt: QueryLimits,
	) -> Result<Vec<SpanItem>> {
		match expr {
			Expression::Logical(_, _, _) => {
				warn!("Search span does not support logical expression");
				return Ok(vec![]);
			}
			Expression::SpanSet(sp) => {
				let converter =
					CKLogConverter::new(self.schema.clone(), true, true);
				let sql = single_spanset_query(
					sp,
					self.schema.clone(),
					self.schema.projection(),
					opt.range,
					converter,
				);
				let mut results = vec![];
				let rows = send_query(
					self.client.clone(),
					self.ck_cfg.common.clone(),
					sql,
				)
				.await
				.map_err(|e| {
					error!("Query trace error: {:?}", e);
					e
				})?;
				for row in rows {
					let record = TraceRecord::try_from(row).map_err(|e| {
						error!("Convert trace record error: {:?}", e);
						e
					})?;
					results.push(record.into());
				}
				Ok(results)
			}
		}
	}
}

fn traceid_query_sql(
	trace_id: &str,
	_: QueryLimits,
	schema: TraceTable,
) -> String {
	let db = schema.database();
	let trace_ts_table = schema.trace_ts_table();
	let sql = format!(
		r#"
WITH
	'{}' as trace_id,
	(SELECT min(Start) FROM {}.{} WHERE TraceId = trace_id) as start,
	(SELECT max(End) + 1 FROM {}.{} WHERE TraceId = trace_id) as end
SELECT {} FROM {}
WHERE TraceId = trace_id
AND Timestamp >= start
AND Timestamp <= end
"#,
		trace_id,
		db,
		trace_ts_table,
		db,
		trace_ts_table,
		schema.projection().join(","),
		schema.table,
	);
	sql.replace("\n", " ").replace("\t", " ")
}

#[derive(Clone)]
struct TraceTable {
	table: String,
	database: String,
	trace_ts_table: String,
}

impl TraceTable {
	pub fn new(
		table: String,
		database: String,
		trace_ts_table: String,
	) -> Self {
		Self {
			table: format!("{}.{}", database, table),
			database,
			trace_ts_table,
		}
	}
	fn projection(&self) -> Vec<String> {
		TRACE_TABLE_COLS.iter().map(|s| s.to_string()).collect()
	}
	fn database(&self) -> &str {
		self.database.as_str()
	}
	fn trace_ts_table(&self) -> &str {
		self.trace_ts_table.as_str()
	}
}
/*
	 Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
	 TraceId String CODEC(ZSTD(1)),
	 SpanId String CODEC(ZSTD(1)),
	 ParentSpanId String CODEC(ZSTD(1)),
	 TraceState String CODEC(ZSTD(1)),
	 SpanName LowCardinality(String) CODEC(ZSTD(1)),
	 SpanKind LowCardinality(String) CODEC(ZSTD(1)),
	 ServiceName LowCardinality(String) CODEC(ZSTD(1)),
	 ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	 ScopeName String CODEC(ZSTD(1)),
	 ScopeVersion String CODEC(ZSTD(1)),
	 SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
	 Duration Int64 CODEC(ZSTD(1)),
	 StatusCode LowCardinality(String) CODEC(ZSTD(1)),
	 StatusMessage String CODEC(ZSTD(1)),
	 Events Nested (
		 Timestamp DateTime64(9),
		 Name LowCardinality(String),
		 Attributes Map(LowCardinality(String), String)
	 ) CODEC(ZSTD(1)),
	 Links Nested (
		 TraceId String,
		 SpanId String,
		 TraceState String,
		 Attributes Map(LowCardinality(String), String)
	 ) CODEC(ZSTD(1))
*/
static TRACE_TABLE_COLS: [&str; 22] = [
	"Timestamp",
	"TraceId",
	"SpanId",
	"ParentSpanId",
	"TraceState",
	"SpanName",
	"SpanKind",
	"ServiceName",
	"ResourceAttributes",
	"ScopeName",
	"ScopeVersion",
	"SpanAttributes",
	"Duration",
	"StatusCode",
	"StatusMessage",
	"Events.Timestamp",
	"Events.Name",
	"Events.Attributes",
	"Links.TraceId",
	"Links.SpanId",
	"Links.TraceState",
	"Links.Attributes",
];

#[derive(Debug)]
struct TraceRecord {
	timestamp: i64,
	trace_id: String,
	span_id: String,
	parent_span_id: String,
	trace_state: String,
	span_name: String,
	span_kind: String,
	service_name: String,
	resource_attributes: HashMap<String, JSONValue>,
	scope_name: String,
	scope_version: String,
	span_attributes: HashMap<String, JSONValue>,
	duration: i64,
	status_code: String,
	status_message: String,
	events_ts: Vec<DateTime<Utc>>,
	events_name: Vec<String>,
	events_attrs: Vec<HashMap<String, JSONValue>>,
	links_trace_id: Vec<String>,
	links_span_id: Vec<String>,
	links_trace_state: Vec<String>,
	links_attrs: Vec<HashMap<String, JSONValue>>,
}

impl TryFrom<Vec<JSONValue>> for TraceRecord {
	type Error = CKConvertErr;
	fn try_from(
		value: Vec<JSONValue>,
	) -> std::result::Result<Self, Self::Error> {
		if value.len() != 22 {
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
			parent_span_id: value[3].as_str().unwrap_or("").to_string(),
			trace_state: value[4].as_str().unwrap_or("").to_string(),
			span_name: value[5].as_str().unwrap_or("").to_string(),
			span_kind: value[6].as_str().unwrap_or("").to_string(),
			service_name: value[7].as_str().unwrap_or("").to_string(),
			resource_attributes: json_object_to_map_s_jsonv(&value[8])?,
			scope_name: value[9].as_str().unwrap_or("").to_string(),
			scope_version: value[10].as_str().unwrap_or("").to_string(),
			span_attributes: json_object_to_map_s_jsonv(&value[11])?,
			duration: value[12]
				.as_str()
				.unwrap_or("0")
				.parse()
				.map_err(|_| CKConvertErr::Duration)?,
			status_code: value[13].as_str().unwrap_or("").to_string(),
			status_message: value[14].as_str().unwrap_or("").to_string(),
			events_ts: json_array_to_date(&value[15])?,
			events_name: json_array_string(&value[16])?,
			events_attrs: json_array_hashmap(&value[17])?,
			links_trace_id: json_array_string(&value[18])?,
			links_span_id: json_array_string(&value[19])?,
			links_trace_state: json_array_string(&value[20])?,
			links_attrs: json_array_hashmap(&value[21])?,
		};
		Ok(record)
	}
}

impl From<TraceRecord> for SpanItem {
	fn from(value: TraceRecord) -> Self {
		Self {
			ts: DateTime::from_timestamp_nanos(value.timestamp),
			trace_id: value.trace_id.clone(),
			span_id: value.span_id.clone(),
			parent_span_id: value.parent_span_id.clone(),
			trace_state: value.trace_state.clone(),
			span_name: value.span_name.clone(),
			span_kind: SpanKind::from_str_name(&value.span_kind)
				.unwrap_or(SpanKind::Unspecified)
				.into(),
			service_name: value.service_name.clone(),
			resource_attributes: value.resource_attributes,
			scope_name: str_2_opt_str(&value.scope_name),
			scope_version: str_2_opt_str(&value.scope_version),
			span_attributes: value.span_attributes,
			duration: value.duration,
			// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/internal/coreinternal/traceutil/traceutil.go#L37
			// collector sets status_code as "STATUS_CODE_OK" rather than its corresponding number
			status_code: parse_status_code(&value.status_code),
			status_message: str_2_opt_str(&value.status_message),
			span_events: izip!(
				value.events_ts,
				value.events_name,
				value.events_attrs
			)
			.map(|(ts, name, attributes)| SpanEvent {
				ts,
				dropped_attributes_count: 0,
				name,
				attributes,
			})
			.collect(),
			link: izip!(
				value.links_trace_id,
				value.links_span_id,
				value.links_trace_state,
				value.links_attrs
			)
			.map(|(trace_id, span_id, trace_state, attributes)| Links {
				trace_id,
				span_id,
				trace_state,
				attributes,
			})
			.collect(),
		}
	}
}

static STATUS_CODE_STR: [StatusCode; 3] =
	[StatusCode::Unset, StatusCode::Ok, StatusCode::Error];

fn parse_status_code(s: &str) -> Option<i32> {
	STATUS_CODE_STR
		.iter()
		.find(|k| (*k).as_str_name() == s)
		.map(|k| (*k).into())
		.or(Some(StatusCode::Unset.into()))
}

fn str_2_opt_str(s: &str) -> Option<String> {
	if s.is_empty() {
		None
	} else {
		Some(s.to_owned())
	}
}

impl TableSchema for TraceTable {
	fn msg_key(&self) -> &str {
		"Body"
	}
	fn ts_key(&self) -> &str {
		"Timestamp"
	}
	fn table(&self) -> &str {
		&self.table
	}
	fn level_key(&self) -> &str {
		"SeverityNumber"
	}
	fn trace_key(&self) -> &str {
		"TraceId"
	}
	fn span_id_key(&self) -> &str {
		"SpanId"
	}
	fn attributes_key(&self) -> &str {
		"SpanAttributes"
	}
	fn resources_key(&self) -> &str {
		"ResourceAttributes"
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use pretty_assertions::assert_eq;
	use sqlparser::{dialect::ClickHouseDialect, parser::Parser};
	use std::{fs, path::PathBuf};
	use traceql::parse_traceql;

	#[test]
	fn expand_complex_traceql() {
		let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
		d.push("src/storage/ck/traceql_test.yaml");
		let test_cases = fs::read_to_string(d).unwrap();

		#[derive(serde::Deserialize)]
		struct TestCase {
			input: String,
			expect: String,
		}
		let cases: HashMap<String, TestCase> =
			serde_yaml::from_str(&test_cases).unwrap();
		let schema = TraceTable::new(
			"otel_traces".to_string(),
			"otlp".to_string(),
			"xx".to_string(),
		);
		for (name, tc) in cases {
			let expr = parse_traceql(&tc.input).unwrap();
			if let Expression::SpanSet(sp) = expr {
				let converter = CKLogConverter::new(schema.clone(), true, true);
				let sql = single_spanset_query(
					&sp,
					schema.clone(),
					schema.projection(),
					common::TimeRange::default(),
					converter,
				);
				let actual_ast =
					Parser::parse_sql(&ClickHouseDialect {}, &sql).unwrap();
				let expect_ast =
					Parser::parse_sql(&ClickHouseDialect {}, &tc.expect)
						.unwrap();
				assert_eq!(
					expect_ast[0].to_string(),
					actual_ast[0].to_string(),
					"case: {}",
					name
				);
			}
		}
	}
}
