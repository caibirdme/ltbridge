use super::{common::*, converter::CKLogConverter};
use crate::config::Clickhouse;
use crate::storage::trace::{Links, SpanEvent};
use crate::storage::{trace::*, *};
use anyhow::Result;
use async_trait::async_trait;
use chrono::DateTime;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use reqwest::Client;
use serde_json::Value as JSONValue;
use sqlbuilder::builder::*;
use sqlbuilder::builder::{time_range_into_timing, QueryPlan, TableSchema};
use std::collections::HashMap;
use traceql::*;

#[derive(Clone)]
pub struct CKTraceQuerier {
	client: Client,
	table: String,
	ck_cfg: Clickhouse,
}

impl CKTraceQuerier {
	pub fn new(client: Client, table: String, ck_cfg: Clickhouse) -> Self {
		Self {
			client,
			table,
			ck_cfg,
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
		let sql = traceid_query_sql(
			trace_id,
			opt,
			TraceTable::new(self.table.clone()),
		);
		let mut results = vec![];
		let rows =
			send_query(self.client.clone(), self.ck_cfg.clone(), sql).await?;
		for row in rows {
			let record = TraceRecord::try_from(row).map_err(|e| {
				dbg!(&e);
				e
			})?;
			results.push(record.into());
		}
		Ok(results)
	}
	async fn search_span(
		&self,
		_expr: &Expression,
		_opt: QueryLimits,
	) -> Result<Vec<SpanItem>> {
		Ok(vec![])
	}
}

fn traceid_query_sql(
	trace_id: &str,
	limits: QueryLimits,
	schema: TraceTable,
) -> String {
	let conds = vec![Condition {
		column: Column::TraceID,
		cmp: Cmp::Equal(PlaceValue::String(trace_id.to_string())),
	}];
	let selection = Some(conditions_into_selection(conds.as_slice()));
	let qp = QueryPlan::new(
		CKLogConverter::new(schema.clone()),
		schema.clone(),
		schema.projection(),
		selection,
		vec![],
		vec![],
		time_range_into_timing(&limits.range),
		limits.limit,
	);
	qp.as_sql()
}

#[derive(Clone)]
struct TraceTable {
	table: String,
}

impl TraceTable {
	pub fn new(table: String) -> Self {
		Self { table }
	}
	fn projection(&self) -> Vec<String> {
		TRACE_TABLE_COLS.iter().map(|s| s.to_string()).collect()
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
static TRACE_TABLE_COLS: [&str; 17] = [
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
	"Events",
	"Links",
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
	events: Vec<SpanEvent>,
	links: Vec<Links>,
}

impl TryFrom<Vec<JSONValue>> for TraceRecord {
	type Error = CKConvertErr;
	fn try_from(
		value: Vec<JSONValue>,
	) -> std::result::Result<Self, Self::Error> {
		if value.len() != 17 {
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
			events: value[15]
				.as_array()
				.ok_or(CKConvertErr::Array)?
				.iter()
				.map(|v| {
					let obj = v.as_object().ok_or(CKConvertErr::HashMap)?;
					let ts = obj
						.get("Timestamp")
						.ok_or(CKConvertErr::HashMap)?
						.as_str()
						.unwrap_or("");
					Ok(SpanEvent {
						ts: NaiveDateTime::parse_from_str(
							ts,
							"%Y-%m-%d %H:%M:%S.%9f",
						)
						.map_err(|_| CKConvertErr::Timestamp)?,
						dropped_attributes_count: 0,
						name: obj
							.get("Name")
							.ok_or(CKConvertErr::HashMap)?
							.as_str()
							.ok_or(CKConvertErr::HashMap)?
							.to_string(),
						attributes: obj
							.get("Attributes")
							.ok_or(CKConvertErr::HashMap)?
							.as_object()
							.ok_or(CKConvertErr::HashMap)?
							.into_iter()
							.map(|(k, v)| (k.clone(), v.clone()))
							.collect::<HashMap<String, JSONValue>>(),
					})
				})
				.collect::<Result<Vec<SpanEvent>, CKConvertErr>>()?,
			links: value[16]
				.as_array()
				.ok_or(CKConvertErr::Array)?
				.iter()
				.map(|v| {
					let obj = v.as_object().ok_or(CKConvertErr::HashMap)?;
					Ok(Links {
						trace_id: obj
							.get("TraceId")
							.ok_or(CKConvertErr::HashMap)?
							.as_str()
							.unwrap_or("")
							.to_string(),
						span_id: obj
							.get("SpanId")
							.ok_or(CKConvertErr::HashMap)?
							.as_str()
							.unwrap_or("")
							.to_string(),
						trace_state: obj
							.get("TraceState")
							.ok_or(CKConvertErr::HashMap)?
							.as_str()
							.unwrap_or("")
							.to_string(),
						attributes: obj
							.get("Attributes")
							.ok_or(CKConvertErr::HashMap)?
							.as_object()
							.ok_or(CKConvertErr::HashMap)?
							.into_iter()
							.map(|(k, v)| (k.clone(), v.clone()))
							.collect::<HashMap<String, JSONValue>>(),
					})
				})
				.collect::<Result<Vec<Links>, CKConvertErr>>()?,
		};
		Ok(record)
	}
}

impl From<TraceRecord> for SpanItem {
	fn from(value: TraceRecord) -> Self {
		Self {
			ts: DateTime::from_timestamp_nanos(value.timestamp).naive_utc(),
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
			status_code: value.status_code.parse().ok(),
			status_message: str_2_opt_str(&value.status_message),
			span_events: value.events,
			link: value.links,
		}
	}
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
