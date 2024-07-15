use std::collections::HashMap;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use ::clickhouse::{Row,Client};
use anyhow::Result;
use async_trait::async_trait;
use sqlbuilder::builder::*;
use traceql::*;
use crate::storage::{trace::*, *};
use sqlbuilder::builder::{QueryPlan, TableSchema, time_range_into_timing};
use super::converter::CKLogConverter;

#[derive(Clone)]
pub struct CKTraceQuerier {
    client: Client,
    table: String,
}

impl CKTraceQuerier {
    pub fn new(client: Client, table: String) -> Self {
        Self { client, table }
    }
}

#[async_trait]
impl TraceStorage for CKTraceQuerier {
    async fn query_trace(
		&self,
		trace_id: &str,
		opt: QueryLimits,
	) -> Result<Vec<SpanItem>> {
        let sql = traceid_query_sql(trace_id, opt, TraceTable::new(self.table.clone()));
        let mut results = vec![];
        let mut cursor = self.client.query(sql.as_str()).fetch::<TraceRecord>()?;
        while let Some(r) = cursor.next().await? {
            results.push(r.into());
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

fn traceid_query_sql(trace_id: &str, limits: QueryLimits, schema: TraceTable) -> String {
    let conds = vec![Condition{
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

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
struct TraceRecord {
    #[serde(rename = "Timestamp")]
    timestamp: i64,
    #[serde(rename = "TraceId")]
    trace_id: String,
    #[serde(rename = "SpanId")]
    span_id: String,
    #[serde(rename = "ParentSpanId")]
    parent_span_id: String,
    #[serde(rename = "TraceState")]
    trace_state: String,
    #[serde(rename = "SpanName")]
    span_name: String,
    #[serde(rename = "SpanKind")]
    span_kind: String,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "ResourceAttributes")]
    resource_attributes: HashMap<String, String>,
    #[serde(rename = "ScopeName")]
    scope_name: String,
    #[serde(rename = "ScopeVersion")]
    scope_version: String,
    #[serde(rename = "SpanAttributes")]
    span_attributes: HashMap<String, String>,
    #[serde(rename = "Duration")]
    duration: i64,
    #[serde(rename = "StatusCode")]
    status_code: String,
    #[serde(rename = "StatusMessage")]
    status_message: String,
    #[serde(rename = "Events.Timestamp")]
    events_ts: Vec<i64>,
    #[serde(rename = "Events.Name")]
    events_name: Vec<String>,
    #[serde(rename = "Events.Attributes")]
    events_attributes: Vec<HashMap<String, String>>,
    #[serde(rename = "Links.TraceId")]
    links_trace_id: Vec<String>,
    #[serde(rename = "Links.SpanId")]
    links_span_id: Vec<String>,
    #[serde(rename = "Links.TraceState")]
    links_trace_state: Vec<String>,
    #[serde(rename = "Links.Attributes")]
    links_attributes: Vec<HashMap<String, String>>,
}

impl From<TraceRecord> for SpanItem {
    fn from(value: TraceRecord) -> Self {
        let mut events = vec![];
        for i in 0..value.events_ts.len() {
            let mut attrs = HashMap::new();
            for (k, v) in value.events_attributes[i].iter() {
                attrs.insert(k.clone(), serde_json::from_str(v).unwrap());
            }
            events.push(SpanEvent {
                ts: DateTime::from_timestamp_nanos(value.events_ts[i]).naive_utc(),
                dropped_attributes_count: 0,
                name: value.events_name[i].clone(),
                attributes: attrs,
            });
        }
        let mut links = vec![];
        for i in 0..value.links_trace_id.len() {
            let mut attrs = HashMap::new();
            for (k, v) in value.links_attributes[i].iter() {
                attrs.insert(k.clone(), serde_json::from_str(v).unwrap());
            }
            links.push(Links {
                trace_id: value.links_trace_id[i].clone(),
                span_id: value.links_span_id[i].clone(),
                trace_state: value.links_trace_state[i].clone(),
                attributes: attrs,
            });
        }
        Self {
            ts: DateTime::from_timestamp_nanos(value.timestamp).naive_utc(),
            trace_id: value.trace_id.clone(),
            span_id: value.span_id.clone(),
            parent_span_id: value.parent_span_id.clone(),
            trace_state: value.trace_state.clone(),
            span_name: value.span_name.clone(),
            span_kind: value.span_kind.parse().unwrap(),
            service_name: value.service_name.clone(),
            resource_attributes: hash_ss_2_hash_sj(value.resource_attributes),
            scope_name: str_2_opt_str(&value.scope_name),
            scope_version: str_2_opt_str(&value.scope_version),
            span_attributes: hash_ss_2_hash_sj(value.span_attributes),
            duration: value.duration,
            status_code: value.status_code.parse().ok(),
            status_message: str_2_opt_str(&value.status_message),
            span_events: events,
            link: links,
        }
    }
}

fn hash_ss_2_hash_sj(hash: HashMap<String, String>) -> HashMap<String, serde_json::Value> {
    hash.into_iter().map(|(k, v)| (k, serde_json::from_str(v.as_str()).unwrap())).collect()
}

fn str_2_opt_str(s: &String) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.clone())
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