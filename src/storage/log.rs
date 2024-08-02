use super::QueryLimits;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{offset::Utc, DateTime};
use common::LogLevel;
use dyn_clone::DynClone;
use logql::parser::{LogQuery, MetricQuery};
use std::collections::HashMap;

#[async_trait]
pub trait LogStorage: DynClone + Send + Sync {
	async fn query_stream(
		&self,
		q: &LogQuery,
		opt: QueryLimits,
	) -> Result<Vec<LogItem>>;
	async fn query_metrics(
		&self,
		q: &MetricQuery,
		opt: QueryLimits,
	) -> Result<Vec<MetricItem>>;
	async fn labels(&self, _opt: QueryLimits) -> Result<Vec<String>> {
		Ok(vec![])
	}
	async fn label_values(
		&self,
		_label: &str,
		_opt: QueryLimits,
	) -> Result<Vec<String>> {
		Ok(vec![])
	}
	async fn series(
		&self,
		_match: Option<LogQuery>,
		_opt: QueryLimits,
	) -> Result<Vec<HashMap<String, String>>> {
		Ok(vec![])
	}
}

dyn_clone::clone_trait_object!(LogStorage);

#[derive(Debug, Clone)]
pub struct LogItem {
	pub ts: DateTime<Utc>,
	pub trace_id: String,
	pub span_id: String,
	pub level: String,
	pub service_name: String,
	pub message: String,
	pub resource_attributes: HashMap<String, String>,
	pub scope_name: String,
	pub scope_attributes: HashMap<String, String>,
	pub log_attributes: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct MetricItem {
	pub level: LogLevel,
	pub total: u64,
	pub ts: DateTime<Utc>,
}
