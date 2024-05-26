use super::QueryLimits;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::NaiveDateTime;
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
}

dyn_clone::clone_trait_object!(LogStorage);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Copy)]
pub enum LogLevel {
	Trace,
	Debug,
	Info,
	Warn,
	Error,
	Fatal,
}

impl LogLevel {
	pub fn all_levels() -> Vec<String> {
		vec![
			LogLevel::Trace.into(),
			LogLevel::Debug.into(),
			LogLevel::Info.into(),
			LogLevel::Warn.into(),
			LogLevel::Error.into(),
			LogLevel::Fatal.into(),
		]
	}
}

impl TryFrom<String> for LogLevel {
	type Error = anyhow::Error;

	fn try_from(value: String) -> Result<Self> {
		use LogLevel::*;
		match value.to_uppercase().as_str() {
			"TRACE" => Ok(Trace),
			"DEBUG" => Ok(Debug),
			"INFO" => Ok(Info),
			"WARN" => Ok(Warn),
			"ERROR" => Ok(Error),
			"FATAL" => Ok(Fatal),
			_ => Err(anyhow!("Invalid log level: {}", value)),
		}
	}
}

impl From<u32> for LogLevel {
	fn from(l: u32) -> Self {
		use LogLevel::*;
		match l {
			..=4 => Trace,
			5..=8 => Debug,
			9..=12 => Info,
			13..=16 => Warn,
			17..=20 => Error,
			21.. => Fatal,
		}
	}
}

impl From<LogLevel> for u32 {
	fn from(val: LogLevel) -> u32 {
		use LogLevel::*;
		match val {
			Trace => 1,
			Debug => 5,
			Info => 9,
			Warn => 13,
			Error => 17,
			Fatal => 21,
		}
	}
}

impl From<LogLevel> for String {
	fn from(val: LogLevel) -> String {
		use LogLevel::*;
		match val {
			Trace => "TRACE".to_string(),
			Debug => "DEBUG".to_string(),
			Info => "INFO".to_string(),
			Warn => "WARN".to_string(),
			Error => "ERROR".to_string(),
			Fatal => "FATAL".to_string(),
		}
	}
}

#[derive(Debug, Clone)]
pub struct LogItem {
	pub app: String,
	pub server: String,
	pub trace_id: String,
	pub span_id: String,
	pub level: LogLevel,
	pub resources: HashMap<String, String>,
	pub attributes: HashMap<String, String>,
	pub message: String,
	pub ts: NaiveDateTime,
}

#[derive(Debug, Clone)]
pub struct MetricItem {
	pub level: LogLevel,
	pub total: u64,
	pub ts: NaiveDateTime,
}
