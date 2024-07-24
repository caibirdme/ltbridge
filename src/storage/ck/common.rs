use std::{collections::HashMap, time::Duration};
use crate::storage::Direction;
use sqlbuilder::builder::{SortType, TableSchema};
use serde_json::Value as JSONValue;
use serde::Deserialize;
use crate::config::Clickhouse;
use reqwest::{Client, header::CONTENT_TYPE};
use anyhow::Result;
use thiserror::Error;

pub fn to_start_interval(step: Duration) -> &'static str {
	let sec = step.as_secs();
	let v = if sec < 5 {
		"toStartOfSecond(Timestamp) as Tts"
	} else if sec < 10 {
		"toStartOfInterval(Timestamp, INTERVAL 5 SECOND) as Tts"
	} else if sec < 15 {
		"toStartOfInterval(Timestamp, INTERVAL 10 SECOND) as Tts"
	} else if sec < 60 {
		"toStartOfInterval(Timestamp, INTERVAL 30 SECOND) as Tts"
	} else if sec < 5*60 {
		"toStartOfMinute(Timestamp) as Tts"
	} else if sec < 10*60 {
		"toStartOfFiveMinutes(Timestamp) as Tts"
	} else if sec < 30*60 {
		"toStartOfTenMinutes(Timestamp) as Tts"
	} else if sec < 60*60 {
		"toStartOfInterval(Timestamp, INTERVAL 30 MINUTE) as Tts"
	} else if sec < 2*60*60 {
		"toStartOfHour(Timestamp) as Tts"
	} else if sec < 24*60*60 {
		"toStartOfInterval(Timestamp, INTERVAL 2 HOUR) as Tts"
	} else if sec < 7*24*60*60 {
		"toStartOfDay(Timestamp) as Tts"
	} else if sec < 30*24*60*60 {
		// Set Monday is the first day of a week
		// https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#toweek
		"toStartOfWeek(Timestamp, 1) as Tts"
	} else if sec < 365*24*60*60 {
		"toStartOfMonth(Timestamp) as Tts"
	} else {
		"toStartOfYear(Timestamp) as Tts"
	};
	dbg!(step);
	dbg!(v);
	v
}

pub fn direction_to_sorting(
	d: &Option<Direction>,
	schema: &impl TableSchema,
) -> Vec<(String, SortType)> {
	let k = schema.ts_key();
	if let Some(d) = d {
		match d {
			Direction::Forward => vec![(k.to_string(), SortType::Asc)],
			Direction::Backward => vec![(k.to_string(), SortType::Desc)],
		}
	} else {
		vec![]
	}
}

#[derive(Debug, Deserialize)]
pub(crate) struct RecordWarpper {
	pub data: Vec<Vec<JSONValue>>
}

pub(crate) async fn send_query(cli: Client, cfg: Clickhouse, sql: String) -> Result<Vec<Vec<JSONValue>>> {
	dbg!(&sql);
	let req = cli.post(cfg.url.clone())
		.query(&[("default_format","JSONCompact"), ("add_http_cors_header", "1"), ("result_overflow_mode", "break"), ("max_result_rows", "1000"), ("max_result_bytes", "10000000")])
		.header(CONTENT_TYPE, "text/plain;charset=UTF-8")
		.body(sql)
		.basic_auth(cfg.username.clone(), Some(cfg.password.clone()))
		.build()?;
	let res = cli.execute(req).await?.text().await?;
	dbg!(&res);
	let resp: RecordWarpper = serde_json::from_str(&res)?;
	Ok(resp.data)
}

#[derive(Debug, Error)]
pub enum CKConvertErr {
	#[error("Invalid length")]
	InvalidLength,
	#[error("Invalid timestamp")]
	InvalidTimestamp,
	#[error("Invalid hashmap")]
	InvalidHashMap,
	#[error("Invalid duration")]
	InvalidDuration,
	#[error("Invalid array")]
	InvalidArray,

}

pub(crate) fn json_object_to_map_s_s(value: &JSONValue) -> std::result::Result<HashMap<String, String>, CKConvertErr> {
	value.as_object().map(|o| {
		o.iter().map(|(k, v)| {
			(k.clone(), v.as_str().unwrap_or("").to_string())
		}).collect()
	}).ok_or(CKConvertErr::InvalidHashMap)
}

pub(crate) fn json_object_to_map_s_jsonv(value: &JSONValue) -> std::result::Result<HashMap<String, JSONValue>, CKConvertErr> {
	value.as_object().map(|o| {
		o.iter().map(|(k, v)| {
			(k.clone(), v.clone())
		}).collect()
	}).ok_or(CKConvertErr::InvalidHashMap)
}
