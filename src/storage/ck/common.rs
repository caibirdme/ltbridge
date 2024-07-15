use std::time::Duration;
use crate::storage::Direction;
use sqlbuilder::builder::{SortType, TableSchema};

pub fn to_start_interval(step: Duration) -> &'static str {
	let sec = step.as_secs();
	if sec < 5 {
		"toStartOfSecond(Timestamp) as Tts"
	} else if sec < 10 {
		"toStartOfInterval(Timestamp, INTERVAL 5 SECOND) as Tts"
	} else if sec < 15 {
		"toStartOfInterval(Timestamp, INTERVAL 10 SECOND) as Tts"
	} else if sec < 60 {
		"toStartOfInterval(Timestamp, INTERVAL 30 SECOND) as Tts"
	} else if sec < 5*60 {
		"toStartOfTenMinutes(Timestamp) as Tts"
	} else if sec < 10*60 {
		"toStartOfFiveMinutes(Timestamp) as Tts"
	} else if sec < 15*60 {
		"toStartOfTenMinutes(Timestamp) as Tts"
	} else if sec < 30*60 {
		"toStartOfFifteenMinutes(Timestamp) as Tts"
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
	}
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