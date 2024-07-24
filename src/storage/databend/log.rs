use super::converter::DatabendLogConverter;
use crate::storage::{log::*, *};
use anyhow::Result;
use async_trait::async_trait;
use chrono::NaiveDateTime;
use databend_driver::{Connection, Row, TryFromRow};
use logql::parser::{LogQuery, MetricQuery};
use sqlbuilder::builder::*;
use sqlbuilder::{
	builder::QueryPlan,
	visit::{DefaultIRVisitor, LogQLVisitor},
};
use std::{collections::HashMap, time::Duration};
use tokio_stream::StreamExt;

const DEFAULT_STEP: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct BendLogQuerier {
	cli: Box<dyn Connection>,
	schema: LogTable,
}

impl BendLogQuerier {
	pub fn new(cli: Box<dyn Connection>) -> Self {
		Self {
			cli,
			schema: LogTable::default(),
		}
	}
	pub fn with_inverted_index(&mut self, open: bool) {
		self.schema.use_inverted_index = open;
	}
}

#[async_trait]
impl LogStorage for BendLogQuerier {
	async fn query_stream(
		&self,
		q: &LogQuery,
		opt: QueryLimits,
	) -> Result<Vec<LogItem>> {
		let sql = logql_to_sql(q, opt, &self.schema);
		let mut logs = vec![];
		let mut stream = self.cli.query_iter(&sql).await?;
		while let Some(row) = stream.next().await {
			let row = row?;
			let item = row_into_logitem(row)?;
			logs.push(item);
		}
		Ok(logs)
	}
	async fn query_metrics(
		&self,
		q: &MetricQuery,
		opt: QueryLimits,
	) -> Result<Vec<MetricItem>> {
		let v = LogQLVisitor::new(DefaultIRVisitor {});
		let selection = v.visit(&q.log_query);
		let qp = new_from_metricquery(opt, self.schema.clone(), selection);
		let sql = qp.as_sql();
		let mut stream = self.cli.query_iter(&sql).await?;
		let mut metrics = vec![];
		while let Some(row) = stream.next().await {
			let row = row?;
			let (level, nts, total): (u32, NaiveDateTime, u64) =
				row.try_into().map_err(|e: String| anyhow::anyhow!(e))?;
			metrics.push(MetricItem {
				level: level.into(),
				total,
				ts: nts,
			});
		}
		Ok(metrics)
	}
	async fn labels(&self, _: QueryLimits) -> Result<Vec<String>> {
		Ok(vec![])
	}
	async fn label_values(
		&self,
		_: &str,
		_: QueryLimits,
	) -> Result<Vec<String>> {
		Ok(vec![])
	}
}

fn logql_to_sql(
	q: &LogQuery,
	limits: QueryLimits,
	schema: &LogTable,
) -> String {
	let v = LogQLVisitor::new(DefaultIRVisitor {});
	let selection = v.visit(q);
	let qp = QueryPlan::new(
		DatabendLogConverter::new(schema.clone()),
		schema.clone(),
		schema.projection(),
		selection,
		vec![],
		direction_to_sorting(&limits.direction, schema, false),
		time_range_into_timing(&limits.range),
		limits.limit,
	);
	qp.as_sql()
}

#[derive(Debug, Default, Clone, TryFromRow)]
struct LogRaw {
	pub ts: NaiveDateTime,
	pub trace_id: String,
	pub span_id: String,
	pub level: u32,
	pub service_name: String,
	pub message: String,
	pub resource_attributes: HashMap<String, String>,
	pub scope_name: String,
	pub scope_attributes: HashMap<String, String>,
	pub log_attributes: HashMap<String, String>,
}

fn row_into_logitem(row: Row) -> Result<LogItem> {
	let row: LogRaw = row.try_into().map_err(|e: String| anyhow::anyhow!(e))?;
	Ok(LogItem {
		ts: row.ts,
		trace_id: row.trace_id,
		span_id: row.span_id,
		level: row.level.into(),
		service_name: row.service_name,
		message: row.message,
		resource_attributes: row.resource_attributes,
		scope_name: row.scope_name,
		scope_attributes: row.scope_attributes,
		log_attributes: row.log_attributes,
	})
}

/*
	CREATE TABLE logs (
		service_name STRING NOT NULL,
		trace_id STRING,
		span_id STRING,
		level TINYINT,
		resource_attributes MAP(STRING, STRING) NOT NULL,
		scope_name STRING,
		scope_attributes MAP(STRING, STRING),
		log_attributes MAP(STRING, STRING) NOT NULL,
		message STRING NOT NULL,
		ts TIMESTAMP NOT NULL
	) ENGINE=FUSE CLUSTER BY(TO_YYYYMMDDHH(ts), server);
	CREATE INVERTED INDEX message_idx ON logs(message);
*/
#[derive(Debug, Clone)]
pub(crate) struct LogTable {
	pub use_inverted_index: bool,
	msg_key: &'static str,
	ts_key: &'static str,
	table: &'static str,
	level: &'static str,
	trace_id: &'static str,
}

impl Default for LogTable {
	fn default() -> Self {
		Self {
			use_inverted_index: false,
			msg_key: "message",
			ts_key: "timestamp",
			table: "logs",
			level: "level",
			trace_id: "trace_id",
		}
	}
}

impl TableSchema for LogTable {
	fn table(&self) -> &str {
		self.table
	}
	fn ts_key(&self) -> &str {
		self.ts_key
	}
	fn msg_key(&self) -> &str {
		self.msg_key
	}
	fn level_key(&self) -> &str {
		self.level
	}
	fn trace_key(&self) -> &str {
		self.trace_id
	}
	fn resources_key(&self) -> &str {
		"resources"
	}
	fn attributes_key(&self) -> &str {
		"attributes"
	}
}

impl LogTable {
	fn projection(&self) -> Vec<String> {
		vec![
			"app",
			"server",
			"trace_id",
			"span_id",
			"level",
			"tags",
			self.msg_key,
			self.ts_key,
		]
		.into_iter()
		.map(Into::into)
		.collect()
	}
	fn revised_ts_key(&self) -> &str {
		"nts"
	}
}

fn new_from_metricquery(
	limits: QueryLimits,
	schema: LogTable,
	selection: Option<Selection>,
) -> QueryPlan<LogTable, DatabendLogConverter> {
	let (projection, grouping) = metrics_projection_and_grouping(
		&schema,
		limits.step.unwrap_or(DEFAULT_STEP),
	);
	QueryPlan::new(
		DatabendLogConverter::new(schema.clone()),
		schema.clone(),
		projection,
		selection,
		grouping,
		direction_to_sorting(&limits.direction, &schema, true),
		time_range_into_timing(&limits.range),
		limits.limit,
	)
}

fn metrics_projection_and_grouping(
	schema: &LogTable,
	step: Duration,
) -> (Vec<String>, Vec<String>) {
	let projection = vec![
		"level".to_string(),
		format!("{} as nts", truncate_ts(step, schema.ts_key())),
		"count(*) as total".to_string(),
	];
	let grouping = vec!["level".to_string(), "nts".to_string()];
	(projection, grouping)
}

fn direction_to_sorting(
	d: &Option<Direction>,
	schema: &LogTable,
	revise: bool,
) -> Vec<(String, SortType)> {
	let k = if revise {
		schema.revised_ts_key()
	} else {
		schema.ts_key()
	};
	if let Some(d) = d {
		match d {
			Direction::Forward => vec![(k.to_string(), SortType::Asc)],
			Direction::Backward => vec![(k.to_string(), SortType::Desc)],
		}
	} else {
		vec![]
	}
}

fn truncate_seconds(seconds: u32, ts_key: &str) -> String {
	let v = seconds * 1_000_000; // microseconds;
	format!("TO_TIMESTAMP(({}::Int64/{})::Int64*{})", ts_key, v, seconds)
}

fn truncate_ts(d: Duration, ts_key: &str) -> String {
	let secs = d.as_secs();
	match secs {
		..=9 => truncate_seconds(5, ts_key),
		10..=14 => truncate_seconds(10, ts_key),
		15..=29 => truncate_seconds(15, ts_key),
		30..=59 => truncate_seconds(30, ts_key),
		_ => format!("{}({})", get_round_func(d), ts_key),
	}
}

fn get_round_func(d: Duration) -> &'static str {
	// according to bendsql docs: https://docs.databend.com/sql/sql-functions/datetime-functions/to-start-of-day
	const FIVE_MINS: u64 = 5 * 60;
	const TEN_MINS: u64 = 10 * 60;
	const FIFTEEN_MINS: u64 = 15 * 60;
	const ONE_HOUR: u64 = 60 * 60;
	const ONE_DAY: u64 = 24 * 60 * 60;
	const ONE_WEEK: u64 = 7 * 24 * 60 * 60;
	const ONE_MONTH: u64 = 30 * 24 * 60 * 60;
	const ONE_YEAR: u64 = 365 * 24 * 60 * 60;
	let secs = d.as_secs();
	if secs < FIVE_MINS {
		"TO_START_OF_MINUTE"
	} else if secs < TEN_MINS {
		"TO_START_OF_FIVE_MINUTES"
	} else if secs < FIFTEEN_MINS {
		"TO_START_OF_TEN_MINUTES"
	} else if secs < ONE_HOUR {
		"TO_START_OF_FIFTEEN_MINUTES"
	} else if secs < ONE_DAY {
		"TO_START_OF_HOUR"
	} else if secs < ONE_WEEK {
		"TO_START_OF_DAY"
	} else if secs < ONE_MONTH {
		"TO_START_OF_WEEK"
	} else if secs < ONE_YEAR {
		"TO_START_OF_MONTH"
	} else {
		"TO_START_OF_YEAR"
	}
}

#[cfg(test)]
mod tests {
	use super::{super::converter::micro_time, *};
	use chrono::Local;
	use pretty_assertions::assert_eq;
	use sqlparser::{dialect::AnsiDialect, parser::Parser};
	use std::{fs, path::PathBuf};

	#[test]
	fn test_truncate_ts() {
		let test_cases = [
			(
				Duration::from_secs(1),
				"TO_TIMESTAMP((ts::Int64/5000000)::Int64*5)",
			),
			(
				Duration::from_secs(5),
				"TO_TIMESTAMP((ts::Int64/5000000)::Int64*5)",
			),
			(
				Duration::from_secs(10),
				"TO_TIMESTAMP((ts::Int64/10000000)::Int64*10)",
			),
			(
				Duration::from_secs(15),
				"TO_TIMESTAMP((ts::Int64/15000000)::Int64*15)",
			),
			(
				Duration::from_secs(30),
				"TO_TIMESTAMP((ts::Int64/30000000)::Int64*30)",
			),
			(Duration::from_secs(60), "TO_START_OF_MINUTE(ts)"),
			(Duration::from_secs(60 * 4), "TO_START_OF_MINUTE(ts)"),
			(Duration::from_secs(60 * 5), "TO_START_OF_FIVE_MINUTES(ts)"),
			(Duration::from_secs(60 * 60), "TO_START_OF_HOUR(ts)"),
		];
		for (d, expected) in test_cases {
			assert_eq!(expected, truncate_ts(d, "ts"), "case: {:?}", d);
		}
	}

	#[test]
	fn test_get_round_func() {
		let test_cases = [
			(Duration::from_secs(1), "TO_START_OF_MINUTE"),
			(Duration::from_secs(60), "TO_START_OF_MINUTE"),
			(Duration::from_secs(200), "TO_START_OF_MINUTE"),
			(Duration::from_secs(300), "TO_START_OF_FIVE_MINUTES"),
			(Duration::from_secs(10 * 60), "TO_START_OF_TEN_MINUTES"),
			(Duration::from_secs(800), "TO_START_OF_TEN_MINUTES"),
			(Duration::from_secs(15 * 60), "TO_START_OF_FIFTEEN_MINUTES"),
			(Duration::from_secs(59 * 60), "TO_START_OF_FIFTEEN_MINUTES"),
			(Duration::from_secs(60 * 60), "TO_START_OF_HOUR"),
			(Duration::from_secs(60 * 60 * 24), "TO_START_OF_DAY"),
			(Duration::from_secs(60 * 60 * 24 * 6), "TO_START_OF_DAY"),
			(Duration::from_secs(60 * 60 * 24 * 7), "TO_START_OF_WEEK"),
			(Duration::from_secs(60 * 60 * 24 * 30), "TO_START_OF_MONTH"),
			(Duration::from_secs(60 * 60 * 24 * 365), "TO_START_OF_YEAR"),
		];
		for (d, expected) in test_cases {
			assert_eq!(expected, get_round_func(d), "case: {:?}", d);
		}
	}

	#[test]
	fn into_sql() {
		let now = Local::now().naive_local();
		let tb = LogTable {
			use_inverted_index: false,
			msg_key: "message",
			ts_key: "ts",
			table: "logs",
			level: "level",
			trace_id: "trace_id",
		};
		let plan: QueryPlan<LogTable, DatabendLogConverter> = QueryPlan::new(
			DatabendLogConverter::new(tb.clone()),
			tb,
			vec!["msg".to_string(), "ts".to_string()],
			Some(Selection::LogicalAnd(
				Box::new(Selection::Unit(Condition {
					column: Column::Raw("app".to_string()),
					cmp: Cmp::Equal(PlaceValue::String("camp".to_string())),
				})),
				Box::new(Selection::Unit(Condition {
					column: Column::Message,
					cmp: Cmp::Contains("error".to_string()),
				})),
			)),
			vec!["app".to_string(), "server".to_string()],
			vec![("ts".to_string(), SortType::Asc)],
			vec![(OrdType::LargerEqual, now)],
			Some(10),
		);
		assert_eq!(
			plan.as_sql(),
			format!("SELECT msg,ts FROM logs WHERE (app = 'camp' AND message LIKE '%error%') AND ts>='{}' GROUP BY app,server ORDER BY ts ASC LIMIT 10", micro_time(&now))
		);
	}
	#[test]
	fn metrics_sql() {
		let now = Local::now().naive_local();
		let end = now + Duration::from_secs(3600);
		let tb = LogTable {
			use_inverted_index: true,
			msg_key: "message",
			ts_key: "ts",
			table: "log",
			level: "level",
			trace_id: "trace_id",
		};
		let plan: QueryPlan<LogTable, DatabendLogConverter> = QueryPlan::new(
			DatabendLogConverter::new(tb.clone()),
			tb,
			vec![
				"level".to_string(),
				"TO_START_OF_HOUR(ts) as nts".to_string(),
				"count(*) as total".to_string(),
			],
			Some(Selection::LogicalAnd(
				Box::new(Selection::Unit(Condition {
					column: Column::Raw("app".to_string()),
					cmp: Cmp::NotEqual(PlaceValue::String("camp".to_string())),
				})),
				Box::new(Selection::Unit(Condition {
					column: Column::Message,
					cmp: Cmp::Contains("error".to_string()),
				})),
			)),
			vec!["level".to_string(), "nts".to_string()],
			vec![("nts".to_string(), SortType::Desc)],
			vec![(OrdType::LargerEqual, now), (OrdType::SmallerEqual, end)],
			Some(1000),
		);
		assert_eq!(
			plan.as_sql(),
			format!("SELECT level,TO_START_OF_HOUR(ts) as nts,count(*) as total FROM log WHERE (app != 'camp' AND MATCH(message,'error')) AND ts>='{}' AND ts<='{}' GROUP BY level,nts ORDER BY nts DESC LIMIT 1000",micro_time(&now), micro_time(&end))
		);
	}

	#[test]
	fn test_logql_to_sql() {
		let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
		d.push("src/storage/databend/logql_test.yaml");
		let test_cases = fs::read_to_string(d).unwrap();
		#[derive(serde::Deserialize)]
		struct TestCase {
			#[serde(default)]
			inverted: bool,
			input: String,
			expect: String,
		}
		let cases: HashMap<String, TestCase> =
			serde_yaml::from_str(&test_cases).unwrap();
		for (case, c) in cases {
			let q = logql::parser::parse_logql_query(&c.input).unwrap();
			if let logql::parser::Query::LogQuery(lq) = q {
				let mut schema = LogTable::default();
				if c.inverted {
					schema.use_inverted_index = true;
				}
				let actual = logql_to_sql(&lq, QueryLimits::default(), &schema);
				let actual_ast =
					Parser::parse_sql(&AnsiDialect {}, &actual).unwrap();
				let expect_ast =
					Parser::parse_sql(&AnsiDialect {}, &c.expect).unwrap();
				assert_eq!(
					expect_ast[0].to_string(),
					actual_ast[0].to_string(),
					"case: {}",
					case
				);
			} else {
				panic!("case: {}, expect LogQuery, got {:?}", case, q);
			}
		}
	}
}
