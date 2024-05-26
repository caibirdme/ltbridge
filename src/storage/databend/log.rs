use super::{super::log::LogLevel, query_plan::*};
use crate::storage::{log::*, *};
use anyhow::Result;
use async_trait::async_trait;
use chrono::NaiveDateTime;
use databend_driver::{Connection, Row, TryFromRow};
use logql::parser::{Filter, LabelPair, LogQuery, MetricQuery};
use std::{
	collections::{HashMap, HashSet},
	time::Duration,
};
use tokio_stream::StreamExt;

const RESOURCES_PREFIX: &str = "resources.";
const ATTRIBUTES_PREFIX: &str = "attributes.";
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
		let mut conds =
			label_pairs_into_condition(&q.log_query.selector.label_paris);
		conds.extend(filter_into_condition(&q.log_query.filters, &self.schema));
		let selection = if conds.is_empty() {
			None
		} else {
			Some(conditions_into_selection(&conds))
		};
		let mut qp = new_from_metricquery(&opt, &self.schema);
		qp.selection = selection;
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
	async fn labels(&self, opt: QueryLimits) -> Result<Vec<String>> {
		let sql = gen_labels_sql(&self.schema, opt);
		let mut stream = self.cli.query_iter(&sql).await?;
		let mut resource_labels = HashSet::new();
		let mut attribute_labels = HashSet::new();
		while let Some(row) = stream.next().await {
			let row = row?;
			// todo: fix this when driver support try_from directly
			let keys: (Vec<String>, Vec<String>) = TryInto::try_into(row)
				.map_err(|e: String| anyhow::anyhow!(e))?;
			resource_labels.extend(keys.0);
			attribute_labels.extend(keys.1)
		}
		let mut labels: Vec<String> = self
			.schema
			.label_projection()
			.iter()
			.map(|s| s.to_string())
			.collect();
		labels.extend(
			resource_labels
				.into_iter()
				.map(|k| RESOURCES_PREFIX.to_owned() + &k),
		);
		labels.extend(
			attribute_labels
				.into_iter()
				.map(|k| ATTRIBUTES_PREFIX.to_owned() + &k),
		);
		Ok(labels)
	}
	async fn label_values(
		&self,
		label: &str,
		opt: QueryLimits,
	) -> Result<Vec<String>> {
		if label == "level" {
			return Ok(LogLevel::all_levels());
		} else if label == "trace_id" || label == "span_id" {
			// high cardinality, skip for now
			return Ok(vec![]);
		}
		let sql = label_values_into_sql(label, &self.schema, opt);
		let mut stream = self.cli.query_iter(&sql).await?;
		let mut values = HashSet::new();
		while let Some(row) = stream.next().await {
			let row = row?;
			let v = TryInto::<(String,)>::try_into(row)
				.map_err(|e: String| anyhow::anyhow!(e))?;
			values.insert(v.0);
		}
		let values = values
			.into_iter()
			.map(|v| v.trim_matches('"').to_string())
			.collect();
		Ok(values)
	}
}

fn label_values_into_sql(
	label: &str,
	schema: &LogTable,
	opt: QueryLimits,
) -> String {
	let qp = QueryPlan {
		schema,
		projection: if let Some(stripped) = label.strip_prefix(RESOURCES_PREFIX)
		{
			vec![format!("distinct resources['{}'] as tvals", stripped)]
		} else if let Some(stripped) = label.strip_prefix(ATTRIBUTES_PREFIX) {
			vec![format!("distinct attributes['{}'] as tvals", stripped)]
		} else {
			vec![format!("distinct {} as tvals", label)]
		},
		selection: None,
		grouping: vec![],
		sorting: vec![],
		timing: time_range_into_timing(&opt.range),
		limit: opt.limit.or(Some(200)),
	};
	qp.as_sql()
}

fn gen_labels_sql(schema: &LogTable, opt: QueryLimits) -> String {
	QueryPlan {
		schema,
		projection: vec![
			"MAP_KEYS(resources) AS res".to_string(),
			"MAP_KEYS(attributes) AS attrs".to_string(),
		],
		selection: None,
		grouping: vec![],
		sorting: vec![],
		timing: time_range_into_timing(&opt.range),
		limit: opt.limit.or(Some(100)),
	}
	.as_sql()
}

fn logql_to_sql(q: &LogQuery, opt: QueryLimits, schema: &LogTable) -> String {
	let mut conds = label_pairs_into_condition(&q.selector.label_paris);
	conds.extend(filter_into_condition(&q.filters, schema));
	let selection = if conds.is_empty() {
		None
	} else {
		Some(conditions_into_selection(&conds))
	};
	let mut qp = new_from_logquery(&opt, schema);
	qp.selection = selection;
	qp.as_sql()
}

#[derive(Debug, Default, Clone, TryFromRow)]
struct LogRaw {
	app: String,
	server: String,
	trace_id: String,
	span_id: String,
	level: u32,
	resources: HashMap<String, String>,
	attributes: HashMap<String, String>,
	message: String,
	ts: NaiveDateTime,
}

fn row_into_logitem(row: Row) -> Result<LogItem> {
	let LogRaw {
		app,
		server,
		trace_id,
		span_id,
		level,
		resources,
		attributes,
		message,
		ts,
	} = row.try_into().map_err(|e: String| anyhow::anyhow!(e))?;
	Ok(LogItem {
		app,
		server,
		trace_id,
		span_id,
		level: level.into(),
		resources,
		attributes,
		message,
		ts,
	})
}

/*
	CREATE TABLE logs (
		app STRING NOT NULL,
		server STRING NOT NULL,
		trace_id STRING,
		span_id STRING,
		level TINYINT,
		resources MAP(STRING, STRING) NOT NULL,
		attributes MAP(STRING, STRING) NOT NULL,
		message STRING NOT NULL,
		ts TIMESTAMP NOT NULL
	) ENGINE=FUSE CLUSTER BY(TO_YYYYMMDDHH(ts), server);
	CREATE INVERTED INDEX message_idx ON logs(message);
*/
#[derive(Debug, Clone)]
struct LogTable {
	use_inverted_index: bool,
	msg_key: &'static str,
	ts_key: &'static str,
	table: &'static str,
}

impl Default for LogTable {
	fn default() -> Self {
		Self {
			use_inverted_index: false,
			msg_key: "message",
			ts_key: "ts",
			table: "logs",
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
}

static LABEL_PROJECTIONS: [&str; 5] =
	["app", "server", "trace_id", "span_id", "level"];

impl LogTable {
	fn label_projection(&self) -> &[&'static str] {
		&LABEL_PROJECTIONS
	}
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

fn new_from_logquery<'a>(
	limits: &'a QueryLimits,
	schema: &'a LogTable,
) -> QueryPlan<'a, LogTable> {
	QueryPlan {
		schema,
		projection: schema.projection(),
		selection: None,
		grouping: vec![],
		sorting: direction_to_sorting(&limits.direction, schema, false),
		timing: time_range_into_timing(&limits.range),
		limit: limits.limit,
	}
}
fn new_from_metricquery<'a>(
	limits: &'a QueryLimits,
	schema: &'a LogTable,
) -> QueryPlan<'a, LogTable> {
	let (projection, grouping) = metrics_projection_and_grouping(
		schema,
		limits.step.unwrap_or(DEFAULT_STEP),
	);
	QueryPlan {
		schema,
		projection,
		selection: None,
		grouping,
		sorting: direction_to_sorting(&limits.direction, schema, true),
		timing: time_range_into_timing(&limits.range),
		limit: limits.limit,
	}
}

fn metrics_projection_and_grouping(
	schema: &LogTable,
	step: Duration,
) -> (Vec<String>, Vec<&'static str>) {
	let projection = vec![
		"level".to_string(),
		format!("{} as nts", truncate_ts(step, schema.ts_key())),
		"count(*) as total".to_string(),
	];
	let grouping = vec!["level", "nts"];
	(projection, grouping)
}

fn convert_nested_key(key: &str) -> String {
	if let Some(stripped) = key.strip_prefix(RESOURCES_PREFIX) {
		format!("resources['{}']", stripped)
	} else if let Some(stripped) = key.strip_prefix(ATTRIBUTES_PREFIX) {
		format!("attributes['{}']", stripped)
	} else {
		key.to_string()
	}
}

fn label_pairs_into_condition(pairs: &[LabelPair]) -> Vec<Condition> {
	pairs.iter().map(label_pair_to_cond).collect()
}

fn label_pair_to_cond(p: &LabelPair) -> Condition {
	use logql::parser::*;
	if p.label == "level" {
		let u: u32 = LogLevel::try_from(p.value.to_string())
			.unwrap_or(LogLevel::Info)
			.into();
		return Condition {
			column: "level".to_string(),
			cmp: Cmp::Equal(PlaceValue::Integer(u as i64)),
		};
	}
	Condition {
		column: convert_nested_key(&p.label),
		cmp: match p.op {
			Operator::Equal => {
				Cmp::Equal(PlaceValue::String(p.value.to_string()))
			}
			Operator::NotEqual => {
				Cmp::NotEqual(PlaceValue::String(p.value.to_string()))
			}
			Operator::RegexMatch => Cmp::RegexMatch(p.value.to_string()),
			Operator::RegexNotMatch => Cmp::RegexNotMatch(p.value.to_string()),
		},
	}
}

fn direction_to_sorting<'a>(
	d: &'a Option<Direction>,
	schema: &'a LogTable,
	revise: bool,
) -> Vec<(&'a str, SortType)> {
	let k = if revise {
		schema.revised_ts_key()
	} else {
		schema.ts_key()
	};
	if let Some(d) = d {
		match d {
			Direction::Forward => vec![(k, SortType::Asc)],
			Direction::Backward => vec![(k, SortType::Desc)],
		}
	} else {
		vec![]
	}
}

fn filter_into_condition(
	filters: &Option<Vec<Filter>>,
	schema: &LogTable,
) -> Vec<Condition> {
	use logql::parser::*;
	if let Some(filters) = filters {
		filters
			.iter()
			.filter_map(|f| match f {
				Filter::LogLine(l) => Some(l),
				_ => None,
			})
			.map(|l| {
				let cmp = match l.op {
					FilterType::Contain => {
						if schema.use_inverted_index {
							Cmp::Match(l.expression.to_string())
						} else {
							Cmp::Contains(l.expression.to_string())
						}
					}
					FilterType::NotContain => {
						if schema.use_inverted_index {
							Cmp::NotMatch(l.expression.to_string())
						} else {
							Cmp::NotContains(l.expression.to_string())
						}
					}
					FilterType::RegexMatch => {
						Cmp::RegexMatch(l.expression.to_string())
					}
					FilterType::RegexNotMatch => {
						Cmp::RegexNotMatch(l.expression.to_string())
					}
				};
				Condition {
					column: schema.msg_key.to_string(),
					cmp,
				}
			})
			.collect()
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
	use super::*;
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
		let plan = QueryPlan {
			schema: &LogTable {
				use_inverted_index: false,
				msg_key: "message",
				ts_key: "ts",
				table: "logs",
			},
			projection: vec!["msg".to_string(), "ts".to_string()],
			selection: Some(Selection::LogicalAnd(
				Box::new(Selection::Unit(Condition {
					column: "app".to_string(),
					cmp: Cmp::Equal(PlaceValue::String("camp".to_string())),
				})),
				Box::new(Selection::Unit(Condition {
					column: "message".to_string(),
					cmp: Cmp::Contains("error".to_string()),
				})),
			)),
			grouping: vec!["app", "server"],
			sorting: vec![("ts", SortType::Asc)],
			timing: vec![(OrdType::LargerEqual, now)],
			limit: Some(10),
		};
		assert_eq!(
			plan.as_sql(),
			format!("SELECT msg,ts FROM logs WHERE (app = 'camp' AND message LIKE '%error%') AND ts>='{}' GROUP BY app,server ORDER BY ts ASC LIMIT 10",micro_time(&now))
		);
	}
	#[test]
	fn metrics_sql() {
		let now = Local::now().naive_local();
		let end = now + Duration::from_secs(3600);
		let plan = QueryPlan {
			schema: &LogTable {
				use_inverted_index: true,
				msg_key: "message",
				ts_key: "ts",
				table: "log",
			},
			projection: vec![
				"level".to_string(),
				"TO_START_OF_HOUR(ts) as nts".to_string(),
				"count(*) as total".to_string(),
			],
			selection: Some(Selection::LogicalAnd(
				Box::new(Selection::Unit(Condition {
					column: "app".to_string(),
					cmp: Cmp::NotEqual(PlaceValue::String("camp".to_string())),
				})),
				Box::new(Selection::Unit(Condition {
					column: "message".to_string(),
					cmp: Cmp::Match("error".to_string()),
				})),
			)),
			grouping: vec!["level", "nts"],
			sorting: vec![("nts", SortType::Desc)],
			timing: vec![
				(OrdType::LargerEqual, now),
				(OrdType::SmallerEqual, end),
			],
			limit: Some(1000),
		};
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

	#[test]
	fn test_label_sql() {
		let now = Local::now().naive_local();
		let test_cases = [
			(
				QueryLimits {
					range: TimeRange::default(),
					limit: Some(50),
					step: None,
					direction: None,
				},
				"SELECT MAP_KEYS(resources) AS res, MAP_KEYS(attributes) AS attrs FROM logs LIMIT 50".to_string(),
			),
			(
				QueryLimits {
					range: TimeRange::default(),
					limit: None,
					step: None,
					direction: None,
				},
				"SELECT MAP_KEYS(resources) AS res, MAP_KEYS(attributes) AS attrs FROM logs LIMIT 100".to_string(),
			),
			(
				QueryLimits {
					range: TimeRange {
						start: Some(now - Duration::from_secs(60)),
						end: Some(now),
					},
					limit: None,
					step: None,
					direction: None,
				},
				format!(
					"SELECT MAP_KEYS(resources) AS res, MAP_KEYS(attributes) AS attrs FROM logs WHERE ts>='{}' AND ts<='{}' LIMIT 100",
					micro_time(&(now - Duration::from_secs(60))),
					micro_time(&now)
				),
			),
		];
		for (opt, expect) in test_cases.iter() {
			let schema = LogTable::default();
			let actual = gen_labels_sql(&schema, opt.clone());
			let actual_ast =
				Parser::parse_sql(&AnsiDialect {}, &actual).unwrap();
			let expect_ast =
				Parser::parse_sql(&AnsiDialect {}, expect).unwrap();
			assert_eq!(expect_ast[0].to_string(), actual_ast[0].to_string());
		}
	}
}
