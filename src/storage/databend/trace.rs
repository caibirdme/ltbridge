use crate::storage::{trace::*, *};
use anyhow::Result;
use async_trait::async_trait;
use databend::converter::DatabendTraceConverter;
use databend_driver::{Connection, Row, TryFromRow};
use itertools::Itertools;
use sqlbuilder::builder::*;
use std::collections::HashMap;
use tokio_stream::StreamExt;
use traceql::*;

#[derive(Clone)]
pub struct BendTraceQuerier {
	cli: Box<dyn Connection>,
	schema: TraceTable,
}

impl BendTraceQuerier {
	pub fn new(cli: Box<dyn Connection>) -> Self {
		Self {
			cli,
			schema: TraceTable::default(),
		}
	}
}

#[async_trait]
impl TraceStorage for BendTraceQuerier {
	async fn query_trace(
		&self,
		trace_id: &str,
		opt: QueryLimits,
	) -> Result<Vec<SpanItem>> {
		let mut qp = new_qp(&opt, self.schema.clone());
		let conds = vec![Condition {
			column: Column::TraceID,
			cmp: Cmp::Equal(PlaceValue::String(trace_id.to_string())),
		}];
		let selection = Some(conditions_into_selection(conds.as_slice()));
		qp.selection = selection;
		let sql = qp.as_sql();
		let mut spans = vec![];
		let mut stream = self.cli.query_iter(&sql).await?;
		while let Some(row) = stream.next().await {
			let row = row?;
			let item = row_into_spanitem(row)?;
			spans.push(item);
		}
		Ok(spans)
	}

	async fn search_span(
		&self,
		expr: &Expression,
		opt: QueryLimits,
	) -> Result<Vec<SpanItem>> {
		let sql = search_span_sql(expr, &opt, &self.schema);
		let mut spans = vec![];
		let mut stream = self.cli.query_iter(&sql).await?;
		while let Some(row) = stream.next().await {
			let row = row?;
			let item = row_into_spanitem(row)?;
			spans.push(item);
		}
		Ok(spans)
	}
}

fn search_span_sql(
	expr: &Expression,
	opt: &QueryLimits,
	schema: &TraceTable,
) -> String {
	let mut spans = vec![];
	let subq = new_from_expression(expr, opt, schema, &mut spans);
	let complex = ComplexQuery {
		schema: schema.clone(),
		span_selections: spans,
		trace_selections: subq,
		limits: opt.clone(),
	};
	complex.as_sql()
}

/*
CREATE TABLE spans (
	ts TIMESTAMP NOT NULL,
	trace_id STRING NOT NULL,
	span_id STRING NOT NULL,
	parent_span_id STRING,
	trace_state STRING NOT NULL,
	span_name STRING NOT NULL,
	span_kind TINYINT,
	service_name STRING DEFAULT 'unknown',
	resource_attributes Map(STRING, Variant) NOT NULL,
	scope_name STRING,
	scope_version STRING,
	span_attributes Map(STRING, Variant),
	duration BIGINT,
	status_code INT32,
	status_message STRING,
	span_events Variant,
	links Variant
) ENGINE = FUSE CLUSTER BY (TO_YYYYMMDDHH(ts));
*/
#[derive(Debug, Clone)]
pub struct TraceTable {
	t: String,
}

impl Default for TraceTable {
	fn default() -> Self {
		Self {
			t: "spans".to_string(),
		}
	}
}

impl TraceTable {
	fn table_name(&self) -> &str {
		&self.t
	}
	fn projection(&self) -> Vec<String> {
		vec![
			"ts".to_string(),
			"trace_id".to_string(),
			"span_id".to_string(),
			"parent_span_id".to_string(),
			"trace_state".to_string(),
			"span_name".to_string(),
			"span_kind".to_string(),
			"service_name".to_string(),
			"resource_attributes".to_string(),
			"scope_name".to_string(),
			"scope_version".to_string(),
			"span_attributes".to_string(),
			"duration".to_string(),
			"status_code".to_string(),
			"status_message".to_string(),
			"span_events".to_string(),
			"links".to_string(),
		]
	}
	fn trace_key(&self) -> &str {
		"trace_id"
	}
}

impl TableSchema for TraceTable {
	fn table(&self) -> &str {
		self.table_name()
	}
	fn ts_key(&self) -> &str {
		"ts"
	}
	fn msg_key(&self) -> &str {
		""
	}
	fn level_key(&self) -> &str {
		""
	}
	fn trace_key(&self) -> &str {
		self.trace_key()
	}
	fn resources_key(&self) -> &str {
		"resource_attributes"
	}
	fn attributes_key(&self) -> &str {
		"span_attributes"
	}
}

fn new_qp(
	opt: &QueryLimits,
	schema: TraceTable,
) -> QueryPlan<TraceTable, DatabendTraceConverter> {
	let t = opt.range.clone();
	let projection = schema.projection();
	QueryPlan::new(
		DatabendTraceConverter::new(schema.clone()),
		schema,
		projection,
		None,
		vec![],
		vec![],
		time_range_into_timing(&t),
		opt.limit,
	)
}

/*

-- A and (B or C)

SELECT sp.* FROM spans sp
WHERE sp.span_id IN (
	-- 查询同时满足A or B or C的所有span_id以及对应的trace_id
	SELECT span_id FROM (
		SELECT span_id,trace_id FROM spans
		WHERE A
		UNION
		SELECT span_id,trace_id FROM spans
		WHERE B
		UNION
		SELECT span_id,trace_id FROM spans
		WHERE C
	) sub
	-- 查询同时满足A and (B or C)的trace_id
	WHERE sub.trace_id IN (
		SELECT trace_id FROM spans
		--查询满足A的span的trace_id
		WHERE trace_in IN (
			SELECT trace_id in spans
			WHERE A
		)
		--查询满足(B or C)的span的trace_id
		AND (
			trace_id IN (
				SELECT trace_id in spans
				WHERE B
			)
			OR
			trace_id IN (
				SELECT trace_id in spans
				WHERE C
			)
		)
	)
)
*/

struct ComplexQuery {
	schema: TraceTable,
	span_selections: Vec<QueryPlan<TraceTable, DatabendTraceConverter>>,
	trace_selections: SubQuery,
	limits: QueryLimits,
}

impl ComplexQuery {
	fn as_sql(&self) -> String {
		let mut sql = format!(
			"SELECT {} FROM {} sp WHERE sp.span_id IN (SELECT span_id FROM (",
			self.schema
				.projection()
				.iter()
				.map(|v| format!("sp.{}", v))
				.collect::<Vec<String>>()
				.join(","),
			self.schema.table(),
		);
		let w = self
			.span_selections
			.iter()
			.map(|v| format!("({})", v.as_sql()))
			.join(" UNION ");
		sql.push_str(&w);
		sql.push_str(") AS sub WHERE ");
		sql.push_str(self.trace_selections.as_sql().as_ref());
		sql.push(')');
		if let Some(limit) = self.limits.limit {
			sql.push_str(&format!(" LIMIT {}", limit));
		}
		sql
	}
}

enum SubQuery {
	Basic(QueryPlan<TraceTable, DatabendTraceConverter>),
	And(Box<SubQuery>, Box<SubQuery>),
	Or(Box<SubQuery>, Box<SubQuery>),
}

impl SubQuery {
	fn as_sql(&self) -> String {
		match self {
			SubQuery::Basic(qp) => {
				format!("sub.trace_id IN ({})", qp.as_sql())
			}
			SubQuery::And(l, r) => {
				let l_sql = l.as_sql();
				let r_sql = r.as_sql();
				format!("({} AND {})", l_sql, r_sql)
			}
			SubQuery::Or(l, r) => {
				let l_sql = l.as_sql();
				let r_sql = r.as_sql();
				format!("({} OR {})", l_sql, r_sql)
			}
		}
	}
}

fn field_value_to_place_value(f: &FieldValue) -> PlaceValue {
	match f {
		FieldValue::String(s) => PlaceValue::String(s.clone()),
		FieldValue::Integer(i) => PlaceValue::Integer(*i),
		FieldValue::Float(f) => PlaceValue::Float(*f),
		_ => unimplemented!("field value to place value"),
	}
}

fn construct_condition(
	key: Column,
	value: PlaceValue,
	op: ComparisonOperator,
) -> Condition {
	match op {
		ComparisonOperator::Equal => Condition {
			column: key,
			cmp: Cmp::Equal(value.clone()),
		},
		ComparisonOperator::NotEqual => Condition {
			column: key,
			cmp: Cmp::NotEqual(value.clone()),
		},
		ComparisonOperator::LessThan => Condition {
			column: key,
			cmp: Cmp::Less(value.clone()),
		},
		ComparisonOperator::LessThanOrEqual => Condition {
			column: key,
			cmp: Cmp::LessEqual(value.clone()),
		},
		ComparisonOperator::GreaterThan => Condition {
			column: key,
			cmp: Cmp::Larger(value.clone()),
		},
		ComparisonOperator::GreaterThanOrEqual => Condition {
			column: key,
			cmp: Cmp::LargerEqual(value.clone()),
		},
		ComparisonOperator::RegularExpression => Condition {
			column: key,
			cmp: match value {
				PlaceValue::String(s) => Cmp::RegexMatch(s),
				_ => unimplemented!("regular expression"),
			},
		},
		ComparisonOperator::NegatedRegularExpression => Condition {
			column: key,
			cmp: match value {
				PlaceValue::String(s) => Cmp::RegexNotMatch(s),
				_ => unimplemented!("negated regular expression"),
			},
		},
	}
}

fn field_expr_to_condition(expr: &FieldExpr) -> Condition {
	match &expr.kv {
		FieldType::Intrinsic(intrisinc) => match intrisinc {
			IntrisincField::Status(status) => construct_condition(
				Column::Raw("status_code".to_string()),
				PlaceValue::Integer((*status).into()),
				expr.operator,
			),
			IntrisincField::Duraion(d) => construct_condition(
				Column::Raw("duration".to_string()),
				PlaceValue::Integer(d.as_nanos() as i64),
				expr.operator,
			),
			IntrisincField::Kind(kind) => construct_condition(
				Column::Raw("span_kind".to_string()),
				PlaceValue::Integer((*kind).into()),
				expr.operator,
			),
			IntrisincField::Name(name) => construct_condition(
				Column::Raw("span_name".to_string()),
				PlaceValue::String(name.clone()),
				expr.operator,
			),
			IntrisincField::ServiceName(name) => construct_condition(
				Column::Raw("service_name".to_string()),
				PlaceValue::String(name.clone()),
				expr.operator,
			),
			_ => unimplemented!("intrinsic field"),
		},
		FieldType::Resource(key, val) => {
			let value = field_value_to_place_value(val);
			construct_condition(
				Column::Resources(key.clone()),
				value,
				expr.operator,
			)
		}
		FieldType::Span(key, val) => {
			let value = field_value_to_place_value(val);
			construct_condition(
				Column::Attributes(key.clone()),
				value,
				expr.operator,
			)
		}
		FieldType::Unscoped(..) => unimplemented!("unscoped field"),
	}
}

fn spanset_to_qp(spanset: &SpanSet) -> Selection {
	match spanset {
		SpanSet::Expr(expr) => {
			// expand unscoped into (resource or span)
			if let FieldType::Unscoped(s, v) = &expr.kv {
				let left = SpanSet::Expr(FieldExpr {
					kv: FieldType::Span(s.to_string(), v.clone()),
					operator: expr.operator,
				});
				let right = SpanSet::Expr(FieldExpr {
					kv: FieldType::Resource(s.to_string(), v.clone()),
					operator: expr.operator,
				});
				return Selection::LogicalOr(
					Box::new(spanset_to_qp(&left)),
					Box::new(spanset_to_qp(&right)),
				);
			}
			let c = field_expr_to_condition(expr);
			Selection::Unit(c)
		}
		SpanSet::Logical(left, op, right) => {
			let l = spanset_to_qp(left);
			let r = spanset_to_qp(right);
			match op {
				LogicalOperator::And => {
					Selection::LogicalAnd(Box::new(l), Box::new(r))
				}
				LogicalOperator::Or => {
					Selection::LogicalOr(Box::new(l), Box::new(r))
				}
			}
		}
	}
}

fn new_from_expression(
	expr: &Expression,
	opt: &QueryLimits,
	schema: &TraceTable,
	spans: &mut Vec<QueryPlan<TraceTable, DatabendTraceConverter>>,
) -> SubQuery {
	match expr {
		Expression::SpanSet(spanset) => {
			let selection = spanset_to_qp(spanset);
			let mut qp = new_qp(opt, schema.clone());
			qp.limit = None;
			qp.projection = vec!["span_id".to_string(), "trace_id".to_string()];
			qp.selection = Some(selection);
			spans.push(qp.clone());
			qp.projection = vec!["trace_id".to_string()];
			SubQuery::Basic(qp)
		}
		Expression::Logical(left, op, right) => {
			let l = new_from_expression(left, opt, schema, spans);
			let r = new_from_expression(right, opt, schema, spans);
			match op {
				LogicalOperator::And => SubQuery::And(Box::new(l), Box::new(r)),
				LogicalOperator::Or => SubQuery::Or(Box::new(l), Box::new(r)),
			}
		}
	}
}

#[derive(Debug, Default, Clone, TryFromRow)]
struct TraceRaw {
	ts: NaiveDateTime,
	trace_id: String,
	span_id: String,
	parent_span_id: String,
	trace_state: String,
	span_name: String,
	span_kind: i32,
	service_name: String,
	resource_attributes: HashMap<String, String>,
	scope_name: Option<String>,
	scope_version: Option<String>,
	span_attributes: HashMap<String, String>,
	duration: i64,
	status_code: Option<i32>,
	status_message: Option<String>,
	span_events: String,
	link: String,
}

fn row_into_spanitem(row: Row) -> Result<SpanItem> {
	let raw =
		TraceRaw::try_from(row).map_err(|e: String| anyhow::anyhow!(e))?;
	let attr_json: HashMap<String, serde_json::Value> = raw
		.span_attributes
		.into_iter()
		.map(|(k, v)| (k, serde_json::from_str(&v).unwrap_or_default()))
		.collect();
	let resource_json: HashMap<String, serde_json::Value> = raw
		.resource_attributes
		.into_iter()
		.map(|(k, v)| (k, serde_json::from_str(&v).unwrap_or_default()))
		.collect();
	let events: Vec<SpanEvent> = serde_json::from_str(&raw.span_events)?;
	let links: Vec<Links> = serde_json::from_str(&raw.link)?;
	Ok(SpanItem {
		ts: raw.ts.and_utc(),
		trace_id: raw.trace_id,
		span_id: raw.span_id,
		parent_span_id: raw.parent_span_id,
		trace_state: raw.trace_state,
		span_name: raw.span_name,
		span_kind: raw.span_kind,
		service_name: raw.service_name,
		resource_attributes: resource_json,
		scope_name: raw.scope_name,
		scope_version: raw.scope_version,
		span_attributes: attr_json,
		duration: raw.duration,
		status_code: raw.status_code,
		status_message: raw.status_message,
		span_events: events,
		link: links,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use common::TimeRange;
	use pretty_assertions::assert_eq;
	use sqlparser::{dialect::AnsiDialect, parser::Parser};
	use std::{fs, path::PathBuf};
	use traceql::parse_traceql;

	#[test]
	fn expand_complex_traceql() {
		let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
		d.push("src/storage/databend/traceql_test.yaml");
		let test_cases = fs::read_to_string(d).unwrap();

		#[derive(serde::Deserialize)]
		struct TestCase {
			input: String,
			expect: String,
			limit: u32,
		}
		let cases: HashMap<String, TestCase> =
			serde_yaml::from_str(&test_cases).unwrap();
		for (name, tc) in cases {
			let expr = parse_traceql(&tc.input).unwrap();
			let opt = QueryLimits {
				limit: Some(tc.limit),
				range: TimeRange {
					start: None,
					end: None,
				},
				direction: None,
				step: None,
			};
			let tb = TraceTable::default();
			let sql = search_span_sql(&expr, &opt, &tb);
			let actual_ast = Parser::parse_sql(&AnsiDialect {}, &sql).unwrap();
			let expect_ast =
				Parser::parse_sql(&AnsiDialect {}, &tc.expect).unwrap();
			assert_eq!(
				expect_ast[0].to_string(),
				actual_ast[0].to_string(),
				"case: {}",
				name
			);
		}
	}
}
