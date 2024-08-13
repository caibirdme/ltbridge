use super::builder::{
	Cmp, Column, Condition, PlaceValue, QueryConverter, QueryPlan, Selection,
	TableSchema,
};
use itertools::Itertools as _;
use traceql::{
	ComparisonOperator, Expression, FieldExpr, FieldType, FieldValue,
	IntrisincField, LogicalOperator, SpanSet,
};

enum SubQuery<T: TableSchema, C: QueryConverter> {
	Basic(QueryPlan<T, C>),
	And(Box<SubQuery<T, C>>, Box<SubQuery<T, C>>),
	Or(Box<SubQuery<T, C>>, Box<SubQuery<T, C>>),
}

impl<T, C> SubQuery<T, C>
where
	T: TableSchema,
	C: QueryConverter,
{
	fn new(
		converter: C,
		expr: &Expression,
		schema: T,
		spans: &mut Vec<QueryPlan<T, C>>,
	) -> Self
	where
		C: Clone,
		T: Clone,
	{
		match expr {
			Expression::SpanSet(spanset) => {
				let selection = spanset_to_qp(spanset);
				let mut qp = QueryPlan::new(
					converter.clone(),
					schema.clone(),
					vec![
						schema.span_id_key().to_string(),
						schema.trace_key().to_string(),
					],
					Some(selection.clone()),
					vec![],
					vec![],
					vec![],
					None,
				);
				spans.push(qp.clone());
				qp.projection = vec![schema.trace_key().to_string()];
				SubQuery::Basic(qp)
			}
			Expression::Logical(left, op, right) => {
				let l =
					Self::new(converter.clone(), left, schema.clone(), spans);
				let r =
					Self::new(converter.clone(), right, schema.clone(), spans);
				match op {
					LogicalOperator::And => {
						SubQuery::And(Box::new(l), Box::new(r))
					}
					LogicalOperator::Or => {
						SubQuery::Or(Box::new(l), Box::new(r))
					}
				}
			}
		}
	}
	fn as_sql(&self) -> String {
		match self {
			SubQuery::Basic(qp) => {
				format!("sub.{} IN ({})", qp.schema.trace_key(), qp.as_sql())
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
				Column::Raw("StatusCode".to_string()),
				PlaceValue::Integer((*status).into()),
				expr.operator,
			),
			IntrisincField::Duraion(d) => construct_condition(
				Column::Raw("Duration".to_string()),
				PlaceValue::Integer(d.as_nanos() as i64),
				expr.operator,
			),
			IntrisincField::Kind(kind) => construct_condition(
				Column::Raw("SpanKind".to_string()),
				PlaceValue::Integer((*kind).into()),
				expr.operator,
			),
			IntrisincField::Name(name) => construct_condition(
				Column::Raw("SpanName".to_string()),
				PlaceValue::String(name.clone()),
				expr.operator,
			),
			IntrisincField::ServiceName(name) => construct_condition(
				Column::Raw("ServiceName".to_string()),
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

fn field_value_to_place_value(f: &FieldValue) -> PlaceValue {
	match f {
		FieldValue::String(s) => PlaceValue::String(s.clone()),
		FieldValue::Integer(i) => PlaceValue::Integer(*i),
		FieldValue::Float(f) => PlaceValue::Float(*f),
		_ => unimplemented!("field value to place value"),
	}
}

pub struct ComplexQuery<T: TableSchema, C: QueryConverter> {
	schema: T,
	span_selections: Vec<QueryPlan<T, C>>,
	trace_selections: SubQuery<T, C>,
}

impl<T, C> ComplexQuery<T, C>
where
	T: TableSchema,
	C: QueryConverter,
{
	pub fn new(expr: &Expression, schema: T, converter: C) -> Self
	where
		C: Clone,
		T: Clone,
	{
		let mut spans = vec![];
		let trace_selections =
			SubQuery::new(converter, expr, schema.clone(), &mut spans);
		ComplexQuery {
			schema: schema.clone(),
			span_selections: spans,
			trace_selections,
		}
	}
	pub fn as_sql(&self) -> String {
		let mut sql = format!(
			"SELECT * FROM {} sp WHERE sp.{} IN (SELECT {} FROM (",
			self.schema.table(),
			self.schema.span_id_key(),
			self.schema.span_id_key(),
		);
		let w = self
			.span_selections
			.iter()
			.map(|v| format!("({})", v.as_sql()))
			.join(" UNION ");
		sql.push_str(&w);
		sql.push_str(") AS sub WHERE ");
		sql.push_str(self.trace_selections.as_sql().as_ref());
		sql.push_str(") LIMIT 500");
		sql
	}
}
