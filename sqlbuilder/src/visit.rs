use super::builder::{
	conditions_into_selection, Cmp, Column, Condition, PlaceValue, Selection,
};
use logql::parser::*;

pub const RESOURCES_PREFIX: &str = "resources_";
pub const ATTRIBUTES_PREFIX: &str = "attributes_";

pub trait IRVisitor {
	fn label_pair(&self, label: &LabelPair) -> Condition;
	fn log_filter(&self, filter: &LogLineFilter) -> Condition;
}

pub struct LogQLVisitor<T> {
	udf: T,
}

impl<T: IRVisitor> LogQLVisitor<T> {
	pub fn new(udf: T) -> Self {
		Self { udf }
	}
	pub fn visit(&self, q: &LogQuery) -> Option<Selection> {
		let mut conds = self.visit_labels(&q.selector.label_paris);
		conds.extend(self.visit_filters(&q.filters));
		dbg!("conds is {}", &conds);
		if conds.is_empty() {
			None
		} else {
			Some(conditions_into_selection(&conds))
		}
	}
	fn visit_labels(&self, labels: &[LabelPair]) -> Vec<Condition> {
		labels.iter().map(|p| self.udf.label_pair(p)).collect()
	}
	fn visit_filters(&self, filters: &Option<Vec<Filter>>) -> Vec<Condition> {
		if let Some(filters) = filters {
			filters
				.iter()
				.filter_map(|f| match f {
					Filter::LogLine(l) => Some(l),
					_ => None,
				})
				.map(|l| self.udf.log_filter(l))
				.collect()
		} else {
			vec![]
		}
	}
}

pub struct DefaultIRVisitor;

impl IRVisitor for DefaultIRVisitor {
	fn label_pair(&self, p: &LabelPair) -> Condition {
		if matches!(p.label.to_lowercase().as_str(), "trace_id" | "traceid") {
			return Condition {
				column: Column::TraceID,
				cmp: Cmp::Equal(PlaceValue::String(p.value.to_string())),
			};
		}
		if matches!(p.label.to_lowercase().as_str(), "level" | "severitytext") {
			return Condition {
				column: Column::Level,
				cmp: match p.op {
					Operator::NotEqual => {
						Cmp::NotEqual(PlaceValue::String(p.value.to_string()))
					}
					Operator::RegexMatch => {
						Cmp::RegexMatch(p.value.to_string())
					}
					Operator::RegexNotMatch => {
						Cmp::RegexNotMatch(p.value.to_string())
					}
					Operator::Equal => {
						Cmp::Equal(PlaceValue::String(p.value.to_string()))
					}
				},
			};
		}
		Condition {
			column: maybe_nested_key(&p.label),
			cmp: match p.op {
				Operator::Equal => {
					Cmp::Equal(PlaceValue::String(p.value.to_string()))
				}
				Operator::NotEqual => {
					Cmp::NotEqual(PlaceValue::String(p.value.to_string()))
				}
				Operator::RegexMatch => Cmp::RegexMatch(p.value.to_string()),
				Operator::RegexNotMatch => {
					Cmp::RegexNotMatch(p.value.to_string())
				}
			},
		}
	}

	fn log_filter(&self, l: &LogLineFilter) -> Condition {
		let cmp = match l.op {
			FilterType::Contain => Cmp::Contains(l.expression.to_string()),
			FilterType::NotContain => {
				Cmp::NotContains(l.expression.to_string())
			}
			FilterType::RegexMatch => Cmp::RegexMatch(l.expression.to_string()),
			FilterType::RegexNotMatch => {
				Cmp::RegexNotMatch(l.expression.to_string())
			}
		};
		Condition {
			column: Column::Message,
			cmp,
		}
	}
}

fn maybe_nested_key(key: &str) -> Column {
	if let Some(stripped) = key.strip_prefix(RESOURCES_PREFIX) {
		Column::Resources(stripped.to_string())
	} else if let Some(stripped) = key.strip_prefix(ATTRIBUTES_PREFIX) {
		Column::Attributes(stripped.to_string())
	} else {
		Column::Raw(key.to_string())
	}
}
