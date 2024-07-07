use chrono::NaiveDateTime;
use common::TimeRange;
use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cmp {
	Equal(PlaceValue),
	NotEqual(PlaceValue),
	RegexMatch(String),
	RegexNotMatch(String),
	Contains(String),
	NotContains(String),
	Larger(PlaceValue),
	LargerEqual(PlaceValue),
	Less(PlaceValue),
	LessEqual(PlaceValue),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaceValue {
	String(String),
	Integer(i64),
	Float(ordered_float::OrderedFloat<f64>),
}

impl Display for PlaceValue {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			PlaceValue::String(s) => write!(f, "'{}'", s),
			PlaceValue::Integer(i) => write!(f, "{}", i),
			PlaceValue::Float(fl) => write!(f, "{}", fl),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
	Unit(Condition),
	LogicalAnd(Box<Selection>, Box<Selection>),
	LogicalOr(Box<Selection>, Box<Selection>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition {
	pub column: Column,
	pub cmp: Cmp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Column {
	Message,
	Timestamp,
	Level,
	TraceID,
	Resources(String),
	Attributes(String),
	Raw(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortType {
	Asc,
	Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrdType {
	LargerEqual,
	SmallerEqual,
}

pub trait TableSchema {
	fn table(&self) -> &str;
	fn ts_key(&self) -> &str;
	fn msg_key(&self) -> &str;
	fn level_key(&self) -> &str;
	fn trace_key(&self) -> &str;
	fn resources_key(&self) -> &str;
	fn attributes_key(&self) -> &str;
}

#[derive(Debug, Clone)]
pub struct QueryPlan<T: TableSchema, C: QueryConverter> {
	converter: C,
	pub schema: T,
	pub projection: Vec<String>,
	pub selection: Option<Selection>,
	pub grouping: Vec<String>,
	pub sorting: Vec<(String, SortType)>,
	pub timing: Vec<(OrdType, NaiveDateTime)>,
	pub limit: Option<u32>,
}

impl<T: TableSchema, C: QueryConverter> QueryPlan<T, C> {
	pub fn new(
		converter: C,
		schema: T,
		projection: Vec<String>,
		selection: Option<Selection>,
		grouping: Vec<String>,
		sorting: Vec<(String, SortType)>,
		timing: Vec<(OrdType, NaiveDateTime)>,
		limit: Option<u32>,
	) -> Self {
		Self {
			converter,
			schema,
			projection,
			selection,
			grouping,
			sorting,
			timing,
			limit,
		}
	}
}

impl<T, C> QueryPlan<T, C>
where
	T: TableSchema,
	C: QueryConverter,
{
	pub fn as_sql(&self) -> String {
		let mut sql = self.projection_part();
		sql.push_str(&format!(" FROM {}", self.schema.table()));
		let where_part = self.where_part();
		if !where_part.is_empty() {
			sql.push_str(&format!(" WHERE {}", where_part));
		}
		if let Some(grouping) = self.grouping_part() {
			sql.push(' ');
			sql.push_str(&grouping);
		}
		if !self.sorting.is_empty() {
			sql.push_str(" ORDER BY ");
			sql.push_str(&self.sorting_part());
		}
		if let Some(limit) = self.limit_part() {
			sql.push(' ');
			sql.push_str(&limit);
		}
		sql
	}
	fn where_part(&self) -> String {
		let mut where_part = self.selection_part();
		let timing = self.timing_part();
		if !timing.is_empty() {
			if !where_part.is_empty() {
				where_part.push_str(" AND ");
			}
			where_part.push_str(&timing.join(" AND "));
		}
		where_part
	}
	fn projection_part(&self) -> String {
		format!("SELECT {}", self.projection.join(","))
	}
	fn selection_to_sql(&self, s: &Selection) -> String {
		match s {
			Selection::Unit(ref c) => self.converter.convert_condition(c),
			Selection::LogicalAnd(ref l, ref r) => {
				let l = self.selection_to_sql(l);
				let r = self.selection_to_sql(r);
				format!("({} AND {})", l, r)
			}
			Selection::LogicalOr(ref l, ref r) => {
				let l = self.selection_to_sql(l);
				let r = self.selection_to_sql(r);
				format!("({} OR {})", l, r)
			}
		}
	}
	fn selection_part(&self) -> String {
		if let Some(s) = &self.selection {
			self.selection_to_sql(s)
		} else {
			"".to_string()
		}
	}
	fn grouping_part(&self) -> Option<String> {
		if self.grouping.is_empty() {
			None
		} else {
			let w = format!("GROUP BY {}", self.grouping.join(","));
			Some(w)
		}
	}
	fn sorting_part(&self) -> String {
		self.sorting
			.iter()
			.map(|(c, t)| match t {
				SortType::Asc => format!("{} ASC", c),
				SortType::Desc => format!("{} DESC", c),
			})
			.collect::<Vec<String>>()
			.join(",")
	}
	fn timing_part(&self) -> Vec<String> {
		let ts_key = self.schema.ts_key();
		self.timing
			.iter()
			.map(|(o, t)| self.converter.convert_timing(ts_key, o, t))
			.collect()
	}
	fn limit_part(&self) -> Option<String> {
		self.limit.map(|l| format!("LIMIT {}", l))
	}
}

pub fn time_range_into_timing(
	range: &TimeRange,
) -> Vec<(OrdType, NaiveDateTime)> {
	let mut timing = vec![];
	if let Some(start) = range.start {
		timing.push((OrdType::LargerEqual, start));
	}
	if let Some(end) = range.end {
		timing.push((OrdType::SmallerEqual, end));
	}
	timing
}

pub fn conditions_into_selection(conds: &[Condition]) -> Selection {
	if conds.len() == 1 {
		return Selection::Unit(conds[0].clone());
	}
	let left = conditions_into_selection(&conds[..1]);
	let right = conditions_into_selection(&conds[1..]);
	Selection::LogicalAnd(Box::new(left), Box::new(right))
}

pub trait QueryConverter {
	fn convert_condition(&self, c: &Condition) -> String;
	fn convert_timing(&self, ts_key: &str, o: &OrdType, t: &NaiveDateTime) -> String;
}

#[cfg(test)]
mod tests {
	use super::*;
	use ordered_float::OrderedFloat;

	#[test]
	fn fmt_place_value_display() {
		let s = PlaceValue::String("hello".to_string());
		assert_eq!(format!("{}", s), "'hello'");
		let i = PlaceValue::Integer(123);
		assert_eq!(format!("{}", i), "123");
		let f = PlaceValue::Float(OrderedFloat(1.23));
		assert_eq!(format!("{}", f), "1.23");
	}
}
