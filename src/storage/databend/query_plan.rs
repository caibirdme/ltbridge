use std::fmt::Display;

use crate::storage::TimeRange;
use chrono::NaiveDateTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cmp {
	Equal(PlaceValue),
	NotEqual(PlaceValue),
	RegexMatch(String),
	RegexNotMatch(String),
	Contains(String),
	NotContains(String),
	Match(String),
	NotMatch(String),
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
	pub column: String,
	pub cmp: Cmp,
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
}

#[derive(Debug, Clone)]
pub struct QueryPlan<'a, T: TableSchema> {
	pub schema: &'a T,
	pub projection: Vec<String>,
	pub selection: Option<Selection>,
	pub grouping: Vec<&'a str>,
	pub sorting: Vec<(&'a str, SortType)>,
	pub timing: Vec<(OrdType, NaiveDateTime)>,
	pub limit: Option<u32>,
}

impl<'a, T> QueryPlan<'a, T>
where
	T: TableSchema,
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
	fn convert_unit(c: &Condition) -> String {
		match &c.cmp {
			Cmp::Equal(v) => format!("{} = {}", c.column, v),
			Cmp::NotEqual(v) => format!("{} != {}", c.column, v),
			Cmp::Larger(v) => format!("{} > {}", c.column, v),
			Cmp::LargerEqual(v) => format!("{} >= {}", c.column, v),
			Cmp::Less(v) => format!("{} < {}", c.column, v),
			Cmp::LessEqual(v) => format!("{} <= {}", c.column, v),
			Cmp::RegexMatch(v) => format!("{} REGEXP '{}'", c.column, v),
			Cmp::RegexNotMatch(v) => format!("{} NOT REGEXP '{}'", c.column, v),
			Cmp::Contains(v) => format!("{} LIKE '%{}%'", c.column, v),
			Cmp::NotContains(v) => format!("{} NOT LIKE '%{}%'", c.column, v),
			Cmp::Match(v) => format!("MATCH({},'{}')", c.column, v),
			Cmp::NotMatch(v) => format!("NOT MATCH({},'{}')", c.column, v),
		}
	}
	fn selection_to_sql(s: &Selection) -> String {
		match s {
			Selection::Unit(ref c) => Self::convert_unit(c),
			Selection::LogicalAnd(ref l, ref r) => {
				let l = Self::selection_to_sql(l);
				let r = Self::selection_to_sql(r);
				format!("({} AND {})", l, r)
			}
			Selection::LogicalOr(ref l, ref r) => {
				let l = Self::selection_to_sql(l);
				let r = Self::selection_to_sql(r);
				format!("({} OR {})", l, r)
			}
		}
	}
	fn selection_part(&self) -> String {
		if let Some(s) = &self.selection {
			Self::selection_to_sql(s)
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
			.map(|(o, t)| {
				let ts = micro_time(t);
				match o {
					OrdType::LargerEqual => {
						format!("{}>='{}'", ts_key, ts)
					}
					OrdType::SmallerEqual => {
						format!("{}<='{}'", ts_key, ts)
					}
				}
			})
			.collect()
	}
	fn limit_part(&self) -> Option<String> {
		self.limit.map(|l| format!("LIMIT {}", l))
	}
}

pub fn micro_time(t: &NaiveDateTime) -> String {
	t.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
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
