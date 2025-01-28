use chrono::NaiveDateTime;
use itertools::Itertools as _;
use sqlbuilder::builder::*;

#[derive(Clone)]
pub struct CKLogConverter<T: TableSchema> {
	table: T,
	replace_dash_to_dot: bool,
	level_insenstive: bool,
}

impl<T: TableSchema> CKLogConverter<T> {
	pub fn new(
		table: T,
		replace_dash_to_dot: bool,
		level_insenstive: bool,
	) -> Self {
		Self {
			table,
			replace_dash_to_dot,
			level_insenstive,
		}
	}
}

impl<T: TableSchema> QueryConverter for CKLogConverter<T> {
	fn convert_condition(&self, c: &Condition) -> String {
		// special case for level
		if matches!(c.column, Column::Level) {
			if let Some(s) = self.convert_level(&c.cmp) {
				return s;
			}
		}
		let col_name = self.column_name(&c.column);
		match &c.cmp {
			Cmp::Equal(v) => format!("{} = {}", col_name, v),
			Cmp::NotEqual(v) => format!("{} != {}", col_name, v),
			Cmp::Larger(v) => format!("{} > {}", col_name, v),
			Cmp::LargerEqual(v) => format!("{} >= {}", col_name, v),
			Cmp::Less(v) => format!("{} < {}", col_name, v),
			Cmp::LessEqual(v) => format!("{} <= {}", col_name, v),
			Cmp::RegexMatch(v) => format!("match({}, '{}')", col_name, v),
			Cmp::RegexNotMatch(v) => {
				format!("NOT match({}, '{}')", col_name, v)
			}
			Cmp::Contains(v) => v
				.split(' ')
				.map(|s| format!("hasToken({}, '{}')", col_name, s))
				.collect_vec()
				.join(" AND "),
			Cmp::NotContains(v) => v
				.split(' ')
				.map(|s| format!("NOT hasToken({}, '{}')", col_name, s))
				.collect_vec()
				.join(" AND "),
		}
	}
	fn convert_timing(
		&self,
		ts_key: &str,
		o: &OrdType,
		t: &NaiveDateTime,
	) -> String {
		let ts = t.and_utc().timestamp();
		match o {
			OrdType::LargerEqual => {
				format!("{}>=toDateTime({})", "TimestampTime".to_string(), ts)
			}
			OrdType::SmallerEqual => {
				format!("{}<=toDateTime({})", "TimestampTime".to_string(), ts)
			}
		}
	}
}

impl<T: TableSchema> CKLogConverter<T> {
	fn convert_level(&self, cmp: &Cmp) -> Option<String> {
		let insensitive = self.level_insenstive;
		let key = self.table.level_key();
		match cmp {
			Cmp::Equal(v) => {
				if insensitive {
					Some(format!("{} ILIKE {}", key, v))
				} else {
					Some(format!("{} = {}", key, v))
				}
			}
			Cmp::NotEqual(v) => {
				if insensitive {
					Some(format!("{} NOT ILIKE {}", key, v))
				} else {
					Some(format!("{} != {}", key, v))
				}
			}
			Cmp::RegexMatch(v) => Some(format!("match({}, '{}')", key, v)),
			Cmp::RegexNotMatch(v) => {
				Some(format!("NOT match({}, '{}')", key, v))
			}
			_ => None,
		}
	}
	fn column_name(&self, c: &Column) -> String {
		match c {
			Column::Message => self.table.msg_key().to_string(),
			Column::Timestamp => self.table.ts_key().to_string(),
			Column::Level => self.table.level_key().to_string(),
			Column::TraceID => self.table.trace_key().to_string(),
			Column::Resources(s) => {
				if self.replace_dash_to_dot {
					format!(
						"{}['{}']",
						self.table.resources_key(),
						s.replace("_", ".")
					)
				} else {
					format!("{}['{}']", self.table.resources_key(), s)
				}
			}
			Column::Attributes(s) => {
				if self.replace_dash_to_dot {
					format!("{}['{}']", self.table.attributes_key(), s)
				} else {
					format!(
						"{}['{}']",
						self.table.attributes_key(),
						s.replace("_", ".")
					)
				}
			}
			Column::Raw(s) => s.clone(),
		}
	}
}
