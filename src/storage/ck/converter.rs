use chrono::NaiveDateTime;
use itertools::Itertools as _;
use sqlbuilder::builder::*;

pub struct CKLogConverter<T: TableSchema> {
	table: T,
	replace_dash_to_dot: bool,
}

impl<T: TableSchema> CKLogConverter<T> {
	pub fn new(table: T, replace_dash_to_dot: bool) -> Self {
		Self {
			table,
			replace_dash_to_dot,
		}
	}
}

impl<T: TableSchema> QueryConverter for CKLogConverter<T> {
	fn convert_condition(&self, c: &Condition) -> String {
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
				format!("{}>=toDateTime64({}, 9)", ts_key, ts)
			}
			OrdType::SmallerEqual => {
				format!("{}<=toDateTime64({}, 9)", ts_key, ts)
			}
		}
	}
}

impl<T: TableSchema> CKLogConverter<T> {
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
