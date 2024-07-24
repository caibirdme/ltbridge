use super::{log::LogTable, trace::TraceTable};
use chrono::NaiveDateTime;
use sqlbuilder::builder::*;

#[derive(Clone)]
pub struct DatabendLogConverter {
	table: LogTable,
}

impl DatabendLogConverter {
	pub fn new(table: LogTable) -> Self {
		Self { table }
	}
}

fn column_name(obj: &impl TableSchema, c: &Column) -> String {
	match c {
		Column::Message => obj.msg_key().to_string(),
		Column::Timestamp => obj.ts_key().to_string(),
		Column::Level => obj.level_key().to_string(),
		Column::TraceID => obj.trace_key().to_string(),
		Column::Resources(s) => format!("{}['{}']", obj.resources_key(), s),
		Column::Attributes(s) => format!("{}['{}']", obj.attributes_key(), s),
		Column::Raw(s) => s.clone(),
	}
}

impl QueryConverter for DatabendLogConverter {
	fn convert_condition(&self, c: &Condition) -> String {
		let col_name = column_name(&self.table, &c.column);
		match &c.cmp {
			Cmp::Equal(v) => format!("{} = {}", col_name, v),
			Cmp::NotEqual(v) => format!("{} != {}", col_name, v),
			Cmp::Larger(v) => format!("{} > {}", col_name, v),
			Cmp::LargerEqual(v) => format!("{} >= {}", col_name, v),
			Cmp::Less(v) => format!("{} < {}", col_name, v),
			Cmp::LessEqual(v) => format!("{} <= {}", col_name, v),
			Cmp::RegexMatch(v) => format!("{} REGEXP '{}'", col_name, v),
			Cmp::RegexNotMatch(v) => format!("{} NOT REGEXP '{}'", col_name, v),
			Cmp::Contains(v) => {
				if self.table.use_inverted_index {
					format!("MATCH({},'{}')", col_name, v)
				} else {
					format!("{} LIKE '%{}%'", col_name, v)
				}
			}
			Cmp::NotContains(v) => {
				if self.table.use_inverted_index {
					format!("NOT MATCH({},'{}')", col_name, v)
				} else {
					format!("{} NOT LIKE '%{}%'", col_name, v)
				}
			}
		}
	}

	fn convert_timing(
		&self,
		ts_key: &str,
		o: &OrdType,
		t: &NaiveDateTime,
	) -> String {
		convert_timing(ts_key, o, t)
	}
}

fn convert_timing(ts_key: &str, o: &OrdType, t: &NaiveDateTime) -> String {
	let ts = micro_time(t);
	match o {
		OrdType::LargerEqual => {
			format!("{}>='{}'", ts_key, ts)
		}
		OrdType::SmallerEqual => {
			format!("{}<='{}'", ts_key, ts)
		}
	}
}

pub fn micro_time(t: &NaiveDateTime) -> String {
	t.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

#[derive(Clone)]
pub struct DatabendTraceConverter {
	table: TraceTable,
}

impl DatabendTraceConverter {
	pub fn new(table: TraceTable) -> Self {
		Self { table }
	}
}

impl QueryConverter for DatabendTraceConverter {
	fn convert_condition(&self, c: &Condition) -> String {
		let col_name = column_name(&self.table, &c.column);
		match &c.cmp {
			Cmp::Equal(v) => format!("{} = {}", col_name, v),
			Cmp::NotEqual(v) => format!("{} != {}", col_name, v),
			Cmp::Larger(v) => format!("{} > {}", col_name, v),
			Cmp::LargerEqual(v) => format!("{} >= {}", col_name, v),
			Cmp::Less(v) => format!("{} < {}", col_name, v),
			Cmp::LessEqual(v) => format!("{} <= {}", col_name, v),
			Cmp::RegexMatch(v) => format!("{} REGEXP '{}'", col_name, v),
			Cmp::RegexNotMatch(v) => format!("{} NOT REGEXP '{}'", col_name, v),
			Cmp::Contains(v) => format!("{} LIKE '%{}%'", col_name, v),
			Cmp::NotContains(v) => format!("{} NOT LIKE '%{}%'", col_name, v),
		}
	}

	fn convert_timing(
		&self,
		ts_key: &str,
		o: &OrdType,
		t: &NaiveDateTime,
	) -> String {
		convert_timing(ts_key, o, t)
	}
}
