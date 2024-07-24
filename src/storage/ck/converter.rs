use chrono::NaiveDateTime;
use itertools::Itertools as _;
use sqlbuilder::builder::*;

pub struct CKLogConverter<T: TableSchema> {
	table: T,
}

impl<T: TableSchema> CKLogConverter<T> {
	pub fn new(table: T) -> Self {
		Self { table }
	}
}

impl<T: TableSchema> QueryConverter for CKLogConverter<T> {
	fn convert_condition(&self, c: &Condition) -> String {
		let col_name = column_name(&self.table, &c.column);
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
