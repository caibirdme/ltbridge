use chrono::NaiveDateTime;
use sqlbuilder::builder::*;

#[derive(Clone)]
pub struct DatabendConverter {}

impl QueryConverter for DatabendConverter {
	fn convert_condition(c: &Condition) -> String {
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
}

pub fn micro_time(t: &NaiveDateTime) -> String {
	t.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}
