use chrono::NaiveDateTime;

#[derive(Debug, Default, Clone)]
pub struct TimeRange {
	pub start: Option<NaiveDateTime>,
	pub end: Option<NaiveDateTime>,
}
