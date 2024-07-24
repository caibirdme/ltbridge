use chrono::NaiveDateTime;

pub mod level;
pub use level::LogLevel;

#[derive(Debug, Default, Clone)]
pub struct TimeRange {
	pub start: Option<NaiveDateTime>,
	pub end: Option<NaiveDateTime>,
}
