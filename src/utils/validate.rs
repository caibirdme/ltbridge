use chrono::DateTime;
use validator::ValidationError;

pub fn unix_timestamp(secs: u64) -> Result<(), ValidationError> {
	DateTime::from_timestamp(secs as i64, 0)
		.ok_or(ValidationError::new("invalid unix timestamp"))
		.map(|_| ())
}
