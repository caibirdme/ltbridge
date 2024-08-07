use anyhow::{anyhow, Result};

#[derive(Debug, Clone, Hash, Eq, PartialEq, Copy)]
pub enum LogLevel {
	Trace,
	Debug,
	Info,
	Warn,
	Error,
	Fatal,
}

impl LogLevel {
	pub fn all_levels() -> Vec<String> {
		vec![
			LogLevel::Trace.into(),
			LogLevel::Debug.into(),
			LogLevel::Info.into(),
			LogLevel::Warn.into(),
			LogLevel::Error.into(),
			LogLevel::Fatal.into(),
		]
	}
}

static LOG_LEVEL_ENUM: [(&str, LogLevel); 6] = [
	("TRACE", LogLevel::Trace),
	("DEBUG", LogLevel::Debug),
	("INFO", LogLevel::Info),
	("WARN", LogLevel::Warn),
	("ERROR", LogLevel::Error),
	("FATAL", LogLevel::Fatal),
];

impl TryFrom<String> for LogLevel {
	type Error = anyhow::Error;

	fn try_from(value: String) -> Result<Self> {
		let u = value.to_uppercase();
		for (s, lvl) in LOG_LEVEL_ENUM {
			if u.starts_with(s) {
				return Ok(lvl);
			}
		}
		Err(anyhow!("invalid log level: {}", value))
	}
}

impl TryFrom<&str> for LogLevel {
	type Error = anyhow::Error;

	fn try_from(value: &str) -> Result<Self> {
		value.to_string().try_into()
	}
}

impl From<u32> for LogLevel {
	fn from(l: u32) -> Self {
		use LogLevel::*;
		match l {
			..=4 => Trace,
			5..=8 => Debug,
			9..=12 => Info,
			13..=16 => Warn,
			17..=20 => Error,
			21.. => Fatal,
		}
	}
}

impl From<LogLevel> for u32 {
	fn from(val: LogLevel) -> u32 {
		use LogLevel::*;
		match val {
			Trace => 1,
			Debug => 5,
			Info => 9,
			Warn => 13,
			Error => 17,
			Fatal => 21,
		}
	}
}

impl From<LogLevel> for String {
	fn from(val: LogLevel) -> String {
		use LogLevel::*;
		match val {
			Trace => "TRACE".to_string(),
			Debug => "DEBUG".to_string(),
			Info => "INFO".to_string(),
			Warn => "WARN".to_string(),
			Error => "ERROR".to_string(),
			Fatal => "FATAL".to_string(),
		}
	}
}
