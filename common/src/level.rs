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

impl TryFrom<String> for LogLevel {
	type Error = anyhow::Error;

	fn try_from(value: String) -> Result<Self> {
		use LogLevel::*;
		let u = value.to_uppercase();
		match u.as_str() {
			"TRACE" => Ok(Trace),
			"DEBUG" => Ok(Debug),
			"INFO" => Ok(Info),
			"WARN" => Ok(Warn),
			"ERROR" => Ok(Error),
			"FATAL" => Ok(Fatal),
			_ => {
				if u.starts_with("TRACE") {
					Ok(Trace)
				} else if u.starts_with("DEBUG") {
					Ok(Debug)
				} else if u.starts_with("INFO") {
					Ok(Info)
				} else if u.starts_with("WARN") {
					Ok(Warn)
				} else if u.starts_with("ERROR") {
					Ok(Error)
				} else if u.starts_with("FATAL") {
					Ok(Fatal)
				} else {
					Err(anyhow!("Invalid log level: {}", value))
				}
			},
		}
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