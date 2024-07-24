use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::{env, time::Duration};

#[derive(Clone, Deserialize)]
pub struct AppConfig {
	pub server: Server,
	pub log_source: DataSource,
	pub trace_source: DataSource,
}

#[derive(Clone, Deserialize)]
pub struct Log {
	pub level: String,
	pub file: String,
	// see https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives
	pub filter_directives: String,
}

impl Default for Log {
	fn default() -> Self {
		Self {
			level: "info".to_string(),
			file: "info.log".to_string(),
			filter_directives: "info".to_string(),
		}
	}
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
pub struct Quickwit {
	pub domain: String,
	pub index: String,
	#[serde(with = "humantime_serde")]
	#[serde(default = "default_query_timeout")]
	pub timeout: Duration, // seconds
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
pub struct Databend {
	#[serde(default = "default_driver")]
	pub driver: String,
	pub domain: String,
	pub port: u16,
	pub database: String,
	pub username: String,
	pub password: String,
	#[serde(default = "default_ssl_mode")]
	pub ssl_mode: bool,
	#[serde(with = "humantime_serde")]
	#[serde(default = "default_connect_timeout")]
	pub connect_timeout: Duration, // seconds
	#[serde(default)]
	pub inverted_index: bool,
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
pub struct Clickhouse {
	pub url: String,
	pub database: String,
	pub username: String,
	pub password: String,
	pub table: String,
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
pub enum DataSource {
	#[serde(rename = "databend")]
	Databend(Databend),
	#[serde(rename = "quickwit")]
	Quickwit(Quickwit),
	#[serde(rename = "clickhouse")]
	Clickhouse(Clickhouse),
}

fn default_driver() -> String {
	"databend".to_string()
}
const fn default_ssl_mode() -> bool {
	false
}

const fn default_query_timeout() -> Duration {
	Duration::from_secs(60)
}

const fn default_connect_timeout() -> Duration {
	Duration::from_secs(10)
}

// databend dns, for details see https://github.com/datafuselabs/bendsql?tab=readme-ov-file#dsn
impl From<Databend> for String {
	fn from(value: Databend) -> Self {
		format!(
			"{}://{}:{}@{}:{}/{}?sslmode={}&connect_timeout={}",
			value.driver,
			value.username,
			value.password,
			value.domain,
			value.port,
			value.database,
			if value.ssl_mode { "enable" } else { "disable" },
			value.connect_timeout.as_secs(),
		)
	}
}

impl TryFrom<Databend> for databend_driver::Client {
	type Error = databend_driver::Error;

	fn try_from(value: Databend) -> Result<Self, Self::Error> {
		let client = databend_driver::Client::new(String::from(value));
		Ok(client)
	}
}

#[derive(Clone, Deserialize)]
pub struct Server {
	pub listen_addr: String,
	#[serde(with = "humantime_serde")]
	pub timeout: Duration,
	pub log: Log,
}

impl AppConfig {
	pub fn new() -> Result<Self, ConfigError> {
		let default_config =
			env::var("LGTMRS_CONFIG").unwrap_or("config.yaml".to_string());
		Config::builder()
			.add_source(File::with_name(&default_config))
			.build()?
			.try_deserialize()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use pretty_assertions::assert_eq;

	#[test]
	fn test_quickwit_enum() {
		let j = serde_json::json!({
			"quickwit": {
				"domain": "http://localhost:1234",
				"index": "xxx_index",
				"timeout": "300s",
			}}
		);
		let actual = serde_json::from_value(j).unwrap();
		let expect = DataSource::Quickwit(Quickwit {
			domain: "http://localhost:1234".to_string(),
			index: "xxx_index".to_string(),
			timeout: Duration::from_secs(300),
		});
		assert_eq!(expect, actual);
	}

	#[test]
	fn test_databend_enum() {
		let j = r#"
		{
			"databend": {
				"driver": "databend",
				"domain": "localhost",
				"port": 3306,
				"database":"db",
				"username": "root",
				"password": "password",
				"inverted_index": true
			}
		}
		"#;
		let cfg = serde_json::from_str::<DataSource>(j).unwrap();
		let expect = DataSource::Databend(Databend {
			driver: "databend".to_string(),
			domain: "localhost".to_string(),
			port: 3306,
			database: "db".to_string(),
			username: "root".to_string(),
			password: "password".to_string(),
			ssl_mode: false,
			connect_timeout: Duration::from_secs(10),
			inverted_index: true,
		});
		assert_eq!(cfg, expect);
	}
}
