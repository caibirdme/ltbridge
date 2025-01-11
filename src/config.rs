use config::{Config, ConfigError, File};
use serde::Deserialize;
use std::{env, net::SocketAddr, str::FromStr, time::Duration};
use tracing_subscriber::filter::Builder;
use validator::{Validate, ValidationError};

#[derive(Clone, Deserialize, Validate)]
pub struct AppConfig {
	#[validate(nested)]
	pub server: Server,
	#[serde(default = "default_cache")]
	#[validate(nested)]
	pub cache: Cache,
	pub log_source: DataSource,
	pub trace_source: DataSource,
}

#[derive(Clone, Deserialize, Default, Validate)]
#[validate(schema(function = "validate_cache_config"))]
pub struct Cache {
	#[serde(default = "default_cache_max_capacity")]
	pub max_capacity: u64,
	#[serde(with = "humantime_serde", default = "default_cache_duration")]
	pub time_to_live: Duration,
	#[serde(with = "humantime_serde", default = "default_cache_duration")]
	pub time_to_idle: Duration,
}

fn validate_cache_config(cfg: &Cache) -> Result<(), ValidationError> {
	if cfg.time_to_idle > cfg.time_to_live {
		return Err(ValidationError::new(
			"time_to_idle must be no greater than time_to_live",
		));
	}
	Ok(())
}

const fn default_cache() -> Cache {
	Cache {
		max_capacity: default_cache_max_capacity(),
		time_to_live: default_cache_duration(),
		time_to_idle: default_cache_duration(),
	}
}

const fn default_cache_max_capacity() -> u64 {
	// 1GB
	1024 * 1024 * 1024
}

const fn default_cache_duration() -> Duration {
	Duration::from_secs(2 * 60)
}

#[derive(Clone, Deserialize, Validate)]
pub struct Log {
	pub file: String,
	// see https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives
	#[validate(custom(function = "validate_log_filter_directives"))]
	pub filter_directives: String,
}

impl Default for Log {
	fn default() -> Self {
		Self {
			file: "info.log".to_string(),
			filter_directives: "info".to_string(),
		}
	}
}

fn validate_log_filter_directives(dirs: &str) -> Result<(), ValidationError> {
	Builder::default()
		.parse(dirs)
		.map_err(|_| ValidationError::new("invalid log filter directives"))
		.map(|_| ())
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
pub struct ClickhouseTrace {
	#[serde(flatten)]
	pub common: Clickhouse,
	pub trace_ts_table: String,
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
pub struct CKLogLabel {
	#[serde(rename = "resources", default = "empty_vec")]
	pub resource_attributes: Vec<String>,
	#[serde(rename = "attributes", default = "empty_vec")]
	pub log_attributes: Vec<String>,
}

fn empty_vec() -> Vec<String> {
	vec![]
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
pub struct ClickhouseLog {
	#[serde(flatten)]
	pub common: Clickhouse,
	pub label: CKLogLabel,
	pub replace_dash_to_dot: Option<bool>,
	#[serde(default = "default_log_level")]
	pub default_log_level: String,
	pub level_case_sensitive: Option<bool>,
}

fn default_log_level() -> String {
	"info".to_string()
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
pub enum ClickhouseConf {
	#[serde(rename = "trace")]
	Trace(ClickhouseTrace),
	#[serde(rename = "log")]
	Log(ClickhouseLog),
}

#[derive(Clone, Deserialize, PartialEq, Eq, Debug)]
pub enum DataSource {
	#[serde(rename = "databend")]
	Databend(Databend),
	#[serde(rename = "quickwit")]
	Quickwit(Quickwit),
	#[serde(rename = "clickhouse")]
	Clickhouse(ClickhouseConf),
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

#[derive(Clone, Deserialize, Validate)]
pub struct Server {
	#[validate(custom(function = "validate_ip_addr"))]
	pub listen_addr: String,
	#[serde(with = "humantime_serde")]
	pub timeout: Duration,
	#[validate(nested)]
	pub log: Log,
}

fn validate_ip_addr(addr: &str) -> Result<(), ValidationError> {
	SocketAddr::from_str(addr)
		.map_err(|_| ValidationError::new("invalid bind address"))
		.map(|_| ())
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
	fn test_deser_cklog() {
		let j = r#"
		{
			"log": {
				"url": "http://127.0.0.1:8123",
				"database": "default",
				"table": "otel_logs",
				"username": "default",
				"password": "a11221122a",
				"label": {
					"resources": ["a"],
					"attributes": ["b"]
				}
			}
		}"#;
		let actual = serde_json::from_str::<ClickhouseConf>(j).unwrap();
		let expect = ClickhouseConf::Log(ClickhouseLog {
			common: Clickhouse {
				url: "http://127.0.0.1:8123".to_string(),
				database: "default".to_string(),
				table: "otel_logs".to_string(),
				username: "default".to_string(),
				password: "a11221122a".to_string(),
			},
			label: CKLogLabel {
				resource_attributes: vec!["a".to_string()],
				log_attributes: vec!["b".to_string()],
			},
			replace_dash_to_dot: None,
			default_log_level: "info".to_string(),
			level_case_sensitive: None,
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

	#[test]
	fn test_decode_whole_file() -> anyhow::Result<()> {
		let cfg: AppConfig = Config::builder()
			.add_source(File::with_name("./config.yaml"))
			.build()?
			.try_deserialize()?;
		let exp = ClickhouseLog {
			common: Clickhouse {
				url: "http://127.0.0.1:8123".to_string(),
				database: "default".to_string(),
				table: "otel_logs".to_string(),
				username: "default".to_string(),
				password: "a11221122a".to_string(),
			},
			label: CKLogLabel {
				resource_attributes: vec![
					"host.arch".to_string(),
					"telemetry.sdk.version".to_string(),
					"process.runtime.name".to_string(),
				],
				log_attributes: vec![
					"quantity".to_string(),
					"code.function".to_string(),
				],
			},
			replace_dash_to_dot: Some(true),
			default_log_level: "debug".to_string(),
			level_case_sensitive: Some(false),
		};
		assert_eq!(
			cfg.log_source,
			DataSource::Clickhouse(ClickhouseConf::Log(exp))
		);
		assert_eq!(cfg.cache.max_capacity, default_cache_max_capacity());
		assert_eq!(cfg.cache.time_to_live, default_cache_duration());
		Ok(())
	}

	#[test]
	fn test_cache_config() -> anyhow::Result<()> {
		let cfg: AppConfig = Config::builder()
			.add_source(File::with_name("./testdata/config/cache.yaml"))
			.build()?
			.try_deserialize()?;

		assert_eq!(cfg.cache.max_capacity, default_cache_max_capacity());
		assert_eq!(cfg.cache.time_to_live, Duration::from_secs(10 * 60));
		assert_eq!(cfg.cache.time_to_idle, default_cache_duration());
		Ok(())
	}

	#[test]
	fn test_whole_file_validation() -> anyhow::Result<()> {
		let cfg: AppConfig = Config::builder()
			.add_source(File::with_name("./config.yaml"))
			.build()?
			.try_deserialize()?;
		cfg.validate()?;
		Ok(())
	}

	#[test]
	fn test_cache_config_validate() {
		let test_cases = vec![
			(
				Cache {
					max_capacity: default_cache_max_capacity(),
					time_to_live: Duration::from_secs(10 * 60),
					time_to_idle: default_cache_duration(),
				},
				1,
			),
			(
				Cache {
					max_capacity: default_cache_max_capacity(),
					time_to_live: Duration::from_secs(10 * 60),
					time_to_idle: default_cache_duration(),
				},
				0,
			),
			(
				Cache {
					max_capacity: default_cache_max_capacity(),
					time_to_live: Duration::from_secs(10 * 60),
					time_to_idle: default_cache_duration(),
				},
				0,
			),
		];
		for (i, (input, expect)) in test_cases.into_iter().enumerate() {
			let actual = input.validate();
			if expect > 0 {
				assert!(actual.is_err(), "case {}", i);
			} else {
				assert!(actual.is_ok(), "case {}", i);
			}
		}
	}

	#[test]
	fn test_server_config_validate() {
		let test_cases = vec![
			(
				Server {
					listen_addr: "0.0.0.0:6778".to_string(),
					timeout: Duration::from_secs(30),
					log: Log::default(),
				},
				0,
			),
			(
				Server {
					listen_addr: ":6778".to_string(),
					timeout: Duration::from_secs(30),
					log: Log::default(),
				},
				1,
			),
			(
				Server {
					listen_addr: "0.0.0.0".to_string(),
					timeout: Duration::from_secs(30),
					log: Log::default(),
				},
				1,
			),
			(
				Server {
					listen_addr: "0.0.0.0:6778".to_string(),
					timeout: Duration::from_secs(30),
					log: Log {
						file: "info.log".to_string(),
						filter_directives: "wtf,,;asd".to_string(),
					},
				},
				1,
			),
		];
		for (i, (input, expect)) in test_cases.into_iter().enumerate() {
			let actual = input.validate();
			if expect > 0 {
				assert!(actual.is_err(), "case {}", i);
			} else {
				assert!(actual.is_ok(), "case {}, err: {:?}", i, actual);
			}
		}
	}
}
