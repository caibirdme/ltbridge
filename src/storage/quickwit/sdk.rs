use super::QuickwitServerConfig;
use crate::utils::log::ResultLogger;
use anyhow::{anyhow, Result};
use chrono::NaiveDateTime;
use itertools::Itertools;
use reqwest::Client;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

#[derive(Clone)]
pub struct QuickwitSdk {
	client: Client,
	cfg: QuickwitServerConfig,
}

impl QuickwitSdk {
	pub fn new(cfg: QuickwitServerConfig) -> Self {
		let client = Client::builder().timeout(cfg.timeout).build().unwrap();
		Self { client, cfg }
	}
	pub async fn search_records<I>(
		&self,
		query: &I,
	) -> Result<SearchResponseRest>
	where
		I: Serialize,
	{
		let mut p = self.cfg.qw_endpoint.clone();
		p.path_segments_mut().unwrap().push("search");
		let res = self
			.client
			.post(p)
			.json(query)
			.send()
			.await?
			.text()
			.await
			.map_err(|e| anyhow!(e))
			.log_e()?;
		serde_json::from_str(&res).map_err(|e| anyhow!(e)).log_e()
	}
	pub async fn level_aggregation(
		&self,
		mut query: SearcgRequest,
		ts_key: String,
		interval: String,
	) -> Result<VolumeAggrResponse> {
		let aggs = Some(serde_json::json!({
			"volume": {
				"date_histogram": {
					"field": ts_key,
					"fixed_interval": interval,
					"min_doc_count": 1
				},
				"aggs": {
					"levels": {
						"terms": {
							"field": "severity_text",
							"min_doc_count": 1
						}
					}
				}
			}
		}));
		query.aggs = aggs;
		let mut p = self.cfg.qw_endpoint.clone();
		p.path_segments_mut().unwrap().push("search");
		self.client
			.post(p)
			.json(&query)
			.send()
			.await?
			.json()
			.await
			.map_err(|e| anyhow!(e))
			.log_e()
	}
	pub async fn field_caps(&self, ts: TimeRange) -> Result<Vec<String>> {
		let mut p = self.cfg.es_endpoint.clone();
		p.path_segments_mut().unwrap().push("_field_caps");
		let mut qb = self.client.get(p);
		let mut arr = vec![];
		if let Some(start) = ts.start {
			arr.push((
				"start_timestamp",
				start.and_utc().timestamp().to_string(),
			));
		}
		if let Some(end) = ts.end {
			arr.push(("end_timestamp", end.and_utc().timestamp().to_string()));
		}
		if !arr.is_empty() {
			qb = qb.query(arr.as_slice());
		}
		let res: FieldCapResponse = qb
			.send()
			.await?
			.json()
			.await
			.map_err(|e| anyhow!(e))
			.log_e()?;
		Ok(res.fields.keys().cloned().collect_vec())
	}
	pub async fn field_terms(
		&self,
		key: &str,
		ts: TimeRange,
	) -> Result<Vec<String>> {
		let mut body = serde_json::json!({
			"query": "*",
			"max_hits": 0,
			"aggs": {
				"field_vals": {
					"terms": {
						"field": key
					}
				}
			}
		});
		if let Some(start) = ts.start {
			append_key_to_object(
				&mut body,
				"start_timestamp",
				serde_json::json!(start.and_utc().timestamp()),
			);
		}
		if let Some(end) = ts.end {
			append_key_to_object(
				&mut body,
				"end_timestamp",
				serde_json::json!(end.and_utc().timestamp()),
			);
		}
		let mut p = self.cfg.qw_endpoint.clone();
		p.path_segments_mut().unwrap().push("search");
		let ftr: FieldTermsResponse = self
			.client
			.post(p)
			.json(&body)
			.send()
			.await?
			.json()
			.await
			.map_err(|e| anyhow!(e))
			.log_e()?;
		Ok(ftr
			.aggregations
			.get("field_vals")
			.map_or_else(Vec::new, |agg| {
				agg.buckets.iter().map(|b| b.key.clone()).collect()
			}))
	}
}

fn append_key_to_object(
	obj: &mut serde_json::Value,
	key: &str,
	val: serde_json::Value,
) {
	if let serde_json::Value::Object(m) = obj {
		m.insert(key.to_string(), val);
	}
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct FieldTermsResponse {
	pub aggregations: HashMap<String, FieldTermsAggEle>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct FieldTermsAggEle {
	pub buckets: Vec<TermBucketElem>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TermBucketElem {
	pub key: String,
	pub doc_count: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct FieldCapResponse {
	pub indices: Vec<String>,
	// actually we only care about the keys
	pub fields: HashMap<String, JsonValue>,
}

pub struct TimeRange {
	pub start: Option<NaiveDateTime>,
	pub end: Option<NaiveDateTime>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct VolumeAggrResponse {
	pub num_hits: u64,
	pub aggregations: Aggregations,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Aggregations {
	pub volume: TopLevel,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct TopLevel {
	pub buckets: Vec<Bucket>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Bucket {
	pub key: f64,
	pub key_as_string: String,
	pub levels: Levels,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Levels {
	pub buckets: Vec<LevelBucket>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct LevelBucket {
	pub doc_count: u32,
	pub key: String,
}

#[derive(Serialize, Debug, Default)]
pub struct SearcgRequest {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub aggs: Option<JsonValue>,
	pub query: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub max_hits: Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub start_timestamp: Option<i64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub end_timestamp: Option<i64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub sort_by: Option<SortBy>,
}

#[derive(Debug)]
pub struct SortBy {
	pub fields: Vec<SortField>,
}

impl Serialize for SortBy {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let mut s = String::new();
		for (i, field) in self.fields.iter().enumerate() {
			if i > 0 {
				s.push(',');
			}
			if let SortOrder::Asc = field.order {
				s.push('-')
			}
			s.push_str(&field.field);
		}
		serializer.serialize_str(&s)
	}
}

#[derive(Debug)]
pub enum SortOrder {
	Asc,
	Desc,
}

#[derive(Debug)]
pub struct SortField {
	pub field: String,
	pub order: SortOrder,
}

/// SearchResponseRest represents the response returned by the REST search API
/// and is meant to be serialized into JSON.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SearchResponseRest {
	/// Overall number of documents matching the query.
	#[serde(default)]
	pub num_hits: u64,
	/// List of hits returned.
	#[serde(default)]
	pub hits: Vec<JsonValue>,
	/// List of snippets
	#[serde(skip_serializing_if = "Option::is_none")]
	pub snippets: Option<Vec<JsonValue>>,
	/// Elapsed time.
	pub elapsed_time_micros: u64,
	/// Search errors.
	#[serde(default)]
	pub errors: Vec<String>,
	/// Aggregations.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub aggregations: Option<JsonValue>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogRecord {
	pub timestamp_nanos: u64,
	#[serde(default)]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub observed_timestamp_nanos: Option<u64>,
	#[serde(default)]
	#[serde(skip_serializing_if = "String::is_empty")]
	pub service_name: String,
	#[serde(default)]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub severity_text: Option<String>,
	pub severity_number: i32,
	#[serde(default)]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub body: Option<JsonValue>,
	#[serde(default)]
	#[serde(skip_serializing_if = "HashMap::is_empty")]
	pub attributes: HashMap<String, JsonValue>,
	#[serde(default)]
	#[serde(skip_serializing_if = "is_zero")]
	pub dropped_attributes_count: u32,
	#[serde(default)]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub trace_id: Option<String>,
	#[serde(default)]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub span_id: Option<String>,
	#[serde(default)]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub trace_flags: Option<u32>,
	#[serde(default)]
	#[serde(skip_serializing_if = "HashMap::is_empty")]
	pub resource_attributes: HashMap<String, JsonValue>,
	#[serde(default)]
	#[serde(skip_serializing_if = "is_zero")]
	pub resource_dropped_attributes_count: u32,
	#[serde(default)]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub scope_name: Option<String>,
	#[serde(default)]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub scope_version: Option<String>,
	#[serde(default)]
	#[serde(skip_serializing_if = "HashMap::is_empty")]
	pub scope_attributes: HashMap<String, JsonValue>,
	#[serde(default)]
	#[serde(skip_serializing_if = "is_zero")]
	pub scope_dropped_attributes_count: u32,
}

fn is_zero(count: &u32) -> bool {
	*count == 0
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_de_aggs() {
		let j = r#"{
			"num_hits": 1,
			"hits": [],
			"elapsed_time_micros": 5545,
			"errors": [],
			"aggregations": {
				"volume": {
					"buckets": [
						{
							"doc_count": 40,
							"key": 1617235200000,
							"key_as_string": "2021-04-01T00:00:00Z",
							"levels": {
								"buckets": [
									{
										"doc_count": 1,
										"key": "INFO"
									}
								],
								"doc_count_error_upper_bound": 0,
								"sum_other_doc_count": 0
							}
						}
					]
				}
			}
		}"#;
		let actual: VolumeAggrResponse = serde_json::from_str(j).unwrap();
		let expect = VolumeAggrResponse {
			num_hits: 1,
			aggregations: Aggregations {
				volume: TopLevel {
					buckets: vec![Bucket {
						key: 1617235200000.0,
						key_as_string: "2021-04-01T00:00:00Z".to_string(),
						levels: Levels {
							buckets: vec![LevelBucket {
								doc_count: 1,
								key: "INFO".to_string(),
							}],
						},
					}],
				},
			},
		};
		assert_eq!(expect, actual);
	}

	#[test]
	fn deser_search_resp() {
		let v = r#"
		{
			"num_hits": 1,
			"hits": [
			  {
				"body": {
				  "message": "Targeted ad request received for [books]"
				},
				"observed_timestamp_nanos": 1717159904229798000,
				"resource_attributes": {
				  "container.id": "a6db066c52885f810d579a27c4fd69f6d5f5bd5939f5233206660002f63bd6f8",
				  "host.arch": "amd64",
				  "host.name": "a6db066c5288",
				  "os.description": "Linux 6.6.6-2401.0.1.tl4.4.x86_64",
				  "os.type": "linux",
				  "process.command_line": "/opt/java/openjdk/bin/java -javaagent:/usr/src/app/opentelemetry-javaagent.jar oteldemo.AdService",
				  "process.executable.path": "/opt/java/openjdk/bin/java",
				  "process.pid": 1,
				  "process.runtime.description": "Eclipse Adoptium OpenJDK 64-Bit Server VM 21.0.3+9-LTS",
				  "process.runtime.name": "OpenJDK Runtime Environment",
				  "process.runtime.version": "21.0.3+9-LTS",
				  "service.instance.id": "aa35fd6a-5766-48f0-9c55-fd2d607f23e8",
				  "service.namespace": "opentelemetry-demo",
				  "telemetry.distro.name": "opentelemetry-java-instrumentation",
				  "telemetry.distro.version": "2.3.0",
				  "telemetry.sdk.language": "java",
				  "telemetry.sdk.name": "opentelemetry",
				  "telemetry.sdk.version": "1.37.0"
				},
				"scope_name": "oteldemo.AdService",
				"service_name": "adservice",
				"severity_number": 9,
				"severity_text": "INFO",
				"span_id": "3ebd111788850591",
				"timestamp_nanos": 1717159904229794000,
				"trace_flags": 1,
				"trace_id": "c37c9dd34eedbc7f1acf96a63fadf9a0"
			  }
			],
			"elapsed_time_micros": 21011,
			"errors": []
		  }
		"#;
		let actual: SearchResponseRest = serde_json::from_str(v).unwrap();
		assert_eq!(1, actual.num_hits);
	}
}
