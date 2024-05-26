use super::QuickwitServerConfig;
use anyhow::{anyhow, Result};
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
		self.client
			.post(self.cfg.endpoint.clone())
			.json(query)
			.send()
			.await?
			.json()
			.await
			.map_err(|e| anyhow!(e))
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
		self.client
			.post(self.cfg.endpoint.clone())
			.json(&query)
			.send()
			.await?
			.json()
			.await
			.map_err(|e| anyhow!(e))
	}
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
	pub num_hits: u64,
	/// List of hits returned.
	pub hits: Vec<JsonValue>,
	/// List of snippets
	#[serde(skip_serializing_if = "Option::is_none")]
	pub snippets: Option<Vec<JsonValue>>,
	/// Elapsed time.
	pub elapsed_time_micros: u64,
	/// Search errors.
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
}
