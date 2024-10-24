#![allow(dead_code)]

use chrono::NaiveDateTime;
use serde::{
	ser::{SerializeMap, SerializeStruct, Serializer},
	Serialize,
};
use serde_json::Value as JSONValue;

#[derive(Debug, Serialize)]
pub struct ESQuery {
	pub query: BoolQuery,
	pub sort: Option<Sort>,
	pub size: Option<u32>,
}

#[derive(Debug)]
pub enum BoolQuery {
	Filter(Vec<QueryContext>),
	Should(ShouldContext),
}

impl Serialize for BoolQuery {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let mut map = serializer.serialize_map(Some(1))?;
		match self {
			BoolQuery::Filter(queries) => {
				// 使用一个嵌套的map来创建"filter"键
				let filter_map = serde_json::json!({
					"filter": queries
				});
				map.serialize_entry("bool", &filter_map)?;
			}
			BoolQuery::Should(context) => {
				// 使用一个嵌套的map来创建"should"键和"minimum_should_match"
				let should_map = serde_json::json!({
					"should": context.contexts,
					"minimum_should_match": context.minimum_should_match
				});
				map.serialize_entry("bool", &should_map)?;
			}
		}
		map.end()
	}
}

#[derive(Debug)]
pub struct ShouldContext {
	pub contexts: Vec<QueryContext>,
	pub minimum_should_match: usize,
}

#[derive(Debug)]
pub struct TermContext {
	pub field: String,
	pub val: JSONValue,
}

impl Serialize for TermContext {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let mut map = serializer.serialize_map(Some(1))?;
		map.serialize_entry(&self.field, &self.val)?;
		map.end()
	}
}

#[derive(Debug)]
pub struct MatchContext {
	pub field: String,
	pub val: String,
}

impl Serialize for MatchContext {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let mut map = serializer.serialize_map(Some(1))?;
		map.serialize_entry(&self.field, &self.val)?;
		map.end()
	}
}

#[derive(Debug)]
pub enum QueryContext {
	Bool(BoolQuery),
	Term(TermContext),
	Match(MatchContext),
	MatchPhrase(MatchContext),
	Range(Range),
}

impl Serialize for QueryContext {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		match self {
			QueryContext::Bool(query) => {
				// 对于Bool变体，直接使用BoolQuery的序列化结果
				query.serialize(serializer)
			}
			QueryContext::Term(context) => {
				let mut map = serializer.serialize_map(Some(1))?;
				let term_obj = serde_json::json!({ context.field.to_string(): context.val });
				map.serialize_entry("term", &term_obj)?;
				map.end()
			}
			QueryContext::Match(context) => {
				let mut map = serializer.serialize_map(Some(1))?;
				let match_obj = serde_json::json!({ context.field.to_string(): context.val });
				map.serialize_entry("match", &match_obj)?;
				map.end()
			}
			QueryContext::MatchPhrase(context) => {
				let mut map = serializer.serialize_map(Some(1))?;
				let match_phrase_obj = serde_json::json!({ context.field.to_string(): context.val });
				map.serialize_entry("match_phrase", &match_phrase_obj)?;
				map.end()
			}
			QueryContext::Range(rg) => rg.serialize(serializer),
		}
	}
}

impl Serialize for ShouldContext {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let mut map = serializer.serialize_map(Some(2))?;
		map.serialize_entry("should", &self.contexts)?;
		map.serialize_entry(
			"minimum_should_match",
			&self.minimum_should_match,
		)?;
		map.end()
	}
}

#[derive(Debug)]
pub struct Range {
	pub field: String,
	pub gte: Option<NaiveDateTime>,
	pub lte: Option<NaiveDateTime>,
}

impl Serialize for Range {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let mut map = serializer.serialize_map(Some(1))?;
		let mut field_map = std::collections::HashMap::new();

		// 构建内部字段映射
		if let Some(lte) = self.lte {
			field_map
				.insert("lte", lte.format("%Y-%m-%d %H:%M:%S").to_string());
		}
		if let Some(gte) = self.gte {
			field_map
				.insert("gte", gte.format("%Y-%m-%d %H:%M:%S").to_string());
		}

		// 使用字段名作为键，将内部映射作为值
		let range_value =
			std::collections::HashMap::from([(self.field.clone(), field_map)]);

		// 将整个结构序列化为 "range" 键下的值
		map.serialize_entry("range", &range_value)?;
		map.end()
	}
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
	Asc,
	Desc,
}

#[derive(Debug)]
pub struct Sort {
	pub sort: Vec<(String, Order)>,
}

impl Serialize for Sort {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		// 创建一个序列化结构，"sort" 是这个结构的名称，1 表示我们将要序列化一个字段。
		let mut state = serializer.serialize_struct("Sort", 1)?;
		// 由于 sort 字段是一个数组，我们需要手动处理每个元素。
		let sort: Vec<_> = self
			.sort
			.iter()
			.map(|(field_name, order)| {
				// 对于每个元组，我们创建一个 HashMap 来表示 {"tuple.0": "asc|desc"}
				let mut map = std::collections::HashMap::new();
				map.insert(
					field_name.clone(),
					format!("{:?}", order).to_lowercase(),
				);
				map
			})
			.collect();

		// 现在将处理好的 sort 字段添加到序列化结构中。
		state.serialize_field("sort", &sort)?;
		state.end()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use chrono::Utc;
	use pretty_assertions::assert_eq;

	#[test]
	fn test_ser_sort() {
		let test_cases = vec![(
			Sort {
				sort: vec![
					("foo".to_string(), Order::Asc),
					("bar".to_string(), Order::Desc),
				],
			},
			serde_json::json!({
				"sort": [
					{"foo": "asc"},
					{"bar": "desc"}
				]
			}),
		)];
		for (input, expected) in test_cases {
			let serialized = serde_json::to_string(&input).unwrap();
			let actual =
				serde_json::from_str::<serde_json::Value>(&serialized).unwrap();
			assert_eq!(expected, actual, "actual: {}", serialized);
		}
	}

	#[test]
	fn test_ser_range() {
		let test_cases = vec![
			(
				Range {
					field: "field_name".to_string(),
					gte: Some(
						NaiveDateTime::parse_from_str(
							"2024-05-20 13:00:00",
							"%Y-%m-%d %H:%M:%S",
						)
						.unwrap(),
					),
					lte: Some(
						NaiveDateTime::parse_from_str(
							"2024-05-20 15:00:00",
							"%Y-%m-%d %H:%M:%S",
						)
						.unwrap(),
					),
				},
				serde_json::json!({
					"range": {
						"field_name": {
							"gte": "2024-05-20 13:00:00",
							"lte": "2024-05-20 15:00:00"
						}
					}
				}),
			),
			(
				Range {
					field: "foo".to_string(),
					gte: None,
					lte: Some(
						NaiveDateTime::parse_from_str(
							"2024-05-20 15:00:00",
							"%Y-%m-%d %H:%M:%S",
						)
						.unwrap(),
					),
				},
				serde_json::json!({
					"range": {
						"foo": {
							"lte": "2024-05-20 15:00:00"
						}
					}
				}),
			),
			(
				Range {
					field: "bar".to_string(),
					gte: Some(
						NaiveDateTime::parse_from_str(
							"2024-05-20 13:00:00",
							"%Y-%m-%d %H:%M:%S",
						)
						.unwrap(),
					),
					lte: None,
				},
				serde_json::json!({
					"range": {
						"bar": {
							"gte": "2024-05-20 13:00:00"
						}
					}
				}),
			),
		];
		for (range, expected) in test_cases {
			let serialized = serde_json::to_string(&range).unwrap();
			let actual =
				serde_json::from_str::<serde_json::Value>(&serialized).unwrap();
			assert_eq!(expected, actual, "actual: {}", serialized);
		}
	}

	#[test]
	fn test_ser_boolquery() {
		let now = Utc::now().naive_utc();
		let now_fmt = now.format("%Y-%m-%d %H:%M:%S").to_string();
		let test_cases = vec![
			(
				BoolQuery::Filter(vec![
					QueryContext::Term(TermContext {
						field: "foo".to_string(),
						val: JSONValue::String("bar".to_string()),
					}),
					QueryContext::Match(MatchContext {
						field: "baz".to_string(),
						val: "qux".to_string(),
					}),
					QueryContext::Bool(BoolQuery::Should(ShouldContext {
						contexts: vec![
							QueryContext::Term(TermContext {
								field: "quux".to_string(),
								val: JSONValue::String("corge".to_string()),
							}),
							QueryContext::Match(MatchContext {
								field: "grault".to_string(),
								val: "garply".to_string(),
							}),
						],
						minimum_should_match: 1,
					})),
				]),
				serde_json::json!(
					{
						"bool": {
							"filter": [
								{ "term": { "foo": "bar" } },
								{ "match": { "baz": "qux" } },
								{
									"bool": {
										"should": [
											{ "term": { "quux": "corge" } },
											{ "match": { "grault": "garply" } }
										],
										"minimum_should_match": 1
									}
								}
							]
						}
					}
				),
			),
			(
				BoolQuery::Should(ShouldContext {
					contexts: vec![
						QueryContext::Term(TermContext {
							field: "foo".to_string(),
							val: JSONValue::String("bar".to_string()),
						}),
						QueryContext::Match(MatchContext {
							field: "baz".to_string(),
							val: "qux".to_string(),
						}),
					],
					minimum_should_match: 2,
				}),
				serde_json::json!(
					{
						"bool": {
							"should": [
								{ "term": { "foo": "bar" } },
								{ "match": { "baz": "qux" } }
							],
							"minimum_should_match": 2
						}
					}
				),
			),
			(
				BoolQuery::Should(ShouldContext {
					contexts: vec![
						QueryContext::Term(TermContext {
							field: "foo".to_string(),
							val: JSONValue::String("bar".to_string()),
						}),
						QueryContext::Bool(BoolQuery::Filter(vec![
							QueryContext::Term(TermContext {
								field: "baz".to_string(),
								val: JSONValue::String("qux".to_string()),
							}),
							QueryContext::Match(MatchContext {
								field: "quux".to_string(),
								val: "corge".to_string(),
							}),
						])),
					],
					minimum_should_match: 2,
				}),
				serde_json::json!(
					{
						"bool": {
							"should": [
								{ "term": { "foo": "bar" } },
								{
									"bool": {
										"filter": [
											{ "term": { "baz": "qux" } },
											{ "match": { "quux": "corge" } }
										]
									}
								}
							],
							"minimum_should_match": 2
						}
					}
				),
			),
			(
				BoolQuery::Should(ShouldContext {
					contexts: vec![
						QueryContext::Term(TermContext {
							field: "foo".to_string(),
							val: JSONValue::String("bar".to_string()),
						}),
						QueryContext::Bool(BoolQuery::Filter(vec![
							QueryContext::MatchPhrase(MatchContext {
								field: "baz".to_string(),
								val: "qux".to_string(),
							}),
							QueryContext::Match(MatchContext {
								field: "quux".to_string(),
								val: "corge".to_string(),
							}),
						])),
					],
					minimum_should_match: 2,
				}),
				serde_json::json!(
					{
						"bool": {
							"should": [
								{ "term": { "foo": "bar" } },
								{
									"bool": {
										"filter": [
											{ "match_phrase": { "baz": "qux" } },
											{ "match": { "quux": "corge" } }
										]
									}
								}
							],
							"minimum_should_match": 2
						}
					}
				),
			),
			(
				BoolQuery::Should(ShouldContext {
					contexts: vec![
						QueryContext::Term(TermContext {
							field: "foo".to_string(),
							val: JSONValue::String("bar".to_string()),
						}),
						QueryContext::Bool(BoolQuery::Filter(vec![
							QueryContext::MatchPhrase(MatchContext {
								field: "baz".to_string(),
								val: "qux".to_string(),
							}),
							QueryContext::Match(MatchContext {
								field: "quux".to_string(),
								val: "corge".to_string(),
							}),
						])),
						QueryContext::Range(Range {
							field: "ts".to_string(),
							gte: Some(now),
							lte: None,
						}),
					],
					minimum_should_match: 2,
				}),
				serde_json::json!({
					"bool": {
						"should": [
							{ "term": { "foo": "bar" } },
							{
								"bool": {
									"filter": [
										{ "match_phrase": { "baz": "qux" } },
										{ "match": { "quux": "corge" } }
									]
								}
							},
							{
								"range": {
									"ts": {
										"gte": now_fmt,
									}
								}
							}
						],
						"minimum_should_match": 2
					}
				}),
			),
		];
		for (query, expected) in test_cases {
			let serialized = serde_json::to_string(&query).unwrap();
			let actual =
				serde_json::from_str::<serde_json::Value>(&serialized).unwrap();
			assert_eq!(expected, actual, "actual: {}", serialized);
		}
	}
}
