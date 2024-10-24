use crate::storage::trace::{
	Links as BLinks, SpanEvent as BSpanEvent, SpanItem,
};
use opentelemetry_proto::tonic::{
	common::v1::{
		any_value::Value, AnyValue, ArrayValue, InstrumentationScope, KeyValue,
		KeyValueList,
	},
	resource::v1::Resource,
	trace::v1::{
		span::{Event, Link, SpanKind},
		ResourceSpans, ScopeSpans, Span, Status,
	},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use std::{collections::HashMap, time::Duration};

mod search;
mod traceid;

pub(crate) use search::{search_tag_values, search_tags, search_trace_v2};
pub(crate) use traceid::get_trace_by_id;

fn spanevent_into_otlp_event(value: &BSpanEvent) -> Event {
	Event {
		time_unix_nano: value.ts.timestamp_nanos_opt().unwrap() as u64,
		name: value.name.clone(),
		attributes: hash_into_kv_pairs(value.attributes.clone()),
		dropped_attributes_count: 0,
	}
}

fn links_into_otlp_link(value: &BLinks) -> Link {
	Link {
		trace_id: hex::decode(&value.trace_id).unwrap_or_default(),
		span_id: hex::decode(&value.span_id).unwrap_or_default(),
		trace_state: value.trace_state.clone(),
		attributes: hash_into_kv_pairs(value.attributes.clone()),
		dropped_attributes_count: 0,
		flags: 0,
	}
}

fn spanitem_into_resourcespans(value: &SpanItem) -> ResourceSpans {
	ResourceSpans {
		scope_spans: vec![trace_2_scope_span(value)],
		resource: Some(Resource {
			attributes: hash_into_kv_pairs(value.resource_attributes.clone()),
			dropped_attributes_count: 0,
		}),
		schema_url: SCHEMA_URL.to_string(),
	}
}

fn trace_2_scope_span(value: &SpanItem) -> ScopeSpans {
	ScopeSpans {
		scope: spanitem_into_instrumentation_scope(value),
		spans: vec![spanitem_to_otlp_span(value)],
		schema_url: SCHEMA_URL.to_string(),
	}
}

fn spanitem_to_otlp_span(value: &SpanItem) -> Span {
	Span {
		trace_id: hex::decode(&value.trace_id).unwrap_or_default(),
		span_id: hex::decode(&value.span_id).unwrap_or_default(),
		trace_state: value.trace_state.clone(),
		parent_span_id: hex::decode(&value.parent_span_id).unwrap_or_default(),
		flags: 0,
		name: value.span_name.clone(),
		kind: SpanKind::try_from(value.span_kind).unwrap().into(),
		start_time_unix_nano: value.ts.timestamp_nanos_opt().unwrap() as u64,
		end_time_unix_nano: (value.ts
			+ Duration::from_nanos(value.duration as u64))
		.timestamp_nanos_opt()
		.unwrap() as u64,
		attributes: hash_into_kv_pairs(value.span_attributes.clone()),
		dropped_attributes_count: 0,
		events: value
			.span_events
			.iter()
			.map(spanevent_into_otlp_event)
			.collect(),
		dropped_events_count: 0,
		links: value.link.iter().map(links_into_otlp_link).collect(),
		dropped_links_count: 0,
		status: Some(Status {
			code: value.status_code.unwrap_or_default(),
			message: value.status_message.clone().unwrap_or_default(),
		}),
	}
}

fn spanitem_into_instrumentation_scope(
	value: &SpanItem,
) -> Option<InstrumentationScope> {
	if value.scope_name.is_none() && value.scope_version.is_none() {
		return None;
	}
	Some(InstrumentationScope {
		name: value.scope_name.clone().unwrap_or_default(),
		version: value.scope_version.clone().unwrap_or_default(),
		attributes: vec![],
		dropped_attributes_count: 0,
	})
}

fn hash_into_kv_pairs(
	hash: HashMap<String, serde_json::Value>,
) -> Vec<KeyValue> {
	hash.into_iter()
		.map(|(k, v)| KeyValue {
			key: k,
			value: match v {
				serde_json::Value::Null => None,
				_ => json_value_to_opt_pb_any_value(v),
			},
		})
		.collect()
}

pub fn json_value_to_pb_any_value(jv: serde_json::Value) -> AnyValue {
	use serde_json::value::Value::*;
	match jv {
		Null => AnyValue { value: None },
		Bool(b) => AnyValue {
			value: Some(Value::BoolValue(b)),
		},
		Number(n) => {
			if n.is_f64() {
				AnyValue {
					value: Some(Value::DoubleValue(n.as_f64().unwrap())),
				}
			} else if n.is_i64() {
				AnyValue {
					value: Some(Value::IntValue(n.as_i64().unwrap())),
				}
			} else {
				AnyValue {
					value: Some(Value::IntValue(n.as_u64().unwrap() as i64)),
				}
			}
		}
		String(s) => AnyValue {
			value: Some(Value::StringValue(s)),
		},
		Array(arr) => AnyValue {
			value: Some(Value::ArrayValue(ArrayValue {
				values: arr
					.into_iter()
					.map(json_value_to_pb_any_value)
					.collect(),
			})),
		},
		Object(obj) => AnyValue {
			value: Some(Value::KvlistValue(KeyValueList {
				values: obj
					.into_iter()
					.map(|(k, v)| KeyValue {
						key: k,
						value: json_value_to_opt_pb_any_value(v),
					})
					.collect(),
			})),
		},
	}
}

pub fn json_value_to_opt_pb_any_value(
	jv: serde_json::Value,
) -> Option<AnyValue> {
	use serde_json::value::Value::*;
	match jv {
		Null => None,
		_ => Some(json_value_to_pb_any_value(jv)),
	}
}

#[cfg(test)]
mod tests {
	use crate::storage::trace::SpanEvent;

	#[test]
	fn deser_span_events() {
		let json = r#"{"attributes":{"ctx.deadline":"999.918375ms","message.detail":"{\"msg\":\"caibirdme\"}","message.uncompressed_size":19},"dropped_attributes_count":0,"name":"SENT","time_unix_nano":"2024-04-21T09:20:12.167916Z"}
		"#;
		let events: SpanEvent = serde_json::from_str(json).unwrap();
		assert_eq!(events.name, "SENT");
	}
}
