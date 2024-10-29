use std::{cmp::Ordering, sync::Arc};

use super::*;
use crate::{errors::AppError, state::AppState};
use axum::{
	extract::{rejection::QueryRejection, Path, Query, State},
	Json,
};
use common::TimeRange;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use logql::parser;
use moka::{sync::Cache, Expiry};
use serde::de::DeserializeOwned;
use tokio::time::{interval_at, Instant};
use tracing::{debug, error};

// since tag key and value of any services may contain '/' '-' '|' ...
// we use '---' '|||' to split them
const KEY_SPLITER: &str = "---";
const PAIR_SPLITER: &str = "|||";
const SERIES_CACHE_KEY: &str = "srs";
const LABELS_CACHE_KEY: &str = "lbs";
const LABEL_VALUES_CACHE_KEY_PREFIX: &str = "lbvs:";

pub async fn query_labels(
	State(state): State<AppState>,
	_: Query<QueryLabelsRequest>,
) -> Result<QueryLabelsResponse, AppError> {
	let cache = state.cache;
	if let Some(c) = cache.get(LABELS_CACHE_KEY) {
		return deserialize_from_slice(&c);
	}
	let labels = state
		.log_handle
		.labels(QueryLimits {
			limit: None,
			range: t_hours_before(2),
			direction: None,
			step: None,
		})
		.await?;
	let should_cache = !labels.is_empty();
	let resp = QueryLabelsResponse {
		status: ResponseStatus::Success,
		data: labels,
	};
	if should_cache {
		let d = serialize_to_vec(&resp)?;
		cache.insert(LABELS_CACHE_KEY.to_string(), Arc::new(d));
	}
	Ok(resp)
}

fn t_hours_before(hours: u64) -> TimeRange {
	let start = Utc::now() - Duration::from_secs(hours * 60 * 60);
	TimeRange {
		start: Some(start.naive_utc()),
		end: None,
	}
}

fn label_values_cache_key(k: &str) -> String {
	LABEL_VALUES_CACHE_KEY_PREFIX.to_string() + k
}

fn series_cache_key_with_matches(matches: &str) -> String {
	SERIES_CACHE_KEY.to_string() + KEY_SPLITER + matches
}

pub async fn query_label_values(
	State(state): State<AppState>,
	Path(label): Path<String>,
	_: Query<QueryLabelValuesRequest>,
) -> Result<QueryLabelsResponse, AppError> {
	let cache = state.cache;
	let cache_key = label_values_cache_key(&label);
	if let Some(c) = cache.get(&cache_key) {
		debug!("hit cache for label values: {}", cache_key);
		return deserialize_from_slice(&c);
	}
	debug!("miss cache for label values: {}", cache_key);
	let values = state
		.log_handle
		.label_values(
			&label,
			QueryLimits {
				limit: None,
				range: t_hours_before(2),
				direction: None,
				step: None,
			},
		)
		.await?;
	let should_cache = !values.is_empty();
	let resp = QueryLabelsResponse {
		status: ResponseStatus::Success,
		data: values,
	};
	if should_cache {
		let d = serialize_to_vec(&resp)?;
		cache.insert(cache_key, Arc::new(d));
	}
	Ok(resp)
}

pub async fn query_series(
	State(state): State<AppState>,
	req: Result<Query<QuerySeriesRequest>, QueryRejection>,
) -> Result<Json<QuerySeriesResponse>, AppError> {
	let req = req
		.map_err(|e| AppError::InvalidQueryString(e.to_string()))?
		.0;
	let matches = if let parser::Query::LogQuery(lq) =
		parser::parse_logql_query(req.matches.as_str())?
	{
		lq
	} else {
		return Err(AppError::InvalidQueryString(req.matches));
	};
	// if no label pairs, client should not call this api
	// instead, it should call query_labels
	if matches.selector.label_paris.is_empty() {
		return Err(AppError::InvalidQueryString(
			req.matches.as_str().to_string(),
		));
	}
	let canonicalized_matches =
		canonicalize_matches(&matches.selector.label_paris);
	let cache_key_with_matches =
		series_cache_key_with_matches(&canonicalized_matches);
	if let Some(v) = state.cache.get(&cache_key_with_matches) {
		debug!("hit cache for series: {}", cache_key_with_matches);
		return Ok(Json(QuerySeriesResponse {
			status: ResponseStatus::Success,
			data: deserialize_from_slice(&v)?,
		}));
	}
	debug!("miss cache for series: {}", cache_key_with_matches);
	// try best to find cache whose key is the longest prefix of cache_key_with_matches
	// by doing this, can we minimize the number of label pairs that we need to filter
	// todo: this is inefficient, we should use a better way to find the longest prefix like trie
	let mut longest_prefix = None;
	for (k, _) in state.cache.iter() {
		if cache_key_with_matches.starts_with(k.as_ref()) {
			match longest_prefix {
				None => {
					longest_prefix = Some(k);
				}
				Some(p) if k.len() > p.len() => {
					longest_prefix = Some(k);
				}
				_ => {}
			}
		}
	}

	let cache_key = if let Some(v) = longest_prefix {
		debug!("use longest prefix cache: {}", v);
		(*v).clone()
	} else {
		SERIES_CACHE_KEY.to_string()
	};
	let mut values = if let Some(v) = state.cache.get(&cache_key) {
		deserialize_from_slice(&v)?
	} else {
		debug!(
			"no cache hit, very slow path, O(n!), cache_key: {}",
			cache_key
		);
		// no cache hit, very slow path, O(n!)
		let v = state
			.log_handle
			.series(
				None,
				QueryLimits {
					limit: None,
					range: t_hours_before(2),
					direction: None,
					step: None,
				},
			)
			.await?;
		// cache result to avoid O(n!)
		if !v.is_empty() {
			let d = serialize_to_vec(&v)?;
			state
				.cache
				.insert(SERIES_CACHE_KEY.to_string(), Arc::new(d));
			let v2 = convert_vec_hashmap(&v);
			cache_values(&state.cache, &v2);
		}
		v
	};
	// get the rest label pairs that we need to filter by
	let rest_label_pairs =
		get_rest_label_pairs(&cache_key, &cache_key_with_matches);
	// filter by matches
	if !rest_label_pairs.is_empty() {
		let before = values.len();
		values.retain(|m| filter_by_matches(m, &rest_label_pairs));
		debug!(
			"filter by matches, before: {}, after: {}",
			before,
			values.len()
		);
	}

	if !values.is_empty() && !rest_label_pairs.is_empty() {
		let d = serialize_to_vec(&values)?;
		state.cache.insert(cache_key_with_matches, Arc::new(d));
	}
	Ok(Json(QuerySeriesResponse {
		status: ResponseStatus::Success,
		data: values,
	}))
}

pub async fn background_refresh_series_cache(
	state: AppState,
	interval: Duration,
) {
	// run every interval
	let mut ticker = interval_at(Instant::now(), interval);
	loop {
		ticker.tick().await;
		debug!("refresh series cache");
		let v = state
			.log_handle
			.series(
				None,
				QueryLimits {
					limit: None,
					range: t_hours_before(2),
					direction: None,
					step: None,
				},
			)
			.await;
		match v {
			Ok(v) => {
				debug!("refresh series cache success, len: {}", v.len());
				// convert vec<hashmap<string, string>> to json will always success
				// so we just unwrap here
				if let Ok(d) = serialize_to_vec(&v) {
					state
						.cache
						.insert(SERIES_CACHE_KEY.to_string(), Arc::new(d));
					let v2 = convert_vec_hashmap(&v);
					cache_values(&state.cache, &v2);
				}
			}
			Err(e) => error!("failed to refresh series cache: {}", e),
		}
	}
}

// using_key is the cache key that we are using, cache_key_with_matches is the full key
// eg:
//   using_key: cc:series
//   cache_key_with_matches: cc:series:k1/0/v1-k2/1/v2
//   return: [k1/0/v1, k2/1/v2]
//
//   using_key: cc:series:k1/0/v1
//   cache_key_with_matches: cc:series:k1/0/v1-k2/1/v2
//   return: [k2/1/v2]
fn get_rest_label_pairs(
	using_key: &str,
	cache_key_with_matches: &str,
) -> Vec<parser::LabelPair> {
	let start = using_key.len() + KEY_SPLITER.len();
	if start >= cache_key_with_matches.len() {
		return vec![];
	}
	let suffix = &cache_key_with_matches[start..];
	let pairs = suffix
		.split(KEY_SPLITER)
		.filter_map(|s| {
			let parts = s.split(PAIR_SPLITER).collect::<Vec<_>>();
			if parts.len() == 3 {
				Some(parser::LabelPair {
					label: parts[0].to_string(),
					op: str_to_operator(parts[1].chars().next().unwrap_or('0')),
					value: parts[2].to_string(),
				})
			} else {
				debug!("invalid label pair, using_key: {}, cache_key_with_matches: {}, suffix: {}", using_key, cache_key_with_matches, suffix);
				None
			}
		})
		.collect();
	pairs
}

fn str_to_operator(c: char) -> parser::Operator {
	match c {
		'0' => parser::Operator::Equal,
		'1' => parser::Operator::NotEqual,
		'2' => parser::Operator::RegexMatch,
		'3' => parser::Operator::RegexNotMatch,
		_ => panic!("invalid operator: {}", c),
	}
}

fn operator_to_str(op: &parser::Operator) -> char {
	match op {
		parser::Operator::Equal => '0',
		parser::Operator::NotEqual => '1',
		parser::Operator::RegexMatch => '2',
		parser::Operator::RegexNotMatch => '3',
	}
}

// canonicalize the matches to a string
// make {k1="v1", k2!="v2"} to k1/0/v1-k2/1/v2
fn canonicalize_matches(matches: &[parser::LabelPair]) -> String {
	let mut arr = matches.to_vec();
	// sort by label but servicename is always first
	arr.sort_by(|a, b| {
		if a.label.eq_ignore_ascii_case("servicename") {
			Ordering::Less
		} else if b.label.eq_ignore_ascii_case("servicename") {
			Ordering::Greater
		} else {
			a.label.cmp(&b.label)
		}
	});
	let mut s = arr.into_iter().fold(String::new(), |mut acc, v| {
		acc.push_str(&v.label);
		acc.push_str(PAIR_SPLITER);
		acc.push(operator_to_str(&v.op));
		acc.push_str(PAIR_SPLITER);
		acc.push_str(&v.value);
		acc.push_str(KEY_SPLITER);
		acc
	});
	// remove the last '|||'
	s.truncate(s.len() - KEY_SPLITER.len());
	s
}

fn filter_by_matches(
	values: &HashMap<String, String>,
	matches: &Vec<parser::LabelPair>,
) -> bool {
	for parser::LabelPair { label, op, value } in matches {
		if let Some(actual) = values.get(label) {
			match *op {
				parser::Operator::Equal => {
					if !actual.eq(value) {
						return false;
					}
				}
				parser::Operator::NotEqual => {
					if actual.eq(value) {
						return false;
					}
				}
				parser::Operator::RegexMatch => {
					// check if actual matches regex value
					if !regex_match(actual, value) {
						return false;
					}
				}
				parser::Operator::RegexNotMatch => {
					if regex_match(actual, value) {
						return false;
					}
				}
			}
		} else if matches!(
			op,
			parser::Operator::Equal | parser::Operator::RegexMatch
		) {
			return false;
		}
	}
	true
}

fn regex_match(actual: &str, value: &str) -> bool {
	if let Ok(r) = regex::Regex::new(value) {
		r.is_match(actual)
	} else {
		false
	}
}

fn cache_values(
	cache: &Cache<String, Arc<Vec<u8>>>,
	values: &HashMap<&String, Vec<&String>>,
) {
	for (k, v) in values {
		let key = label_values_cache_key(k);
		let resp = CacheLabelResponse {
			status: ResponseStatus::Success,
			data: v,
		};
		if let Ok(d) = serialize_to_vec(&resp) {
			cache.insert(key, Arc::new(d));
		}
	}
}

fn convert_vec_hashmap(
	input: &Vec<HashMap<String, String>>,
) -> HashMap<&String, Vec<&String>> {
	let mut result: HashMap<&String, Vec<&String>> = HashMap::new();

	for map in input {
		for (key, value) in map {
			result.entry(key).or_default().push(value);
		}
	}

	result
}

fn serialize_to_vec<T: ?Sized + Serialize>(v: &T) -> Result<Vec<u8>, AppError> {
	use std::io::Write;
	rmp_serde::to_vec(v).map_err(AppError::from).and_then(|v| {
		let mut buf = Vec::new();
		let mut enc = GzEncoder::new(&mut buf, Compression::default());
		enc.write_all(&v)?;
		enc.finish()?;
		Ok(buf)
	})
}

fn deserialize_from_slice<T: DeserializeOwned>(
	v: &[u8],
) -> Result<T, AppError> {
	use std::io::Read;
	let mut dec = GzDecoder::new(v);
	let mut buf = Vec::new();
	dec.read_to_end(&mut buf)?;
	rmp_serde::from_read(buf.as_slice()).map_err(AppError::from)
}

#[derive(Serialize, Debug)]
struct CacheLabelResponse<'a> {
	pub status: ResponseStatus,
	pub data: &'a Vec<&'a String>,
}

// extend the cache expiry time when the key is updated
pub struct LabelCacheExpiry {
	pub extend_when_update: Duration,
}

impl Expiry<String, Arc<Vec<u8>>> for LabelCacheExpiry {
	fn expire_after_update(
		&self,
		key: &String,
		_value: &Arc<Vec<u8>>,
		_updated_at: std::time::Instant,
		duration_until_expiry: Option<Duration>,
	) -> Option<Duration> {
		if !key.eq(SERIES_CACHE_KEY)
			&& !key.starts_with(LABEL_VALUES_CACHE_KEY_PREFIX)
		{
			return duration_until_expiry;
		}
		if let Some(d) = duration_until_expiry {
			Some(std::cmp::max(d, self.extend_when_update))
		} else {
			Some(self.extend_when_update)
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use logql::parser::{self, LabelPair};

	#[test]
	fn test_get_rest_label_pairs() {
		let test_cases = vec![
			("cc:series:", "cc:series:", vec![]),
			(
				"cc:series:",
				"cc:series:---k1|||0|||v1",
				vec![LabelPair {
					label: "k1".to_string(),
					op: parser::Operator::Equal,
					value: "v1".to_string(),
				}],
			),
			(
				"cc:series:",
				"cc:series:---k1|||0|||v1---k2|||1|||v2",
				vec![
					LabelPair {
						label: "k1".to_string(),
						op: parser::Operator::Equal,
						value: "v1".to_string(),
					},
					LabelPair {
						label: "k2".to_string(),
						op: parser::Operator::NotEqual,
						value: "v2".to_string(),
					},
				],
			),
			(
				"cc:series:---k1|||0|||v1",
				"cc:series:---k1|||0|||v1---k2|||1|||v2",
				vec![LabelPair {
					label: "k2".to_string(),
					op: parser::Operator::NotEqual,
					value: "v2".to_string(),
				}],
			),
			(
				"cc:series:---k1|||0|||v1---k2|||1|||v2",
				"cc:series:---k1|||0|||v1---k2|||1|||v2---k3|||2|||v3",
				vec![LabelPair {
					label: "k3".to_string(),
					op: parser::Operator::RegexMatch,
					value: "v3".to_string(),
				}],
			),
		];
		for (using_key, cache_key, expected) in test_cases {
			let actual = get_rest_label_pairs(using_key, cache_key);
			assert_eq!(actual, expected);
		}
	}

	#[test]
	fn test_canonicalize_matches() {
		let test_cases = vec![
			(
				vec![LabelPair {
					label: "k1".to_string(),
					op: parser::Operator::Equal,
					value: "v1".to_string(),
				}],
				"k1|||0|||v1",
			),
			(
				vec![
					LabelPair {
						label: "k1".to_string(),
						op: parser::Operator::Equal,
						value: "v1".to_string(),
					},
					LabelPair {
						label: "k2".to_string(),
						op: parser::Operator::NotEqual,
						value: "v2".to_string(),
					},
				],
				"k1|||0|||v1---k2|||1|||v2",
			),
			(
				vec![
					LabelPair {
						label: "k1".to_string(),
						op: parser::Operator::Equal,
						value: "v1".to_string(),
					},
					LabelPair {
						label: "k2".to_string(),
						op: parser::Operator::NotEqual,
						value: "v2".to_string(),
					},
					LabelPair {
						label: "ServiceName".to_string(),
						op: parser::Operator::RegexNotMatch,
						value: "ss".to_string(),
					},
				],
				"ServiceName|||3|||ss---k1|||0|||v1---k2|||1|||v2",
			),
			(
				vec![
					LabelPair {
						label: "k1".to_string(),
						op: parser::Operator::Equal,
						value: "v1".to_string(),
					},
					LabelPair {
						label: "k2".to_string(),
						op: parser::Operator::NotEqual,
						value: "v2".to_string(),
					},
					LabelPair {
						label: "k3".to_string(),
						op: parser::Operator::RegexMatch,
						value: "v3".to_string(),
					},
				],
				"k1|||0|||v1---k2|||1|||v2---k3|||2|||v3",
			),
		];
		for (matches, expected) in test_cases {
			let actual = canonicalize_matches(&matches);
			assert_eq!(actual, expected);
		}
	}

	#[test]
	fn test_serialize_and_deserialize() {
		let m: HashMap<String, String> = vec![
			("aaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbb"),
			("ccccccccccccccccccc", "ddddddddddddddddddd"),
			("eeeeeeeeeeeeeeeeeee", "fffffffffffffffffff"),
			("ggggggggggggggggggg", "hhhhhhhhhhhhhhhhhhh"),
			("iiiiiiiiiiiiiiiiiii", "jjjjjjjjjjjjjjjjjjj"),
			("kkkkkkkkkkkkkkkkkkk", "lllllllllllllllllll"),
			("mmmmmmmmmmmmmmmmmmm", "nnnnnnnnnnnnnnnnnnn"),
			("ooooooooooooooooooo", "ppppppppppppppppppp"),
			("qqqqqqqqqqqqqqqqqqq", "rrrrrrrrrrrrrrrrrrr"),
			("sssssssssssssssssss", "ttttttttttttttttttt"),
			("uuuuuuuuuuuuuuuuuuu", "vvvvvvvvvvvvvvvvvvv"),
			("wwwwwwwwwwwwwwwwwww", "xxxxxxxxxxxxxxxxxxx"),
			("yyyyyyyyyyyyyyyyyyy", "zzzzzzzzzzzzzzzzzzz"),
		]
		.into_iter()
		.map(|(k, v)| (k.to_string(), v.to_string()))
		.collect();
		let d = serialize_to_vec(&m).unwrap();
		let m2 = deserialize_from_slice::<HashMap<String, String>>(&d).unwrap();
		assert_eq!(m, m2);
	}
}
