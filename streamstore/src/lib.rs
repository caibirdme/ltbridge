use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::RwLock;

pub trait SeriesStore {
	fn add(&self, records: Vec<HashMap<String, String>>);
	fn query(
		&self,
		conditions: HashMap<String, String>,
	) -> Vec<HashMap<String, String>>;
	fn labels(&self) -> Option<Vec<String>>;
	fn label_values(&self, label: &str) -> Option<Vec<String>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Labels(HashMap<String, String>);

impl Labels {
	fn new(labels: HashMap<String, String>) -> Self {
		Labels(labels)
	}

	fn hash(&self) -> u64 {
		let mut sorted_labels: Vec<_> = self.0.iter().collect();
		sorted_labels.sort_by(|a, b| a.0.cmp(b.0));

		let mut hasher = DefaultHasher::new();
		for (k, v) in sorted_labels {
			k.hash(&mut hasher);
			v.hash(&mut hasher);
		}
		hasher.finish()
	}

	fn matches(&self, conditions: &HashMap<String, String>) -> bool {
		conditions
			.iter()
			.all(|(k, v)| self.0.get(k).map_or(false, |value| value == v))
	}
}

/// Stream storage implementation
pub struct StreamStore {
	// Use HashSet to store unique label combinations
	streams: RwLock<HashSet<u64>>,
	// Store the actual content of label combinations
	labels_store: RwLock<HashMap<u64, Labels>>,
	// Inverted index: label name -> label value -> stream hash values
	label_index: RwLock<HashMap<String, HashMap<String, HashSet<u64>>>>,
}

impl StreamStore {
	pub fn new() -> Self {
		Self {
			streams: RwLock::new(HashSet::new()),
			labels_store: RwLock::new(HashMap::new()),
			label_index: RwLock::new(HashMap::new()),
		}
	}
}

impl SeriesStore for StreamStore {
	fn add(&self, records: Vec<HashMap<String, String>>) {
		let mut streams = self.streams.write().unwrap();
		let mut labels_store = self.labels_store.write().unwrap();
		let mut label_index = self.label_index.write().unwrap();

		for record in records {
			let labels = Labels::new(record);
			let hash = labels.hash();

			// Skip if label combination already exists
			if streams.contains(&hash) {
				continue;
			}

			// Update inverted index
			for (key, value) in labels.0.iter() {
				label_index
					.entry(key.clone())
					.or_insert_with(HashMap::new)
					.entry(value.clone())
					.or_insert_with(HashSet::new)
					.insert(hash);
			}

			// Store label combination
			streams.insert(hash);
			labels_store.insert(hash, labels);
		}
	}

	fn query(
		&self,
		conditions: HashMap<String, String>,
	) -> Vec<HashMap<String, String>> {
		if conditions.is_empty() {
			return self
				.labels_store
				.read()
				.unwrap()
				.values()
				.map(|labels| labels.0.clone())
				.collect();
		}

		let label_index = self.label_index.read().unwrap();
		let labels_store = self.labels_store.read().unwrap();

		// Find stream hash set that satisfies the first condition
		let mut result_hashes: Option<HashSet<u64>> = None;

		// Use inverted index to find candidate set
		for (key, value) in conditions.iter() {
			let current_hashes = label_index
				.get(key)
				.and_then(|value_index| value_index.get(value))
				.cloned()
				.unwrap_or_default();

			result_hashes = match result_hashes {
				None => Some(current_hashes),
				Some(hashes) => Some(&hashes & &current_hashes),
			};

			// Early pruning
			if let Some(hashes) = &result_hashes {
				if hashes.is_empty() {
					return Vec::new();
				}
			}
		}

		// Verify all conditions and collect results
		result_hashes
			.map(|hashes| {
				hashes
					.into_iter()
					.filter_map(|hash| labels_store.get(&hash))
					.filter(|labels| labels.matches(&conditions))
					.map(|labels| labels.0.clone())
					.collect()
			})
			.unwrap_or_default()
	}

	fn labels(&self) -> Option<Vec<String>> {
		let v = self
			.label_index
			.read()
			.unwrap()
			.keys()
			.cloned()
			.collect::<Vec<_>>();
		if v.is_empty() {
			None
		} else {
			Some(v)
		}
	}

	fn label_values(&self, label: &str) -> Option<Vec<String>> {
		self.label_index
			.read()
			.unwrap()
			.get(label)
			.map(|v| v.keys().cloned().collect::<Vec<_>>())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::HashMap;

	// Helper function: create label mapping
	fn create_labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
		pairs
			.iter()
			.map(|(k, v)| (k.to_string(), v.to_string()))
			.collect()
	}

	#[test]
	fn test_add_single_record() {
		let store = StreamStore::new();
		let record = create_labels(&[
			("env", "prod"),
			("service", "api"),
			("region", "us-east"),
		]);

		store.add(vec![record.clone()]);

		// Query verification
		let result = store.query(create_labels(&[("env", "prod")]));
		assert_eq!(result.len(), 1);
		assert_eq!(result[0], record);
	}

	#[test]
	fn test_add_duplicate_records() {
		let store = StreamStore::new();
		let record = create_labels(&[("env", "prod"), ("service", "api")]);

		// Add the same record twice
		store.add(vec![record.clone()]);
		store.add(vec![record.clone()]);

		// Verify that only one record is stored
		let result = store.query(create_labels(&[("env", "prod")]));
		assert_eq!(result.len(), 1);
		assert_eq!(result[0], record);
	}

	#[test]
	fn test_add_multiple_records() {
		let store = StreamStore::new();

		let records = vec![
			create_labels(&[("env", "prod"), ("service", "api")]),
			create_labels(&[("env", "prod"), ("service", "web")]),
			create_labels(&[("env", "dev"), ("service", "api")]),
		];

		store.add(records);

		// Verify total count
		let all_prod = store.query(create_labels(&[("env", "prod")]));
		assert_eq!(all_prod.len(), 2);

		let all_api = store.query(create_labels(&[("service", "api")]));
		assert_eq!(all_api.len(), 2);

		let all_web = store.query(create_labels(&[("service", "web")]));
		assert_eq!(all_web.len(), 1);
	}

	#[test]
	fn test_query_empty_conditions() {
		let store = StreamStore::new();
		let records = vec![
			create_labels(&[("env", "prod"), ("service", "api")]),
			create_labels(&[("env", "dev"), ("service", "web")]),
		];

		store.add(records);

		// Empty conditions should return all records
		let result = store.query(HashMap::new());
		assert_eq!(result.len(), 2);
	}

	#[test]
	fn test_query_single_condition() {
		let store = StreamStore::new();
		let records = vec![
			create_labels(&[("env", "prod"), ("service", "api")]),
			create_labels(&[("env", "prod"), ("service", "web")]),
			create_labels(&[("env", "dev"), ("service", "api")]),
		];

		store.add(records);

		// Test single condition query
		let prod_records = store.query(create_labels(&[("env", "prod")]));
		assert_eq!(prod_records.len(), 2);

		let api_records = store.query(create_labels(&[("service", "api")]));
		assert_eq!(api_records.len(), 2);
	}

	#[test]
	fn test_query_multiple_conditions() {
		let store = StreamStore::new();
		let records = vec![
			create_labels(&[
				("env", "prod"),
				("service", "api"),
				("region", "us-east"),
			]),
			create_labels(&[
				("env", "prod"),
				("service", "web"),
				("region", "us-east"),
			]),
			create_labels(&[
				("env", "dev"),
				("service", "api"),
				("region", "eu-west"),
			]),
		];

		store.add(records);

		// Test multiple conditions query
		let result =
			store.query(create_labels(&[("env", "prod"), ("service", "api")]));
		assert_eq!(result.len(), 1);
		assert!(result[0].get("region").unwrap() == "us-east");
	}

	#[test]
	fn test_query_no_matches() {
		let store = StreamStore::new();
		let records = vec![
			create_labels(&[("env", "prod"), ("service", "api")]),
			create_labels(&[("env", "dev"), ("service", "web")]),
		];

		store.add(records);

		// Test non-existent conditions
		let result = store.query(create_labels(&[("env", "staging")]));
		assert!(result.is_empty());

		// Test impossible combinations
		let result = store.query(create_labels(&[
			("env", "prod"),
			("service", "web"),
			("region", "nowhere"),
		]));
		assert!(result.is_empty());
	}

	#[test]
	fn test_query_partial_matches() {
		let store = StreamStore::new();
		let records = vec![
			create_labels(&[
				("env", "prod"),
				("service", "api"),
				("version", "1.0"),
				("region", "us-east"),
			]),
			create_labels(&[
				("env", "prod"),
				("service", "api"),
				("version", "2.0"),
				("region", "us-west"),
			]),
		];

		store.add(records);

		// Test partial matching
		let result =
			store.query(create_labels(&[("env", "prod"), ("service", "api")]));
		assert_eq!(result.len(), 2);

		// Test exact matching
		let result = store.query(create_labels(&[
			("env", "prod"),
			("service", "api"),
			("version", "1.0"),
		]));
		assert_eq!(result.len(), 1);
	}

	#[test]
	fn test_case_sensitivity() {
		let store = StreamStore::new();
		let record = create_labels(&[("ENV", "prod"), ("Service", "API")]);

		store.add(vec![record]);

		// Test case sensitivity
		let result = store.query(create_labels(&[("env", "prod")]));
		assert!(result.is_empty());

		let result = store.query(create_labels(&[("ENV", "prod")]));
		assert_eq!(result.len(), 1);
	}

	#[test]
	fn test_concurrent_access() {
		use std::sync::Arc;
		use std::thread;

		let store = Arc::new(StreamStore::new());
		let mut handles = vec![];

		// Concurrent addition of records
		for i in 0..10 {
			let store_clone = Arc::clone(&store);
			let handle = thread::spawn(move || {
				let record = create_labels(&[
					("env", "prod"),
					("service", &format!("service-{}", i)),
				]);
				store_clone.add(vec![record]);
			});
			handles.push(handle);
		}

		// Wait for all threads to complete
		for handle in handles {
			handle.join().unwrap();
		}

		// Verify results
		let result = store.query(create_labels(&[("env", "prod")]));
		assert_eq!(result.len(), 10);
	}

	#[test]
	fn test_large_dataset() {
		let store = StreamStore::new();
		let envs = ["prod", "staging", "dev", "test"];
		let services = ["api", "web", "worker", "scheduler", "cache"];
		let regions = ["us-east", "us-west", "eu-west", "eu-east", "ap-south"];
		let versions = ["1.0", "2.0", "3.0"];
		let clusters = ["c1", "c2", "c3", "c4"];

		// Generate 40 records with 5 columns each
		let mut records = Vec::with_capacity(40);
		for i in 0..40 {
			let record = create_labels(&[
				("env", envs[i % envs.len()]),
				("service", services[i % services.len()]),
				("region", regions[i % regions.len()]),
				("version", versions[i % versions.len()]),
				("cluster", clusters[i % clusters.len()]),
			]);
			records.push(record);
		}

		// Add all records
		store.add(records);

		// Test various query combinations
		let prod_api =
			store.query(create_labels(&[("env", "prod"), ("service", "api")]));
		assert_eq!(prod_api.len(), 2); // 40/4/5 = 2 records match prod+api

		let staging_useast = store
			.query(create_labels(&[("env", "staging"), ("region", "us-east")]));
		assert_eq!(staging_useast.len(), 2); // 40/4/5 = 2 records match staging+us-east

		let v1_web = store
			.query(create_labels(&[("version", "1.0"), ("service", "web")]));
		assert_eq!(v1_web.len(), 3); // 40/3/5 ≈ 3 records match v1.0+web

		// Test query with all columns
		let specific_record = store.query(create_labels(&[
			("env", "prod"),
			("service", "api"),
			("region", "us-east"),
			("version", "1.0"),
			("cluster", "c1"),
		]));
		assert!(specific_record.len() <= 1); // At most one record can match all conditions

		// Test distribution of values
		let prod_records = store.query(create_labels(&[("env", "prod")]));
		assert_eq!(prod_records.len(), 10); // 40/4 = 10 records should be prod

		let api_records = store.query(create_labels(&[("service", "api")]));
		assert_eq!(api_records.len(), 8); // 40/5 = 8 records should be api

		let useast_records =
			store.query(create_labels(&[("region", "us-east")]));
		assert_eq!(useast_records.len(), 8); // 40/5 = 8 records should be us-east

		// Test all records
		let all_records = store.query(HashMap::new());
		assert_eq!(all_records.len(), 40);
	}

	#[test]
	fn test_labels_and_values() {
		let store = StreamStore::new();
		let records = vec![
			create_labels(&[
				("env", "prod"),
				("service", "api"),
				("region", "us-east"),
			]),
			create_labels(&[
				("env", "dev"),
				("service", "web"),
				("region", "us-west"),
			]),
			create_labels(&[
				("env", "prod"),
				("service", "worker"),
				("region", "eu-west"),
			]),
		];

		store.add(records);

		// Test labels() method
		let labels = store.labels().unwrap();
		assert_eq!(labels.len(), 3);
		assert!(labels.contains(&"env".to_string()));
		assert!(labels.contains(&"service".to_string()));
		assert!(labels.contains(&"region".to_string()));

		// Test label_values() method
		let env_values = store.label_values("env").unwrap();
		assert_eq!(env_values.len(), 2);
		assert!(env_values.contains(&"prod".to_string()));
		assert!(env_values.contains(&"dev".to_string()));

		let service_values = store.label_values("service").unwrap();
		assert_eq!(service_values.len(), 3);
		assert!(service_values.contains(&"api".to_string()));
		assert!(service_values.contains(&"web".to_string()));
		assert!(service_values.contains(&"worker".to_string()));

		let region_values = store.label_values("region").unwrap();
		assert_eq!(region_values.len(), 3);
		assert!(region_values.contains(&"us-east".to_string()));
		assert!(region_values.contains(&"us-west".to_string()));
		assert!(region_values.contains(&"eu-west".to_string()));

		// Test non-existent label
		let non_existent = store.label_values("non-existent");
		assert!(non_existent.is_none());
	}

	#[test]
	fn test_empty_store_labels() {
		let store = StreamStore::new();

		// Test labels() on empty store
		let labels = store.labels();
		assert!(labels.is_none());

		// Test label_values() on empty store
		let values = store.label_values("any");
		assert!(values.is_none());
	}
}
