use criterion::{
	black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};
use std::collections::HashMap;
use streamstore::{SeriesStore, StreamStore};

fn generate_test_records(count: usize) -> Vec<HashMap<String, String>> {
	let mut records = Vec::with_capacity(count);
	for i in 0..count {
		let mut record = HashMap::new();
		record.insert(
			"env".to_string(),
			["prod", "staging", "dev"][i % 3].to_string(),
		);
		record.insert("service".to_string(), format!("service-{}", i));
		record.insert(
			"region".to_string(),
			["us-east", "us-west", "eu-west"][i % 3].to_string(),
		);
		record.insert("version".to_string(), format!("{}.0", i % 5));
		records.push(record);
	}
	records
}

fn create_labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
	pairs
		.iter()
		.map(|(k, v)| (k.to_string(), v.to_string()))
		.collect()
}

fn bench_add(c: &mut Criterion) {
	let mut group = c.benchmark_group("add_operations");
	for size in [100, 1000, 10000].iter() {
		group.bench_with_input(
			BenchmarkId::new("add_records", size),
			size,
			|b, &size| {
				let store = StreamStore::new();
				let records = generate_test_records(size);
				b.iter(|| {
					store.add(black_box(records.clone()));
				});
			},
		);
	}
	group.finish();
}

fn bench_query(c: &mut Criterion) {
	let mut group = c.benchmark_group("query_operations");

	// Setup store with data
	let store = StreamStore::new();
	let records = generate_test_records(10000);
	store.add(records);

	// Single condition query
	group.bench_function("query_single_condition", |b| {
		b.iter(|| {
			store.query(black_box(create_labels(&[("env", "prod")])));
		});
	});

	// Multiple conditions query
	group.bench_function("query_multiple_conditions", |b| {
		b.iter(|| {
			store.query(black_box(create_labels(&[
				("env", "prod"),
				("region", "us-east"),
			])));
		});
	});

	// No matches query
	group.bench_function("query_no_matches", |b| {
		b.iter(|| {
			store.query(black_box(create_labels(&[("env", "non-existent")])));
		});
	});

	// Empty query (return all)
	group.bench_function("query_empty_conditions", |b| {
		b.iter(|| {
			store.query(black_box(HashMap::new()));
		});
	});

	group.finish();
}

fn bench_concurrent(c: &mut Criterion) {
	use std::sync::Arc;
	use std::thread;

	let mut group = c.benchmark_group("concurrent_operations");

	for thread_count in [4, 8, 16].iter() {
		group.bench_with_input(
			BenchmarkId::new("concurrent_add", thread_count),
			thread_count,
			|b, &thread_count| {
				b.iter(|| {
					let store: Arc<StreamStore> = Arc::new(StreamStore::new());
					let mut handles = vec![];
					let records_per_thread = 1000;

					for t in 0..thread_count {
						let store_clone = Arc::clone(&store);
						let handle = thread::spawn(move || {
							let records =
								generate_test_records(records_per_thread)
									.into_iter()
									.map(|mut record| {
										record.insert(
											"thread".to_string(),
											format!("thread-{}", t),
										);
										record
									})
									.collect::<Vec<_>>();
							store_clone.add(records);
						});
						handles.push(handle);
					}

					for handle in handles {
						handle.join().unwrap();
					}

					// Query all records after concurrent adds
					black_box(store.query(HashMap::new()));
				});
			},
		);
	}

	group.finish();
}

fn bench_batch_size_impact(c: &mut Criterion) {
	let mut group = c.benchmark_group("batch_size_impact");

	// Test different batch sizes for the same total number of records
	let total_records = 10000;
	for batch_size in [1, 10, 100, 1000] {
		let records_per_batch = total_records / batch_size;
		group.bench_with_input(
			BenchmarkId::new("batch_add", batch_size),
			&batch_size,
			|b, _| {
				let store = StreamStore::new();
				let all_records = generate_test_records(total_records);

				b.iter(|| {
					for chunk in all_records.chunks(records_per_batch) {
						store.add(chunk.to_vec());
					}
				});
			},
		);
	}
	group.finish();
}

criterion_group!(
	benches,
	bench_add,
	bench_query,
	bench_concurrent,
	bench_batch_size_impact
);
criterion_main!(benches);
