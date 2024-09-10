use anyhow::Result;
use ltbridge::storage::{new_log_source, new_trace_source};
use ltbridge::{config::AppConfig, metrics, routes, state};
use moka::sync::Cache;
use std::{fs::OpenOptions, sync::Arc};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
	// load configuration
	let cfg = AppConfig::new().unwrap();

	init_tracing_subscriber(
		cfg.server.log.file.clone(),
		cfg.server.log.filter_directives.as_str(),
	);

	// init metrics
	let metrics_handle = metrics::setup_metrcis();
	// init cache
	let cache = Cache::builder()
		.max_capacity(cfg.cache.max_capacity)
		.time_to_live(cfg.cache.time_to_live)
		.time_to_idle(cfg.cache.time_to_idle)
		.build();

	let trace_handle = new_trace_source(cfg.trace_source.clone()).await?;
	let log_handle = new_log_source(cfg.log_source.clone()).await?;

	// build our application with a route
	let app = routes::new_router(state::AppState {
		config: Arc::new(cfg.clone()),
		trace_handle,
		log_handle,
		cache,
		metrics: Arc::new(metrics_handle),
	});
	// run our app with hyper, listening globally on port 3000
	let listener =
		tokio::net::TcpListener::bind(cfg.server.listen_addr.clone())
			.await
			.unwrap();
	info!("Listening on: {}", cfg.server.listen_addr);
	axum::serve(listener, app).await.unwrap();
	Ok(())
}

fn init_tracing_subscriber(file: String, filter_directives: &str) {
	tracing_subscriber::registry()
		.with(tracing_subscriber::EnvFilter::new(filter_directives))
		.with(
			tracing_subscriber::fmt::layer()
				.json()
				.with_writer(move || get_writer(file.clone())),
		)
		.init();
}

fn get_writer(file: String) -> Box<dyn std::io::Write> {
	if file.as_str() == "stdout" {
		Box::new(std::io::stdout())
	} else if file.as_str() == "stderr" {
		Box::new(std::io::stderr())
	} else {
		Box::new(
			OpenOptions::new()
				.append(true)
				.create(true)
				.open(file)
				.unwrap(),
		)
	}
}
