use anyhow::Result;
use ltbridge::storage::{new_log_source, new_trace_source};
use ltbridge::{config::AppConfig, metrics, routes, state};
use moka::sync::Cache;
use std::{fs::OpenOptions, sync::Arc, time::Duration};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
	// load configuration
	let cfg = AppConfig::new().unwrap();

	let log_file = OpenOptions::new()
		.append(true)
		.create(true)
		.open(&cfg.server.log.file)
		.unwrap();
	// initialize
	// tracing
	tracing_subscriber::registry()
		.with(tracing_subscriber::EnvFilter::new(
			&cfg.server.log.filter_directives,
		))
		.with(
			tracing_subscriber::fmt::layer()
				.json()
				.with_writer(log_file),
		)
		.init();

	// init metrics
	let metrics_handle = metrics::setup_metrcis();
	// init cache
	let cache = Cache::builder()
		.max_capacity(1000)
		.time_to_live(Duration::from_secs(20 * 60))
		.time_to_idle(Duration::from_secs(3 * 60))
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
	let listener = tokio::net::TcpListener::bind(cfg.server.listen_addr)
		.await
		.unwrap();
	axum::serve(listener, app).await.unwrap();
	Ok(())
}
