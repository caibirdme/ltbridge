use crate::{
	config::AppConfig,
	logquery, metrics, routes, state,
	storage::{new_log_source, new_trace_source},
};
use anyhow::Result;
use std::{fs::OpenOptions, sync::Arc};
use tracing::{debug, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use validator::Validate;

pub async fn start() -> Result<()> {
	// load configuration
	let cfg = AppConfig::new().unwrap();
	cfg.validate().unwrap();

	init_tracing_subscriber(
		cfg.server.log.file.clone(),
		cfg.server.log.filter_directives.as_str(),
	);

	// init metrics
	let metrics_handle = metrics::setup_metrcis();
	// init cache
	let cache = state::new_cache(&cfg.cache);

	let trace_handle = new_trace_source(cfg.trace_source.clone()).await?;
	let log_handle = new_log_source(cfg.log_source.clone()).await?;

	let app_state = state::AppState {
		config: Arc::new(cfg.clone()),
		trace_handle,
		log_handle,
		cache,
		metrics: Arc::new(metrics_handle),
	};
	// build our application with a route
	let app = routes::new_router(app_state.clone());

	// start a background task to refresh the series cache
	// so that user won't wait for too long when cache is expired
	if let Some(interval) = cfg.cache.refresh_interval {
		tokio::spawn(async move {
			debug!("start background task to refresh series cache");
			logquery::labels::background_refresh_series_cache(
				app_state, interval,
			)
			.await;
		});
	}
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
	if file.as_str().eq_ignore_ascii_case("stdout") {
		Box::new(std::io::stdout())
	} else if file.as_str().eq_ignore_ascii_case("stderr") {
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
