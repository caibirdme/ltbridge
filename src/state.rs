use crate::{
	config, metrics,
	storage::{log::LogStorage, trace::TraceStorage},
};
use moka::sync::Cache;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
	pub config: Arc<config::AppConfig>,
	pub log_handle: Box<dyn LogStorage>,
	pub trace_handle: Box<dyn TraceStorage>,
	pub cache: Cache<String, Arc<Vec<u8>>>,
	pub metrics: Arc<metrics::Instrumentations>,
}

pub fn new_cache(cfg: &config::Cache) -> Cache<String, Arc<Vec<u8>>> {
	Cache::builder()
		// automatically extend the cache expiry time when the key is updated
		.max_capacity(cfg.max_capacity)
		.time_to_live(cfg.time_to_live)
		.time_to_idle(cfg.time_to_idle)
		.build()
}
