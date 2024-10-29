use crate::{
	config,
	logquery::labels::LabelCacheExpiry,
	metrics,
	storage::{log::LogStorage, trace::TraceStorage},
};
use moka::sync::Cache;
use std::sync::Arc;
use tracing::debug;

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
		.expire_after(LabelCacheExpiry{extend_when_update: cfg.time_to_live})
		.max_capacity(cfg.max_capacity)
		.weigher(|_,v| v.len().try_into().unwrap_or(u32::MAX))
		.eviction_listener(|k,v,action| {
			debug!("eviction listener: key: {}, value_len: {}, action: {:?}", k, v.len(), action);
		})
		.time_to_live(cfg.time_to_live)
		.time_to_idle(cfg.time_to_idle)
		.build()
}
