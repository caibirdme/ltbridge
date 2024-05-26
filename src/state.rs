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
	pub cache: Cache<String, Vec<u8>>,
	pub metrics: Arc<metrics::Instrumentations>,
}
