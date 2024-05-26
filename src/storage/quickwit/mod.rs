use super::{log::LogStorage, trace::TraceStorage};
use crate::config::Quickwit;
use anyhow::Result;
use std::{path::Path, time::Duration};
use url::Url;

pub mod esdsl;
pub mod log;
pub mod qwdsl;
pub mod sdk;
pub mod trace;

#[derive(Clone, Debug)]
pub struct QuickwitServerConfig {
	pub endpoint: url::Url,
	pub timeout: Duration,
}

pub async fn new_log_source(cfg: Quickwit) -> Result<Box<dyn LogStorage>> {
	let pp = Path::new("/api/v1").join(&cfg.index).join("search");
	let endpoint = Url::parse(&cfg.domain)?.join(pp.to_str().unwrap())?;
	let inner = log::QuickwitLog::new(QuickwitServerConfig {
		endpoint,
		timeout: cfg.timeout,
	});
	Ok(Box::new(inner))
}

pub async fn new_trace_source(cfg: Quickwit) -> Result<Box<dyn TraceStorage>> {
	let pp = Path::new("/api/v1").join(&cfg.index).join("search");
	let endpoint = Url::parse(&cfg.domain)?.join(pp.to_str().unwrap())?;
	let inner = trace::QuickwitTrace::new(QuickwitServerConfig {
		endpoint,
		timeout: cfg.timeout,
	});
	Ok(Box::new(inner))
}
