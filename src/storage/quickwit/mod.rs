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
	pub qw_endpoint: url::Url,
	pub es_endpoint: url::Url,
	pub timeout: Duration,
}

impl QuickwitServerConfig {
	pub fn new(cfg: Quickwit) -> Result<Self> {
		let pp = Path::new("/api/v1/").join(&cfg.index);
		let qw_endpoint =
			Url::parse(&cfg.domain)?.join(pp.to_str().unwrap())?;
		let pp = Path::new("/api/v1/_elastic/").join(&cfg.index);
		let es_endpoint =
			Url::parse(&cfg.domain)?.join(pp.to_str().unwrap())?;
		Ok(QuickwitServerConfig {
			qw_endpoint,
			es_endpoint,
			timeout: cfg.timeout,
		})
	}
}

pub async fn new_log_source(cfg: Quickwit) -> Result<Box<dyn LogStorage>> {
	let inner = log::QuickwitLog::new(QuickwitServerConfig::new(cfg)?);
	Ok(Box::new(inner))
}

pub async fn new_trace_source(cfg: Quickwit) -> Result<Box<dyn TraceStorage>> {
	let inner = trace::QuickwitTrace::new(QuickwitServerConfig::new(cfg)?);
	Ok(Box::new(inner))
}
