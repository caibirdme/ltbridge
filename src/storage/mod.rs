use crate::config::{ClickhouseConf, DataSource};
use anyhow::Result;
use chrono::NaiveDateTime;
use std::time::Duration;

pub mod ck;
pub mod databend;
pub mod log;
pub mod quickwit;
pub mod trace;

const DEFAULT_STEP: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Default)]
pub struct QueryLimits {
	pub limit: Option<u32>,
	pub range: common::TimeRange,
	pub direction: Option<Direction>,
	pub step: Option<Duration>,
}

#[derive(Debug, Clone, Default)]
pub enum Direction {
	Forward,
	#[default]
	Backward,
}

pub async fn new_trace_source(
	d: DataSource,
) -> Result<Box<dyn trace::TraceStorage>> {
	match d {
		DataSource::Databend(cfg) => databend::new_trace_source(cfg).await,
		DataSource::Quickwit(cfg) => quickwit::new_trace_source(cfg).await,
		DataSource::Clickhouse(cfg) => match cfg {
			ClickhouseConf::Trace(cfg) => ck::new_trace_source(cfg).await,
			ClickhouseConf::Log(_) => {
				panic!("cannot use ck log config for trace source")
			}
		},
	}
}

pub async fn new_log_source(d: DataSource) -> Result<Box<dyn log::LogStorage>> {
	match d {
		DataSource::Databend(cfg) => databend::new_log_source(cfg).await,
		DataSource::Quickwit(cfg) => quickwit::new_log_source(cfg).await,
		DataSource::Clickhouse(cfg) => match cfg {
			ClickhouseConf::Log(cfg) => ck::new_log_source(cfg).await,
			ClickhouseConf::Trace(_) => {
				panic!("cannot use ck trace config for log source")
			}
		},
	}
}
