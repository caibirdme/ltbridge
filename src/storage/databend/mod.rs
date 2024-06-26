use super::{log::LogStorage, trace::TraceStorage};
use crate::config::Databend;
use anyhow::Result;
use databend_driver::{Client, Connection};

pub(crate) mod converter;
pub mod log;
pub mod trace;

pub async fn new_log_source(cfg: Databend) -> Result<Box<dyn LogStorage>> {
	let use_inv_idx = cfg.inverted_index;
	let cli = Client::try_from(cfg)?;
	let conn = cli.get_conn().await?;
	init_log_source(conn.clone()).await?;
	let mut q = log::BendLogQuerier::new(conn);
	q.with_inverted_index(use_inv_idx);
	Ok(Box::new(q))
}

// when query volume of logs overtime, set numeric_cast_option = 'truncating'
// to ensure time column is truncated to integer(floor)
async fn init_log_source(conn: Box<dyn Connection>) -> Result<()> {
	conn.exec("SET numeric_cast_option = 'truncating';").await?;
	Ok(())
}

pub async fn new_trace_source(cfg: Databend) -> Result<Box<dyn TraceStorage>> {
	let cli = Client::try_from(cfg)?;
	let conn = cli.get_conn().await?;
	let q = trace::BendTraceQuerier::new(conn);
	Ok(Box::new(q))
}
