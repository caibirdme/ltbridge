use super::{log::LogStorage, trace::TraceStorage};
use crate::config::Databend;
use anyhow::Result;
use databend_driver::{Client, Connection};

pub mod log;
pub(crate) mod query_plan;
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

// 查询日志volume的metrics时要把时间转成In64，设置numeric_cast_option = 'truncating'
// 保证所有的数值都向下取整，不然会出现不一致
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
