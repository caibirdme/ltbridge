use super::{log::LogStorage, trace::TraceStorage};
use crate::config::Clickhouse;
use anyhow::Result;
use ::clickhouse::Client;

pub mod log;
pub mod trace;
pub(crate) mod common;
pub(crate) mod converter;


pub async fn new_log_source(cfg: Clickhouse) -> Result<Box<dyn LogStorage>> {
    let cli = Client::default()
        .with_url(cfg.url)
        .with_user(cfg.username)
        .with_password(cfg.password)
        .with_database(cfg.database);
    Ok(Box::new(log::CKLogQuerier::new(cli, cfg.table)))
}

pub async fn new_trace_source(cfg: Clickhouse) -> Result<Box<dyn TraceStorage>> {
    let cli = Client::default()
        .with_url(cfg.url)
        .with_user(cfg.username)
        .with_password(cfg.password)
        .with_database(cfg.database);
    Ok(Box::new(trace::CKTraceQuerier::new(cli, cfg.table)))
}