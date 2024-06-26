use super::builder::Condition;
use logql::parser::*;

pub trait SelectionVisitor {
    fn label_pair(&self, label: &LabelPair) -> Condition;
    fn log_filter(&self, filter: &LogLineFilter) -> Condition;
}

pub struct LogQLVisitor<T> {
    udf: T,
}

impl<T> LogQLVisitor<T> {
    pub fn new(udf: T) -> Self {
        Self { udf }
    }
}