use crate::data_type::{RecordBatchStream, Schema};

pub trait DataSource: Send + Sync {
    fn schema(&self) -> Schema;
    fn scan(&self, projection: Vec<String>) -> anyhow::Result<RecordBatchStream>;
}
