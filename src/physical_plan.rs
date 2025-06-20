mod scan;
mod filter;
mod project;

use crate::data_source::DataSource;
use crate::data_type::{ColumnVector, RecordBatch, RecordBatchStream, Schema};
use std::sync::Arc;

trait PhysicalPlan {
    fn schema(&self) -> Schema;
    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>>;
    fn execute(&self) -> anyhow::Result<RecordBatchStream>;
}

trait Expression {
    fn evaluate(&self, input: &RecordBatch) -> ColumnVector;
}