use crate::data_source::DataSource;
use crate::data_type::{RecordBatchStream, Schema};
use crate::physical_plan::PhysicalPlan;
use std::sync::Arc;

pub struct ScanExec {
    source: Box<dyn DataSource>,
    projection: Vec<String>,
}

impl PhysicalPlan for ScanExec {
    fn schema(&self) -> Schema {
        self.source.schema().select(self.projection.clone())
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![]
    }

    fn execute(&self) -> anyhow::Result<RecordBatchStream> {
        self.source.scan(self.projection.clone())
    }
}
