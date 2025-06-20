// hash join

use crate::data_type::{RecordBatchStream, Schema};
use crate::physical_plan::{Expression, PhysicalPlan};
use std::sync::Arc;

pub struct HashJoinExec {
    schema: Schema,
    left: Arc<dyn PhysicalPlan>,
    right: Arc<dyn PhysicalPlan>,
    on_left: Arc<dyn Expression>,
    on_right: Arc<dyn Expression>,
}

impl PhysicalPlan for HashJoinExec {
    fn schema(&self) -> Schema {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        todo!()
    }

    fn execute(&self) -> anyhow::Result<RecordBatchStream> {
        todo!()
    }
}
