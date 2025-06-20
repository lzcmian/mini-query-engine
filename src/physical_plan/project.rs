use crate::data_type::{RecordBatch, RecordBatchStream, Schema};
use crate::physical_plan::{Expression, PhysicalPlan};
use anyhow::Context;
use std::sync::Arc;

struct ProjectionExec {
    input: Arc<dyn PhysicalPlan>,
    schema: Schema,
    expr: Vec<Arc<dyn Expression>>, // 顺序与schema要求的顺序保持一致
}

impl PhysicalPlan for ProjectionExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn execute(&self) -> anyhow::Result<RecordBatchStream> {
        let input_stream = self
            .input
            .execute()
            .context("Failed to execute projection: error executing child physical plan")?;

        let schema = self.schema.clone();
        let expr = self.expr.clone();

        let projected_stream = input_stream.map(move |batch_result| {
            batch_result.and_then(|batch| {
                // transform batch to new batch
                let fields = expr.iter().map(|e| e.evaluate(&batch)).collect::<Vec<_>>();
                Ok(RecordBatch::new(schema.clone(), fields))
            })
        });

        Ok(Box::new(projected_stream))
    }
}
