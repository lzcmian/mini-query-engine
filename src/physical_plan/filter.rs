use std::sync::Arc;
use crate::data_type::{RecordBatchStream, Schema};
use crate::physical_plan::{Expression, PhysicalPlan};

struct FilterExec {
    input: Arc<dyn PhysicalPlan>,
    predicate: Arc<dyn Expression>,
}

impl PhysicalPlan for FilterExec {
    fn schema(&self) -> Schema {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn execute(&self) -> anyhow::Result<RecordBatchStream> {
        let input_stream = self.input.execute()?;
        let predicate = Arc::clone(&self.predicate);

        let filtered_stream = input_stream.map(move |batch_result| match batch_result {
            Ok(batch) => {
                let mask = predicate.evaluate(&batch);
                Ok(batch.filter(&mask))
            }
            Err(_) => batch_result,
        });
        Ok(Box::new(filtered_stream))
    }
}