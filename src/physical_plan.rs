use crate::data_source::DataSource;
use crate::data_type::{ColumnVector, RecordBatch, RecordBatchStream, Schema};
use std::sync::Arc;

trait PhysicalPlan {
    fn schema(&self) -> Schema;
    fn children(&self) -> Vec<Arc<dyn PhysicalPlan>>;
    fn execute(&self) -> Result<RecordBatchStream, std::io::Error>;
}

trait Expression {
    fn evaluate(&self, input: &RecordBatch) -> ColumnVector;
}

struct ScanExec {
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

    fn execute(&self) -> Result<RecordBatchStream, std::io::Error> {
        self.source.scan(self.projection.clone())
    }
}

//
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

    fn execute(&self) -> Result<RecordBatchStream, std::io::Error> {
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

    fn execute(&self) -> Result<RecordBatchStream, std::io::Error> {
        let input_stream = self.input.execute()?;
        let projected_stream = input_stream.map(move |batch_result| match batch_result {
            Ok(batch) => {
                let projected_batch = batch.project(self.expr);
                Ok(projected_batch)
            }
            Err(_) => batch_result,
        });
        Ok(Box::new(projected_stream))
    }
}
