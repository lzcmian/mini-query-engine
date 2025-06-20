#[derive(Debug, Clone)]
enum Datatype {
    Boolean,
    Int64,
    Float64,
    String,
}

#[derive(Clone)]
pub enum ColumnVector {
    Boolean(Vec<bool>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    String(Vec<String>),
}

impl ColumnVector {
    fn len(&self) -> usize {
        match self {
            ColumnVector::Boolean(v) => v.len(),
            ColumnVector::Int64(v) => v.len(),
            ColumnVector::Float64(v) => v.len(),
            ColumnVector::String(v) => v.len(),
        }
    }

    fn get_type(&self) -> Datatype {
        match self {
            ColumnVector::Boolean(_) => Datatype::Boolean,
            ColumnVector::Int64(_) => Datatype::Int64,
            ColumnVector::Float64(_) => Datatype::Float64,
            ColumnVector::String(_) => Datatype::String,
        }
    }

    pub fn filter(&self, bool_mask: &[bool]) -> ColumnVector {
        if bool_mask.len() != self.len() {
            panic!("Boolean mask length does not match column vector length");
        }
        match self {
            ColumnVector::Boolean(v) => {
                let new_v: Vec<bool> = v
                    .iter()
                    .zip(bool_mask.iter())
                    .filter(|(_, m)| **m)
                    .map(|(val, _)| *val)
                    .collect();
                ColumnVector::Boolean(new_v)
            }
            ColumnVector::Int64(v) => {
                let new_v: Vec<i64> = v
                    .iter()
                    .zip(bool_mask.iter())
                    .filter(|(_, m)| **m)
                    .map(|(val, _)| *val)
                    .collect();
                ColumnVector::Int64(new_v)
            }
            ColumnVector::Float64(v) => {
                let new_v: Vec<f64> = v
                    .iter()
                    .zip(bool_mask.iter())
                    .filter(|(_, m)| **m)
                    .map(|(val, _)| *val)
                    .collect();
                ColumnVector::Float64(new_v)
            }
            ColumnVector::String(v) => {
                let new_v: Vec<String> = v
                    .iter()
                    .zip(bool_mask.iter())
                    .filter(|(_, m)| **m)
                    .map(|(val, _)| val.clone())
                    .collect();
                ColumnVector::String(new_v)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Field {
    name: String,
    datatype: Datatype,
}

#[derive(Debug, Clone)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    // 根据字段的索引（位置）创建一个新的 Schema。
    pub fn project(&self, keep_indices: &[usize]) -> Schema {
        let projected_fields = keep_indices
            .iter()
            .map(|&index| {
                self.fields
                    .get(index)
                    .expect("Project index out of bounds.")
                    .clone()
            })
            .collect();

        Schema {
            fields: projected_fields,
        }
    }

    // 根据字段的名称创建一个新的 Schema。
    pub fn select(&self, names: Vec<String>) -> Schema {
        let selected_fields = self
            .fields
            .iter()
            .filter(|&field| names.contains(&field.name))
            .cloned()
            .collect();

        Schema {
            fields: selected_fields,
        }
    }
}

#[derive(Clone)]
pub struct RecordBatch {
    pub(crate) schema: Schema,
    fields: Vec<ColumnVector>,
}

impl RecordBatch {
    pub fn new(schema: Schema, fields: Vec<ColumnVector>) -> RecordBatch {
        if schema.fields.len() != fields.len() {
            panic!("Schema and fields length mismatch");
        }

        if fields.is_empty() {
            panic!("RecordBatch cannot have empty fields");
        }

        RecordBatch { schema, fields }
    }
    pub fn row_count(&self) -> usize {
        self.fields[0].len()
    }

    pub fn field(&self, i: usize) -> &ColumnVector {
        &self.fields[i]
    }

    pub fn filter(&self, mask_cv_param: &ColumnVector) -> RecordBatch {
        let bool_vec = match mask_cv_param {
            ColumnVector::Boolean(b) => b,
            _ => {
                panic!("Predicate expression must evaluate to a Boolean ColumnVector for filtering")
            }
        };

        if bool_vec.len() != self.row_count() {
            panic!("Filter mask length does not match record batch row count");
        }

        let mut new_fields = Vec::with_capacity(self.fields.len());
        for column_vector in &self.fields {
            let new_cv = column_vector.filter(bool_vec);
            new_fields.push(new_cv);
        }

        RecordBatch {
            schema: self.schema.clone(),
            fields: new_fields,
        }
    }

    pub fn project(&self, keep_indices: &[usize]) -> RecordBatch {
        let projected_fields = keep_indices
            .iter()
            .map(|&index| self.fields[index].clone())
            .collect();
        RecordBatch::new(self.schema.project(keep_indices), projected_fields)
    }
}

pub type RecordBatchStream = Box<dyn Iterator<Item = anyhow::Result<RecordBatch>>>;
