use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use object_store::{local::LocalFileSystem, path::Path};
use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};

fn main() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let mut ids = Int32Array::builder(4);
    ids.append_value(1);
    ids.append_value(2);
    ids.append_null();
    ids.append_value(4);
    let ids = ids.finish();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)]).unwrap();
    println!("batch: {:?}", batch);
}
