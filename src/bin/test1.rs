use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use object_store::{local::LocalFileSystem, path::Path};
use parquet::{
    arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter},
    file::properties::WriterProperties,
};

use otel_arrow_hacking::arrow::gen_arrow_schema;

#[tokio::main]
async fn main() {
    let ids = Int32Array::from_iter_values(vec![1, 2, 3, 4, 5]);
    let schema = Arc::new(Schema::new(vec![Field::new("ids", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)]).unwrap();

    let store = Arc::new(
        LocalFileSystem::new_with_prefix("/Users/albertlockett/Desktop/parquet_files").unwrap(),
    );
    let parquet_os_writer = ParquetObjectWriter::new(store.clone(), Path::from("test1"));
    let mut parquet_writer = AsyncArrowWriter::try_new(
        parquet_os_writer,
        schema.clone(),
        // or pass None, che pas
        Some(WriterProperties::default()),
    )
    .unwrap();

    parquet_writer.write(&batch).await.unwrap();
    parquet_writer.close().await.unwrap();
}
