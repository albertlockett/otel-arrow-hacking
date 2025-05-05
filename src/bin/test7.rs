use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::{execution::context::SessionContext, sql::TableReference};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use parquet::{
    arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter},
    file::properties::WriterProperties,
};

const PARQUET_DIR: &str = "/Users/albertlockett/Desktop/parquet_files";

#[tokio::main]
async fn main() {
    let ids = Int32Array::from_iter_values(vec![1, 2, 3]);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)]).unwrap();

    let store = Arc::new(LocalFileSystem::new_with_prefix(PARQUET_DIR).unwrap());
    let parquet_object_writer =
        ParquetObjectWriter::new(store.clone(), Path::from("test1.parquet"));
    let mut parquet_writer = AsyncArrowWriter::try_new(
        parquet_object_writer,
        schema.clone(),
        Some(WriterProperties::default()),
    )
    .unwrap();
    parquet_writer.write(&batch).await.unwrap();
    parquet_writer.close().await.unwrap();

    let parquet_object_writer =
        ParquetObjectWriter::new(store.clone(), Path::from("test2.parquet"));
    let mut parquet_writer = AsyncArrowWriter::try_new(
        parquet_object_writer,
        schema.clone(),
        Some(WriterProperties::default()),
    )
    .unwrap();
    parquet_writer.write(&batch).await.unwrap();
    parquet_writer.close().await.unwrap();

    let ctx = SessionContext::new();

    let table_ref = TableReference::bare("test7");

    ctx.register_parquet(table_ref, PARQUET_DIR, ParquetReadOptions::default())
        .await
        .unwrap();

    let batches = ctx
        .sql("select * from test7")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    for batch in batches {
        println!("{}", pretty_format_batches(&[batch]).unwrap())
    }
}
