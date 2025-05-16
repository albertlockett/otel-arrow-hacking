use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use arrow_array::{RecordBatch, UInt8Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::{execution::context::SessionContext, sql::TableReference};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use parquet::errors::ParquetError;
use parquet::{
    arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter},
    file::properties::WriterProperties,
};

const PARQUET_DIR: &str = "/Users/a.lockett/Desktop/parquet_files";

#[tokio::main]
async fn main() {
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(PARQUET_DIR).unwrap());

    let schema = Arc::new(Schema::new(vec![
        // Field::new("part", DataType::UInt8, false),
        Field::new("id", DataType::UInt8, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(), 
        vec![
            // Arc::new(UInt8Array::from_iter_values(vec![1, 1, 1])),
            Arc::new(UInt8Array::from_iter_values(vec![1, 2, 3])),
        ]
    ).unwrap();

    export_to_parquet(&batch1, "part_test1/part=1/data1.parquet", object_store.clone()).await.unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(), 
        vec![
            // Arc::new(UInt8Array::from_iter_values(vec![2, 2, 2])),
            Arc::new(UInt8Array::from_iter_values(vec![4, 5, 6])),
        ]
    ).unwrap();
    
    export_to_parquet(&batch2, "part_test1/part=2/data2.parquet", object_store.clone()).await.unwrap();


    let ctx = SessionContext::new();
    let table_ref = TableReference::bare("part_test1");


    ctx.register_parquet(table_ref, format!("{}/part_test1", PARQUET_DIR), ParquetReadOptions {
        table_partition_cols: vec![("part".to_string(), DataType::Int32)],
        schema: Some(&schema.clone()),
        ..Default::default()
    })
    .await
    .unwrap();

    let df = ctx.sql("select * from part_test1 where part=1").await.unwrap();
    let batches = df.explain(true, false).unwrap().collect().await.unwrap();
    for batch in batches {
        println!("{}", pretty_format_batches(&[batch]).unwrap())
    }

    let df = ctx.sql("select * from part_test1 where part=1").await.unwrap();
    let batches = df.collect().await.unwrap();
    for batch in batches {
        println!("{}", pretty_format_batches(&[batch]).unwrap())
    }
}

async fn export_to_parquet(batch: &RecordBatch, path: &str, object_store: Arc<LocalFileSystem>) -> Result<(), ParquetError> {
    let parquet_object_writer =
        ParquetObjectWriter::new(object_store.clone(), Path::from(path));

    let mut parquet_writer = AsyncArrowWriter::try_new(
        parquet_object_writer,
        batch.schema().clone(),
        Some(WriterProperties::default()),
    )?;

    parquet_writer.write(batch).await?;
    parquet_writer.close().await?;

    Ok(())
}