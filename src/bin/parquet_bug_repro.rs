use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    util::pretty::print_batches,
};
use arrow_array::{FixedSizeBinaryArray, RecordBatch, UInt8Array, UInt8DictionaryArray};
use datafusion::{
    prelude::{ParquetReadOptions, SessionContext},
    sql::TableReference,
};
use object_store::{local::LocalFileSystem, path::Path};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::WriterProperties,
};

#[tokio::main]
async fn main() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Dictionary(
            Box::new(DataType::UInt8),
            Box::new(DataType::FixedSizeBinary(8)),
        ),
        true,
    )]));

    let keys = UInt8Array::from_iter_values(vec![0, 0, 1]);
    // let values = ;
    let values = FixedSizeBinaryArray::try_from_iter(
        vec![
            (0u8..8u8).into_iter().collect::<Vec<u8>>(),
            (24u8..32u8).into_iter().collect::<Vec<u8>>(),
        ]
        .into_iter(),
    )
    .unwrap();
    let arr = UInt8DictionaryArray::new(keys, Arc::new(values));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    // write batch to parquet
    let object_store = Arc::new(LocalFileSystem::new_with_prefix("/tmp").unwrap());
    let parquet_object_writer =
        ParquetObjectWriter::new(object_store.clone(), Path::from("test.parquet"));
    let mut parquet_writer = AsyncArrowWriter::try_new(
        parquet_object_writer,
        batch.schema().clone(),
        Some(WriterProperties::default()),
    )
    .unwrap();
    parquet_writer.write(&batch).await.unwrap();
    parquet_writer.close().await.unwrap();

    // read directly using parquet (this works)
    let file = std::fs::File::open("/tmp/test.parquet").unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.build().unwrap();
    let read_batch = reader.next().unwrap().unwrap();
    print_batches(&[read_batch]).unwrap();

    // read using datafusion (this does not work)
    let ctx = SessionContext::new();
    ctx.register_parquet(
        TableReference::bare("tab"),
        "/tmp/test.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    let df = ctx.sql("select * from tab").await.unwrap();
    let batches = df.collect().await.unwrap();
    print_batches(&batches).unwrap();
}
