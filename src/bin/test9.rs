use std::sync::Arc;

use arrow_array::{FixedSizeBinaryArray, RecordBatch, UInt8Array, UInt8DictionaryArray};
use arrow_schema::{DataType, Field, Schema};
use object_store::{local::LocalFileSystem, path::Path};
use parquet::{
    arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter},
    file::properties::WriterProperties,
};

#[tokio::main]
async fn main() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Dictionary(
            Box::new(DataType::UInt8),
            Box::new(DataType::FixedSizeBinary(16)),
        ),
        false,
    )]));
    let keys = UInt8Array::from_iter_values(vec![0, 0, 0, 1, 1, 1, 2, 2]);
    let values = FixedSizeBinaryArray::try_from_iter(
        vec![
            vec![0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
            vec![2, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
            vec![2, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
        ]
        .into_iter(),
    )
    .unwrap();
    let arr = UInt8DictionaryArray::new(keys, Arc::new(values));

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();

    let object_store = Arc::new(LocalFileSystem::new_with_prefix("/tmp/").unwrap());
    let parquet_object_writer =
        ParquetObjectWriter::new(object_store.clone(), Path::from("albert.parquet"));
    let mut parquet_writer = AsyncArrowWriter::try_new(
        parquet_object_writer,
        schema.clone(),
        Some(WriterProperties::default()),
    )
    .unwrap();
    parquet_writer.write(&batch).await.unwrap();
    parquet_writer.close().await.unwrap();
}
