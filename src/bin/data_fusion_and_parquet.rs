use std::sync::Arc;

use arrow::datatypes::UInt16Type;
use arrow::util::pretty::print_batches;
use arrow_array::RecordBatch;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion::sql::TableReference;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::file::properties::WriterProperties;
use prost::Message;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use otel_arrow_rust::Consumer;
use otel_arrow_rust::otap::transform::{remove_delta_encoding, sort_by_parent_id};
use otel_arrow_rust::otap::{OtapBatch, from_record_messages};
use otel_arrow_rust::proto::opentelemetry::arrow::v1::{ArrowPayloadType, BatchArrowRecords};
use otel_arrow_rust::schema::consts;

// will need to have ran:
// `go run ./tools/logs_gen/main.go`
// In the `go` directory of otel-arrow to generate some data
const SERIALIZED_OTAP_LOGS_PATH: &str = "../otel-arrow/go/tools/logs_gen/data/otap_logs.pb";

#[tokio::main]
async fn main() {
    let mut batch_arrow_records = read_batch_arrow_record(SERIALIZED_OTAP_LOGS_PATH).await;
    let mut consumer = Consumer::default();
    let record_messages = consumer.consume_bar(&mut batch_arrow_records).unwrap();
    let mut otap_batch = OtapBatch::Logs(from_record_messages(record_messages));
    optimize_record_batches(&mut otap_batch);

    let ctx = SessionContext::new();

    // Register in-memory tables and run some queries

    if let Some(rb) = otap_batch.get(ArrowPayloadType::Logs) {
        ctx.register_batch("logs", rb.clone()).unwrap();
    }
    if let Some(rb) = otap_batch.get(ArrowPayloadType::LogAttrs) {
        ctx.register_batch("log_attrs", rb.clone()).unwrap();
    }

    let data_frame = ctx.sql("select * from logs limit 10").await.unwrap();
    let batches = data_frame.collect().await.unwrap();
    println!("Logs:");
    print_batches(&batches).unwrap();

    let data_frame = ctx.sql("select * from log_attrs").await.unwrap();
    let batches = data_frame.collect().await.unwrap();
    println!("\nAttributes:");
    print_batches(&batches).unwrap();

    let data_frame = ctx
        .sql(
            "
        select 
            logs.id,
            logs.time_unix_nano,
            logs.body.str,
            log_attrs.str as hostname
        from logs 
        inner join log_attrs on logs.id == log_attrs.parent_id
        where
            log_attrs.key == 'hostname' and
            log_attrs.type = 1 -- Str
    ",
        )
        .await
        .unwrap();
    let batches = data_frame.collect().await.unwrap();
    println!("\nExample join in memory:");
    print_batches(&batches).unwrap();

    // Write record batches to parquet
    let object_store = Arc::new(LocalFileSystem::new_with_prefix("/tmp").unwrap());

    if let Some(rb) = otap_batch.get(ArrowPayloadType::Logs) {
        export_to_parquet(rb, "logs.parquet", object_store.clone()).await;
    }
    if let Some(rb) = otap_batch.get(ArrowPayloadType::LogAttrs) {
        export_to_parquet(rb, "log_attrs.parquet", object_store.clone()).await;
    }

    // register parquet tables
    ctx.register_parquet(
        TableReference::bare("pq_logs"),
        "/tmp/logs.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx.register_parquet(
        TableReference::bare("pq_log_attrs"),
        "/tmp/log_attrs.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    // run some example queries
    // Note don't select trace_id & span_id from parquet, there's some bug reading those

    let data_frame = ctx
        .sql("select id, time_unix_nano, body from pq_logs")
        .await
        .unwrap();
    let batches = data_frame.collect().await.unwrap();
    println!("\n Parquet logs:");
    print_batches(&batches).unwrap();

    let data_frame = ctx
        .sql(
            "
        select
            pq_logs.id, 
            pq_logs.time_unix_nano,
            pq_logs.body.str,
            pq_log_attrs.*
        from
            pq_logs
            inner join pq_log_attrs on pq_logs.id == pq_log_attrs.parent_id
        where
            pq_log_attrs.str == 'host2.org' and
            pq_log_attrs.key == 'hostname'
        ",
        )
        .await
        .unwrap();
    let batches = data_frame.collect().await.unwrap();
    println!("\nExample join parquet:");
    print_batches(&batches).unwrap();
}

async fn read_batch_arrow_record(path: &str) -> BatchArrowRecords {
    let mut file = File::open(path).await.unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).await.unwrap();
    BatchArrowRecords::decode(contents.as_ref()).unwrap()
}

fn optimize_record_batches(otap_batch: &mut OtapBatch) {
    if let Some(rb) = otap_batch.get(ArrowPayloadType::Logs) {
        let optimized = remove_delta_encoding::<UInt16Type>(rb, consts::ID).unwrap();
        otap_batch.set(ArrowPayloadType::Logs, optimized);
    }
    if let Some(rb) = otap_batch.get(ArrowPayloadType::LogAttrs) {
        let optimized = sort_by_parent_id(rb).unwrap();
        otap_batch.set(ArrowPayloadType::LogAttrs, optimized);
    }

    if let Some(rb) = otap_batch.get(ArrowPayloadType::ResourceAttrs) {
        let optimized = sort_by_parent_id(rb).unwrap();
        otap_batch.set(ArrowPayloadType::ResourceAttrs, optimized);
    }

    if let Some(rb) = otap_batch.get(ArrowPayloadType::ScopeAttrs) {
        let optimized = sort_by_parent_id(rb).unwrap();
        otap_batch.set(ArrowPayloadType::ScopeAttrs, optimized);
    }
}

async fn export_to_parquet(batch: &RecordBatch, path: &str, object_store: Arc<LocalFileSystem>) {
    let parquet_object_writer = ParquetObjectWriter::new(object_store.clone(), Path::from(path));

    let mut parquet_writer = AsyncArrowWriter::try_new(
        parquet_object_writer,
        batch.schema().clone(),
        Some(WriterProperties::default()),
    )
    .unwrap();

    parquet_writer.write(batch).await.unwrap();
    parquet_writer.close().await.unwrap();
}
