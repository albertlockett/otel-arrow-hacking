use std::io::Cursor;
use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use arrow_ipc::reader::StreamReader;
// use datafusion::common::file_options::parquet_writer;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use otel_arrow_rust::opentelemetry::{ArrowPayloadType, BatchArrowRecords};
use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use parquet::file::properties::WriterProperties;
use prost::Message;
use tokio::io::AsyncReadExt;
use tokio::fs::File;

const BASE_PARQUET_DIR: &str = "/Users/albertlockett/Desktop/parquet_files";
const OTAP_SAMPLES_PATH: &str = "/Users/albertlockett/Development/otel-arrow/data/otlp_traces.json";
// const OTAP_SAMPLES_PATH: &str = "/Users/albertlockett/Development/otel-arrow/data/otlp_logs.pb";

#[tokio::main]
async fn main() {
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(BASE_PARQUET_DIR).unwrap());

    let mut file = File::open(OTAP_SAMPLES_PATH).await.unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).await.unwrap();
    let otap_batch_records = BatchArrowRecords::decode(contents.as_ref()).unwrap();

    for payload in otap_batch_records.arrow_payloads {
        println!("fuckin w/ payload");
        let cursor = Cursor::new(payload.record);
        let ipc_stream_reader = StreamReader::try_new(cursor, None).unwrap();
        let payload_type = ArrowPayloadType::try_from(payload.r#type).unwrap();
        let table_name = format!("{:?}", payload_type).to_lowercase();

        let mut peekable_stream_reader = ipc_stream_reader.peekable();
        let batch1 = peekable_stream_reader.peek().unwrap().as_ref();
        if batch1.is_err() {
            println!("err {:?}", batch1.unwrap_err());
            continue
        }
        let batch1 = batch1.unwrap();

        let schema = batch1.schema();

        println!("table {} has schema:\n{:#?}", table_name, schema);

        let path = Path::from(format!("{}/data.parquet", table_name));
        let parquet_object_writer = ParquetObjectWriter::new(object_store.clone(), path);
        let parquet_writer = AsyncArrowWriter::try_new(
            parquet_object_writer,
            schema.clone(),
            Some(WriterProperties::default())
        );
        if parquet_writer.is_err() {
            if let Err(e) = parquet_writer {
                println!("err 2 {}", e);
            }
            continue;
        }
        let mut parquet_writer = parquet_writer.unwrap();
        for batch in peekable_stream_reader {
            let batch = batch.unwrap();
            
            println!("writing batch to table {}", table_name);
            println!("{}", pretty_format_batches(&[batch.clone()]).unwrap());
            parquet_writer.write(&batch).await.unwrap();
        }
        parquet_writer.close().await.unwrap();
    }
}