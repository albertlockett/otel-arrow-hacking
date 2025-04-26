
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_ipc::reader::StreamReader;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::DataFileFormat;
use iceberg::transaction::Transaction;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use otel_arrow_rust::opentelemetry::{ArrowPayloadType, BatchArrowRecords};
use parquet::file::properties::WriterProperties;
use prost::Message;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

const ICEBERG_CATALOG_REST_URI: &str = "http://localhost:8181";
const ICEBERG_CATALOG_S3_ENDPOINT: &str = "http://127.0.0.1:9090";
const ICEBERG_CATALOG_S3_ACCESS_KEY_ID: &str = "admin";
const ICEBERG_CATALOG_S3_SECRET_ACCESS_KEY: &str = "password";
const ICEBERG_CATALOG_S3_REGION: &str = "us-east-1";
const OTAP_SAMPLES_PATH: &str = "/Users/albertlockett/Development/otel-arrow/data/otlp_traces.json";

#[tokio::main]
async fn main() {
    let catalog_config = RestCatalogConfig::builder()
        .uri(ICEBERG_CATALOG_REST_URI.into())
        .props(HashMap::from([
            (S3_ENDPOINT.into(), ICEBERG_CATALOG_S3_ENDPOINT.into()),
            (S3_ACCESS_KEY_ID.into(), ICEBERG_CATALOG_S3_ACCESS_KEY_ID.into()),
            (S3_SECRET_ACCESS_KEY.into(), ICEBERG_CATALOG_S3_SECRET_ACCESS_KEY.into()),
            (S3_REGION.into(), ICEBERG_CATALOG_S3_REGION.into()),
        ]))
        .build();
    let catalog = RestCatalog::new(catalog_config);
    let ns_ident = NamespaceIdent::new("otap".into());

    let ns = match catalog.get_namespace(&ns_ident).await {
        Ok(namespace) => namespace,
        Err(e) => {
            println!("error getting ns {:?}", e);
            catalog.create_namespace(&ns_ident, HashMap::new()).await.unwrap()
        }
    };
    
    let mut file = File::open(OTAP_SAMPLES_PATH).await.unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).await.unwrap();
    let otap_batch_records = BatchArrowRecords::decode(contents.as_ref()).unwrap();

    for payload in otap_batch_records.arrow_payloads {
        let cursor = Cursor::new(payload.record);
        let ipc_stream_reader = StreamReader::try_new(cursor, None).unwrap();
        let payload_type = ArrowPayloadType::try_from(payload.r#type).unwrap();
        let table_name = format!("{:?}", payload_type).to_lowercase();

        for record_batch in ipc_stream_reader {
            let record_batch = record_batch.unwrap();
            let schema = record_batch.schema();
            let iceberg_schema = arrow_schema_to_schema(&schema).unwrap();
            
            let table_ident = TableIdent::new(ns.name().clone(), table_name.clone());
            // create table if doesn't exist
            if !catalog.table_exists(&table_ident).await.unwrap() {
                
                let table_creation = TableCreation::builder()
                    .name(table_name.clone())
                    .schema(iceberg_schema.clone())
                    .build();
                catalog.create_table(ns.name(), table_creation).await.unwrap();
            }

            let table = catalog.load_table(&table_ident).await.unwrap();
            let parquet_file_writer = ParquetWriterBuilder::new(
                WriterProperties::default(),
                Arc::new(iceberg_schema),
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone()).unwrap(),
                DefaultFileNameGenerator::new("otap-data".into(), None, DataFileFormat::Parquet),
            );

            let mut data_file_writer = DataFileWriterBuilder::new(parquet_file_writer, None)
                .build()
                .await
                .unwrap();
            data_file_writer.write(record_batch).await.unwrap();
            let data_files = data_file_writer.close().await.unwrap();
            let mut txn = Transaction::new(&table)
                .fast_append(None, Vec::new())
                .unwrap();
            txn.add_data_files(data_files)
                .unwrap();
            txn.apply().await.unwrap()
                .commit(&catalog)
                .await
                .unwrap();

        }
    }
}