use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::DataFileFormat;
use iceberg::transaction::Transaction;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::writer::{
    base_writer::data_file_writer::DataFileWriterBuilder,
    file_writer::{
        ParquetWriterBuilder,
        location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
    },
};
use iceberg::TableIdent;
use iceberg::{Catalog, NamespaceIdent, TableCreation, io::FileIOBuilder};
// use iceberg_catalog_memory::MemoryCatalog;
// use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogConfig};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use sqlx::migrate::MigrateDatabase;

#[tokio::main]
async fn main() {
    let warehouse_location = "/Users/albertlockett/iceberg_warehouse";
    // let sqlite_uri = format!("sqlite:{}/catalog.sqlite", warehouse_location);
    // sqlx::Sqlite::create_database(&sqlite_uri).await.unwrap();

    let config = RestCatalogConfig::builder()
        .uri("http://localhost:8181".to_string())
        .props(HashMap::from([
            (S3_ENDPOINT.into(), format!("http://{}:{}", "127.0.0.1", 9000)),
            (S3_ACCESS_KEY_ID.into(), "admin".into()),
            (S3_SECRET_ACCESS_KEY.into(), "password".into()),
            (S3_REGION.into(), "us-east-1".into()),
        ]))
        // .
        .build();
    let catalog = RestCatalog::new(config);

    // let file_io = FileIOBuilder::new_fs_io().build().unwrap();
    // let sql_catalog_config = SqlCatalogConfig::builder()
    //     .uri(sqlite_uri)
    //     .name("iceberg".into())
    //     .warehouse_location(warehouse_location.into())
    //     .file_io(file_io)
    //     .sql_bind_style(SqlBindStyle::QMark)
    //     .build();
    // let catalog = SqlCatalog::new(sql_catalog_config).await.unwrap();
    // let catalog = MemoryCatalog::new(file_io, Some(warehouse_location.into()));

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.into(),
            "1".into(),
        )])),
    ]));
    let iceberg_schema = arrow_schema_to_schema(&schema).unwrap();

    let ns_ident = NamespaceIdent::new("ns1".into());
    // if catalog.namespace_exists(&ns_ident).await.unwrap() {
    // let ns = catalog
    //     .create_namespace(&ns_ident, HashMap::new())
    //     .await
    //     .unwrap();
    let ns = catalog.get_namespace(&ns_ident).await.unwrap();

    let table_creation = TableCreation::builder()
        .name("table1".into())
        .schema(iceberg_schema.clone())
        .build();
    // let table = catalog
    //     .create_table(ns.name(), table_creation)
    //     .await
    //     .unwrap();
    let table_ident = TableIdent::new(ns.name().clone(), "table1".into());
    let table = catalog.load_table(&table_ident).await.unwrap();
    println!("{:?}", table);

    let parquet_file_writer = ParquetWriterBuilder::new(
        WriterProperties::default(),
        Arc::new(iceberg_schema),
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata().clone()).unwrap(),
        DefaultFileNameGenerator::new("prefix1".into(), None, DataFileFormat::Parquet),
    );

    let mut data_file_writer = DataFileWriterBuilder::new(parquet_file_writer, None)
        .build()
        .await
        .unwrap();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter_values([1, 2, 3, 4]))],
    )
    .unwrap();

    data_file_writer.write(batch).await.unwrap();
    let data_files = data_file_writer.close().await.unwrap();
    println!("{:?}", data_files);

    let mut txn = Transaction::new(&table)
        .fast_append(None, Vec::new())
        .unwrap();
    txn.add_data_files(data_files).unwrap();
    let txn = txn.apply().await.unwrap();
    let table_after = txn.commit(&catalog).await.unwrap();
    println!("{:?}", table_after);
}
