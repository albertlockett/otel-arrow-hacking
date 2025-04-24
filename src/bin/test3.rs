use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::{Catalog, NamespaceIdent, TableCreation, io::FileIOBuilder};
use iceberg_catalog_memory::MemoryCatalog;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

#[tokio::main]
async fn main() {
    let file_io = FileIOBuilder::new_fs_io().build().unwrap();
    let warehouse_location = "/Users/albertlockett/iceberg_warehouse";
    let catalog = MemoryCatalog::new(file_io, Some(warehouse_location.into()));

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.into(),
            "1".into(),
        )])),
    ]));
    let iceberg_schema = arrow_schema_to_schema(&schema).unwrap();

    let ns_ident = NamespaceIdent::new("ns1".into());
    let ns = catalog
        .create_namespace(&ns_ident, HashMap::new())
        .await
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("table1".into())
        .schema(iceberg_schema)
        .build();
    let table = catalog.create_table(ns.name(), table_creation).await;
    println!("{:?}", table);
}
