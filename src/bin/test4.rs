use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use iceberg::Catalog;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_datafusion::IcebergCatalogProvider;

#[tokio::main]
async fn main() {
    env_logger::init();

    let catalog_config = RestCatalogConfig::builder()
        .uri("http://localhost:8181".into())
        .props(HashMap::from([
            (
                S3_ENDPOINT.into(),
                format!("http://{}:{}", "127.0.0.1", 9000),
            ),
            (S3_ACCESS_KEY_ID.into(), "admin".into()),
            (S3_SECRET_ACCESS_KEY.into(), "password".into()),
            (S3_REGION.into(), "us-east-1".into()),
        ]))
        .build();
    let catalog = Arc::new(RestCatalog::new(catalog_config));

    let namespaces = catalog.list_namespaces(None).await.unwrap();
    for ns in namespaces {
        println!("ns = {:?}", ns);
    }

    let catalog_provider = Arc::new(IcebergCatalogProvider::try_new(catalog).await.unwrap());

    let ctx = SessionContext::new();
    ctx.register_catalog("iceberg", catalog_provider);

    let batches = ctx
        .sql("select * from iceberg.ns1.table1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    for batch in batches {
        println!("{:?}", batch)
    }
}
