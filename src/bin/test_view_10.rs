use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Define and register 'people' table
    let people_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let people_batch = RecordBatch::try_new(
        people_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )?;
    let people_table = MemTable::try_new(people_schema, vec![vec![people_batch]])?;
    ctx.register_table("people", Arc::new(people_table))?;

    // Define and register 'orders' table
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int32, false),
        Field::new("item", DataType::Utf8, false),
    ]));
    let orders_batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 2])),
            Arc::new(StringArray::from(vec!["Book", "Pen", "Pencil"])),
        ],
    )?;
    let orders_table = MemTable::try_new(orders_schema, vec![vec![orders_batch]])?;
    ctx.register_table("orders", Arc::new(orders_table))?;

    // Perform join
    let df_people = ctx.table("people").await?;
    let df_orders = ctx.table("orders").await?;
    let joined_df = df_people.join(
        df_orders,
        JoinType::Inner,
        &["id"],        // left join key
        &["person_id"], // right join key
        None,
    )?;

    // Register the join result as a view
    ctx.register_table("person_orders", joined_df.clone().into_view())?;

    // Query the view
    let results = ctx.sql("SELECT name, item FROM person_orders").await?;
    results.show().await?;

    Ok(())
}
