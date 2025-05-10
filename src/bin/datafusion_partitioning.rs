use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::{datasource::MemTable, functions::math::log, physical_plan::display::DisplayableExecutionPlan, prelude::SessionContext};
use futures::StreamExt;

#[tokio::main]
async fn main() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter_values(vec![1, 2, 3, 4]))],
    )
    .unwrap();
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter_values(vec![5, 6, 7, 8]))],
    )
    .unwrap();

    let ctx = SessionContext::new();
    let memory_table = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]]).unwrap();
    ctx.register_table("mem_tab", Arc::new(memory_table)).unwrap();

    let df = ctx.sql("select * from mem_tab").await.unwrap();
    let (session_state, logical_plan) = df.into_parts();

    println!("LP\n---\n{}", logical_plan.display_indent_schema());

    let phs_plan = session_state.create_physical_plan(&logical_plan).await.unwrap();

    println!("EP\n---\n{}", DisplayableExecutionPlan::new(phs_plan.as_ref()).indent(true));

    let task_ctx = session_state.task_ctx();
    
    let rb_stream_p0 = phs_plan.execute(0, task_ctx.clone()).unwrap();

    let rb_stream_p1 = phs_plan.execute(1, task_ctx.clone()).unwrap();

    rb_stream_p0.for_each(|b| {
        println!("p0 batch:");
        println!("{}", pretty_format_batches(&[b.unwrap()]).unwrap());
        futures::future::ready(())
    }).await;

    rb_stream_p1.for_each(|b| {
        println!("p1 batch:");
        println!("{}", pretty_format_batches(&[b.unwrap()]).unwrap());
        futures::future::ready(())
    }).await;


    
}
