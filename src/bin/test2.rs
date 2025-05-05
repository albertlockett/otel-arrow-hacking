use arrow_ipc::reader::{FileReader, StreamReader};
use std::io::Cursor;
// use otel_arrow_rust::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;
use otel_arrow_rust::opentelemetry::{ArrowPayloadType, BatchArrowRecords};
use prost::Message;
use tokio::{fs::File, io::AsyncReadExt};

///
/// To generate batch:
/// go run ./tools/trace_gen/main.go

#[tokio::main]
async fn main() {
    let mut file = File::open("/Users/albertlockett/Development/otel-arrow/data/otlp_traces.json")
        .await
        .unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).await.unwrap();

    // let req = ExportTraceServiceRequest::decode(contents.as_ref()).unwrap();
    let req = BatchArrowRecords::decode(contents.as_ref()).unwrap();
    println!("{:?}", req);

    for payload in req.arrow_payloads {
        let cursor = Cursor::new(payload.record);
        let reader = StreamReader::try_new(cursor, None).unwrap();

        println!("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        println!("~~~~ schema id = {}", payload.schema_id);
        println!(
            "~~~~ type = {:?}",
            ArrowPayloadType::try_from(payload.r#type).unwrap()
        );
        println!("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        for batch in reader {
            println!("{:?}", batch);
        }
    }
}
