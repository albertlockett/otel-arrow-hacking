use arrow_ipc::reader::StreamReader;
use std::io::Cursor;
use otel_arrow_rust::opentelemetry::{ArrowPayloadType, BatchArrowRecords};
use prost::Message;
use tokio::{fs::File, io::AsyncReadExt};

#[tokio::main]
async fn main() {
    let mut file = File::open("/Users/albertlockett/Development/otel-arrow/otap_logs.pb")
        .await
        .unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).await.unwrap();

    let req = BatchArrowRecords::decode(contents.as_ref()).unwrap();

    for payload in req.arrow_payloads {
        let payload_type = ArrowPayloadType::try_from(payload.r#type).unwrap();
        println!("reading type {:?}", payload_type);
        let cursor = Cursor::new(payload.record);
        let reader = StreamReader::try_new(cursor, None).unwrap();

        for batch in reader {
            println!("read = {:?}", batch);
        }
    }
}
