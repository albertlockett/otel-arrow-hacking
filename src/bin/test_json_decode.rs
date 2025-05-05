use std::io::{BufRead, Cursor};

use arrow_ipc::reader::StreamReader;
use base64::prelude::*;
use otel_arrow_rust::opentelemetry::{ArrowPayload, ArrowPayloadType, BatchArrowRecords};
use prost::Message;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

const OTAP_SAMPLES_PATH: &str = "/Users/albertlockett/Development/otel-arrow/data/otap_metrics.json.zst";

#[derive(Deserialize)]
pub struct BatchArrowRecords2 {
    arrow_payloads: Vec<ArrowPayload2>
}

#[derive(Deserialize)]
pub struct ArrowPayload2 {
    schema_id: String,
    #[serde(rename = "type")]
    t: i32,
    record: String,
}

#[tokio::main]
async fn main() {
    let mut file = File::open(OTAP_SAMPLES_PATH).await.unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).await.unwrap();

    let decoded = zstd::decode_all(&*contents).unwrap();

    for line in decoded.lines() {
        let line = line.unwrap();
        println!("{}", line);
        let bar2: BatchArrowRecords2 = serde_json::from_str(&line).unwrap();

        let bar  = BatchArrowRecords {
            batch_id: 0,
            arrow_payloads: bar2.arrow_payloads.into_iter().map(|ap| {
                ArrowPayload {
                    schema_id: ap.schema_id,
                    r#type: ap.t,
                    record: BASE64_STANDARD.decode(ap.record).unwrap(),
                }
            }).collect(),
            headers: vec![],
        };

        for payload in bar.arrow_payloads {
            let cursor = Cursor::new(payload.record);
            let ipc_stream_Reader = StreamReader::try_new(cursor, None).unwrap();
            for batch in ipc_stream_Reader {
                println!("batch = {:?}", batch)
            }
        }
    }
}