use arrow_schema::{DataType, Field, Schema};

pub fn gen_arrow_schema() -> Schema {
    Schema::new(vec![Field::new("id", DataType::Int32, false)])
}
