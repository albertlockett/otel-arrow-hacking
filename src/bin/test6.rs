use arrow::compute::{CastOptions, cast_with_options, kernels::cast};
use arrow_array::UInt32Array;
use arrow_schema::DataType;

fn main() {
    let u32_arr = UInt32Array::from_iter_values(vec![1, 2, u32::MAX]);

    let i32_arr = cast(&u32_arr, &DataType::Int32).unwrap();
    let opts = CastOptions {
        safe: false,
        ..Default::default()
    };
    let i32_arr2 = cast_with_options(&u32_arr, &DataType::Int32, &opts).unwrap();

    // let
    //    let u32_arr = u32_arr.unary_mut(|f| f * 2).unwrap();

    println!("{:?}", u32_arr);
    println!("{:?}", i32_arr);
    println!("{:?}", i32_arr2);
}
