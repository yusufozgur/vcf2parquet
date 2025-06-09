use std::fs;
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch
};
use clap::builder::Str;
use std::sync::Arc;
use parquet::{
    arrow::ArrowWriter,
};
use std::path::PathBuf;

use std::iter::zip;

pub fn create_parquet(out_path: &PathBuf, header: &String, firstrow: &String) {
    // Create the output file path with .parquet extension
    let parquet_path = out_path.with_extension("parquet");
    
    // Check if file exists and delete it
    if parquet_path.exists() {
        fs::remove_file(&parquet_path).expect("Failed to delete existing parquet file");
    }
    let header: Vec<&str> = header.trim().split("\t").collect();
    let firstrow: Vec<&str> = firstrow.trim().split("\t").collect();

    if header.len() != firstrow.len() {
        panic!("Header and first row col numbers are unequal.")
    }

    let mut file = fs::File::create(&parquet_path).expect("Failed to create parquet file");
    
    //let col = Arc::new(StringArray::from_iter_values(firstrow)) as ArrayRef;

    let to_write: Vec<(&str, Arc<dyn arrow::array::Array>)> = zip(header, firstrow)
        .map(|(colname, val)| {
            (
                colname, 
                Arc::new(StringArray::from_iter_values(vec![val])) as ArrayRef
            )
        })
        .collect();

    println!("{:?}",to_write);

    let to_write = RecordBatch::try_from_iter(
        to_write
    ).unwrap();

    let mut writer = ArrowWriter::try_new(
        &mut file, 
        to_write.schema(), 
        None
    ).unwrap();
    
    writer.write(&to_write).unwrap();

    writer.close().unwrap();

}
