use std::fs;
use arrow::{
    array::{ArrayRef, StringArray},
    record_batch::RecordBatch
};
use std::sync::Arc;
use parquet::{
    arrow::ArrowWriter, file::properties::WriterProperties,
    basic::Compression,
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

    let col_and_vals: Vec<(&str, Arc<dyn arrow::array::Array>)> = zip(header, firstrow)
        .map(|(colname, val)| {
            (
                colname, 
                Arc::new(StringArray::from_iter_values(vec![val])) as ArrayRef
            )
        })
        .collect();

    let to_write = RecordBatch::try_from_iter(
        col_and_vals
    ).unwrap();

    // WriterProperties can be used to set Parquet file options
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(
        &mut file, 
        to_write.schema(), 
        Some(props)
    ).unwrap();
    
    writer.write(&to_write).unwrap();

    writer.close().unwrap();

}
