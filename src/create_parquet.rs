use std::fs;
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch
};
use std::sync::Arc;
use parquet::{
    basic::{Compression, ConvertedType, Repetition, Type as PhysicalType},
    schema::{parser, printer, types::Type},
    arrow::ArrowWriter,
    file::properties::WriterProperties
};
use std::path::PathBuf;

fn create_schema (header: &Vec<&str>, firstrow: &Vec<&str> ) -> Vec<Arc<Type>> {
    header.iter().map(|x| {
        match x {
            _ => Arc::new(
                Type::primitive_type_builder("a", PhysicalType::BYTE_ARRAY)
                .with_converted_type(ConvertedType::UTF8)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap()
            )
        }
    }).collect()
}

pub fn create_parquet(out_path: &PathBuf, header: String, firstrow: String) {
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
    let to_write = RecordBatch::try_from_iter(
        [
            (
                "col",
                Arc::new(StringArray::from_iter_values(vec!["zart"])) as ArrayRef
            )
            ]
    ).unwrap();

    let mut writer = ArrowWriter::try_new(
        &mut file, 
        to_write.schema(), 
        None
    ).unwrap();
    
    writer.write(&to_write).unwrap();

    writer.close().unwrap();

}
