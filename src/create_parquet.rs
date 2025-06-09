use std::fs;
use arrow::array::{ArrayRef, Int32Array};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::path::PathBuf;

pub fn create_parquet(out_path: &PathBuf, header: &Option<String>, firstrow: &Option<String>) {
    let ids = Int32Array::from(vec![1, 2, 3, 4]);
    let vals = Int32Array::from(vec![5, 6, 7, 8]);
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as ArrayRef),
        ("val", Arc::new(vals) as ArrayRef),
    ]).unwrap();
    
    // Create the output file path with .parquet extension
    let parquet_path = out_path.with_extension("parquet");
    
    // Check if file exists and delete it
    if parquet_path.exists() {
        fs::remove_file(&parquet_path).expect("Failed to delete existing parquet file");
    }
    
    // Create new file
    let file = fs::File::create(&parquet_path).expect("Failed to create parquet file");
    
    // WriterProperties can be used to set Parquet file options
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).expect("Writing batch");
    // writer must be closed to write footer
    writer.close().unwrap();
}
