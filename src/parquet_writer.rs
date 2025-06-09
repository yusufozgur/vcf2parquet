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
use std::io::BufWriter;

use std::iter::zip;

#[derive(Debug)]
pub struct ParquetWriter {
    writer: ArrowWriter<BufWriter<std::fs::File>>,
    header: Vec<String>,
}

impl ParquetWriter {
    pub fn new(out_path: &PathBuf, header: String, firstrow: String) -> Self {
        // Create the output file path with .parquet extension
        let parquet_path: PathBuf = out_path.with_extension("parquet");
        
        // Check if file exists and delete it
        if parquet_path.exists() {
            fs::remove_file(&parquet_path).expect("Failed to delete existing parquet file");
        }

        let header: Vec<String> = header.trim().split("\t").map(|s| s.to_string()).collect();
        let firstrow: Vec<&str> = firstrow.trim().split("\t").collect();

        if header.len() != firstrow.len() {
            panic!("Header and first row col numbers are unequal.")
        }

        let file = fs::File::create(&parquet_path).expect("Failed to create parquet file");
        let buf_writer = BufWriter::with_capacity(16 * 1024 * 1024, file);

        // WriterProperties can be used to set Parquet file options
        let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();


        let col_and_vals: Vec<(&str, Arc<dyn arrow::array::Array>)> = zip(header.iter(), firstrow)
            .map(|(colname, val)| {
                (
                    colname.as_str(), 
                    Arc::new(StringArray::from_iter_values(vec![val])) as ArrayRef
                )
            })
            .collect();

        let to_write = RecordBatch::try_from_iter(
            col_and_vals
        ).unwrap();

        let mut writer: ArrowWriter<BufWriter<fs::File>> = ArrowWriter::try_new(
            buf_writer, 
            to_write.schema(), 
            Some(props)
        ).unwrap();
        

        writer.write(&to_write).unwrap();


        ParquetWriter { writer, header }
    }

    pub fn write(&mut self, row: &String) {
        let row: Vec<&str> = row.trim().split("\t").collect();

        let col_and_vals
        : Vec<(&str, Arc<dyn arrow::array::Array>)> 
        = zip(self.header.iter(), row)
            .map(|(colname, val)| {
                (
                    colname.as_str(), 
                    Arc::new(StringArray::from_iter_values(vec![val])) as ArrayRef
                )
            })
            .collect();

        let to_write = RecordBatch::try_from_iter(
            col_and_vals
        ).unwrap();        

        self.writer.write(&to_write).unwrap();
    }
}

impl Drop for ParquetWriter {
    fn drop(&mut self) {
        // Take ownership of the writer to call close, replacing it with a dummy writer
        let dummy_file = fs::File::create("/dev/null").unwrap();
        let dummy_buf_writer = BufWriter::new(dummy_file);
        let writer = std::mem::replace(
            &mut self.writer, 
            ArrowWriter::try_new(dummy_buf_writer, Arc::new(arrow::datatypes::Schema::empty()), None).unwrap()
        );
        writer.close().unwrap();
    }
}
