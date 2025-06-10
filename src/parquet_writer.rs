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

#[derive(Debug)]
pub struct ParquetWriter {
    writer: ArrowWriter<std::fs::File>, // Remove BufWriter
    //add the rows to this buffer before writing them in chunks, for performance reasons
    row_buffer: Vec<Vec<String>>,
    header: Vec<String>,
}

impl ParquetWriter {
        pub fn add_row(&mut self, row: &String) {
            let row: Vec<String> = row.trim().split("\t").map(|s| s.to_string()).collect();
            // if row buffer is empty, initialize the values
            if self.row_buffer.is_empty() {
                let row_as_vec_of_vecs: Vec<Vec<String>> = row.iter().map(|x| vec![x.clone()]).collect();
                self.row_buffer = row_as_vec_of_vecs;
            } 
            // if it is not empty, it will have Vec elements inside it, just push the values
            else {
                for (i, val) in row.iter().enumerate() {
                    self.row_buffer[i].push(val.clone());
                }
            }

            self.row_buffer.push(row);
    
            if self.row_buffer.len() >= 1000 {
                self.write();
                self.row_buffer = vec![]
            }
        }
    
        pub fn write(&mut self) {
    
            let col_and_vals
            : Vec<(&str, Arc<dyn arrow::array::Array>)> 
            = zip(self.header.iter(), self.row_buffer.iter())
                .map(|(colname, col_values)| {
                    (
                        colname.as_str(), 
                        Arc::new(StringArray::from_iter_values(col_values.iter().cloned())) as ArrayRef
                    )
                })
                .collect();
    
            let to_write = RecordBatch::try_from_iter(
                col_and_vals
            ).unwrap();        
    
            self.writer.write(&to_write).unwrap();
        }
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

        let mut writer: ArrowWriter<fs::File> = ArrowWriter::try_new(
            file, // Use file directly
            to_write.schema(), 
            Some(props)
        ).unwrap();
        

        writer.write(&to_write).unwrap();


        ParquetWriter { writer, row_buffer: vec![], header }
    }
}

impl Drop for ParquetWriter {
    fn drop(&mut self) {
        // Take ownership of the writer to call close, replacing it with a dummy writer
        let dummy_file = fs::File::create("/dev/null").unwrap();
        let writer = std::mem::replace(
            &mut self.writer, 
            ArrowWriter::try_new(dummy_file, Arc::new(arrow::datatypes::Schema::empty()), None).unwrap()
        );
        writer.close().unwrap();
    }
}
