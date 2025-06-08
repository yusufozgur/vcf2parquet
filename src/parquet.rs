use crate::error::VcfError;
use flate2::bufread::MultiGzDecoder;
use polars::prelude::*;
use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::Path,
};

/// Converts a VCF file to Parquet format
pub fn vcf_to_parquet<P: AsRef<Path>>(
    input_path: P, 
    output_path: &Path
) -> Result<(), VcfError> {
    let input_path = input_path.as_ref();
    let parquet_path = output_path.with_extension("parquet");
    
    // Open the input file with gzip support if needed
    let file = File::open(input_path)
        .map_err(|e| VcfError::Io(e))?;
        
    let reader: Box<dyn Read> = if input_path.to_string_lossy().ends_with(".gz") {
        Box::new(MultiGzDecoder::new(BufReader::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };
    
    // Skip metadata lines
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)
            .map_err(|e| VcfError::Io(e))?;
            
        if bytes_read == 0 {
            return Err(VcfError::InvalidFormat("Empty file or no header found".to_string()));
        }
        
        // The first non-metadata line is the header
        if !line.starts_with("##") {
            break;
        }
    }
    
    // The current line contains the header
    let header = line.trim_end();
    
    // Read the rest of the file
    let mut contents = String::new();
    contents.push_str(header);
    contents.push('\n');
    
    reader.read_to_string(&mut contents)
        .map_err(|e| VcfError::Io(e))?;
    
    // Parse the VCF data using Polars
    let cursor = std::io::Cursor::new(contents);
    let df = CsvReader::new(cursor)
        .infer_schema(None)
        .with_separator(b'\t')
        .has_header(true)
        .finish()
        .map_err(VcfError::Polars)?;
    
    // Write to Parquet
    let mut file = File::create(&parquet_path)
        .map_err(|e| VcfError::Io(e))?;
        
    ParquetWriter::new(&mut file)
        .finish(&mut df.clone())
        .map_err(VcfError::Polars)?;
    
    println!("Data saved to {}", parquet_path.display());
    Ok(())
}
