use anyhow::{Context, Result};
use clap::Parser;
use flate2::bufread::MultiGzDecoder;
use polars::prelude::*;
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};

#[derive(thiserror::Error, Debug)]
pub enum VcfError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),
    
    #[error("Invalid file format: {0}")]
    InvalidFormat(String),
}

#[derive(Parser, Debug)]
#[command(version, about = "Convert VCF files to Parquet format")]
struct Cli {
    /// Path to the input VCF file (.vcf or .vcf.gz)
    input: PathBuf,

    /// Path for the output files (will create .parquet and .metadata.json)
    output: PathBuf,

    /// Number of rows for each chunk that is being streamed
    #[arg(short, long, default_value_t = 500)]
    chunk_size: usize,
}

fn read_metadata<P: AsRef<Path>>(path: P) -> Result<Vec<String>, VcfError> {
    let file = File::open(&path).map_err(VcfError::Io)?;
    let reader: Box<dyn BufRead> = if path.as_ref().to_string_lossy().ends_with(".gz") {
        Box::new(BufReader::new(MultiGzDecoder::new(BufReader::new(file))))
    } else {
        Box::new(BufReader::new(file))
    };

    let mut metadata = Vec::new();
    for line in reader.lines() {
        let line = line.map_err(VcfError::Io)?;
        if line.starts_with("##") {
            metadata.push(line);
        } else {
            break;
        }
    }
    Ok(metadata)
}

fn parse_metadata(metadata: &[String]) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    
    for line in metadata {
        let line = line.trim_start_matches("##");
        if let Some((key, value)) = line.split_once('=') {
            let entry = result.entry(key.to_string()).or_insert_with(|| Value::Array(Vec::new()));
            if let Value::Array(arr) = entry {
                arr.push(Value::String(value.to_string()));
            }
        }
    }
    
    Value::Object(result)
}

fn save_metadata(metadata: &serde_json::Value, output_path: &Path) -> Result<()> {
    let json_path = output_path.with_extension("metadata.json");
    let file = File::create(&json_path)
        .with_context(|| format!("Failed to create metadata file: {}", json_path.display()))?;
    
    serde_json::to_writer_pretty(file, metadata)
        .with_context(|| "Failed to write metadata to JSON")?;
    
    println!("Metadata saved to {}", json_path.display());
    Ok(())
}

fn save_data_to_parquet<P: AsRef<Path>>(input_path: P, output_path: &Path) -> Result<(), VcfError> {
    let input_path = input_path.as_ref();
    let parquet_path = output_path.with_extension("parquet");
    
    // First, read the VCF file and write it to a temporary CSV file
    // since Polars doesn't have direct VCF support
    let file = File::open(input_path).map_err(VcfError::Io)?;
    let reader: Box<dyn Read> = if input_path.to_string_lossy().ends_with(".gz") {
        Box::new(MultiGzDecoder::new(BufReader::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };
    
    // Create a buffered reader and skip the metadata lines
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        reader.read_line(&mut line).map_err(VcfError::Io)?;
        if !line.starts_with("##") {
            break;
        }
    }
    
    // The first non-metadata line is the header
    let header = line.trim_end();
    
    // Read the rest of the file
    let mut contents = String::new();
    contents.push_str(header);
    contents.push('\n');
    reader.read_to_string(&mut contents).map_err(VcfError::Io)?;
    
    // Convert to CSV and read with Polars
    let cursor = std::io::Cursor::new(contents);
    let df = CsvReader::new(cursor)
        .infer_schema(None)
        .with_separator(b'\t')
        .has_header(true)
        .finish()
        .map_err(VcfError::Polars)?;
    
    // Write to Parquet
    let mut file = File::create(&parquet_path).map_err(VcfError::Io)?;
    ParquetWriter::new(&mut file).finish(&mut df.clone()).map_err(VcfError::Polars)?;
    
    println!("Data saved to {}", parquet_path.display());
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    
    // Validate input file
    if !args.input.exists() {
        return Err(format!("File '{}' does not exist.", args.input.display()).into());
    }
    
    let ext = args.input.extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    
    if ext != "vcf" && !(ext == "gz" && args.input.to_string_lossy().ends_with(".vcf.gz")) {
        return Err("Input file must have a .vcf or .vcf.gz extension.".into());
    }
    
    // Process metadata
    let metadata = read_metadata(&args.input)
        .map_err(|e| format!("Failed to read VCF metadata: {}", e))?;
    let metadata_json = parse_metadata(&metadata);
    save_metadata(&metadata_json, &args.output)
        .map_err(|e| format!("Failed to save metadata: {}", e))?;
    
    // Process data
    save_data_to_parquet(&args.input, &args.output)
        .map_err(|e| format!("Failed to convert to Parquet: {}", e))?;
    
    Ok(())
}
