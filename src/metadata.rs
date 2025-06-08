use crate::error::VcfError;
use flate2::bufread::MultiGzDecoder;
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

/// Reads metadata lines from a VCF file (supports both plain and gzipped files)
pub fn read_metadata<P: AsRef<Path>>(path: P) -> Result<Vec<String>, VcfError> {
    let file = File::open(&path).map_err(|e| VcfError::MetadataReadError {
        path: path.as_ref().to_path_buf(),
        source: e,
    })?;

    let reader: Box<dyn BufRead> = if path.as_ref().to_string_lossy().ends_with(".gz") {
        Box::new(BufReader::new(MultiGzDecoder::new(BufReader::new(file))))
    } else {
        Box::new(BufReader::new(file))
    };

    let mut metadata = Vec::new();
    for line in reader.lines() {
        let line = line.map_err(|e| VcfError::MetadataReadError {
            path: path.as_ref().to_path_buf(),
            source: e,
        })?;
        
        if line.starts_with("##") {
            metadata.push(line);
        } else {
            break;
        }
    }
    
    Ok(metadata)
}

/// Parses VCF metadata into a JSON structure
pub fn parse_metadata(metadata: &[String]) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    
    for line in metadata {
        let line = line.trim_start_matches("##");
        if let Some((key, value)) = line.split_once('=') {
            let entry = result.entry(key.to_string())
                .or_insert_with(|| Value::Array(Vec::new()));
                
            if let Value::Array(arr) = entry {
                arr.push(Value::String(value.to_string()));
            }
        }
    }
    
    Value::Object(result)
}

/// Saves metadata to a JSON file
pub fn save_metadata(metadata: &serde_json::Value, output_path: &Path) -> Result<(), VcfError> {
    let json_path = output_path.with_extension("metadata.json");
    let file = File::create(&json_path)
        .map_err(|e| VcfError::MetadataSaveError {
            path: json_path.clone(),
            source: e,
        })?;
    
    serde_json::to_writer_pretty(file, metadata)
        .map_err(|e| VcfError::ConversionError(e.to_string()))?;
    
    println!("Metadata saved to {}", json_path.display());
    Ok(())
}
