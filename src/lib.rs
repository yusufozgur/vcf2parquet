//! VCF to Parquet converter
//! 
//! This library provides functionality to convert VCF (Variant Call Format) files
//! to Parquet format, preserving metadata in a separate JSON file.
//! 
//! # Features
//! - Fast VCF parsing with gzip support
//! - Efficient conversion to Parquet format
//! - Proper handling of VCF metadata in a separate JSON file
//! - Memory-efficient processing of large files

#![warn(missing_docs)]

/// Command line argument parsing
pub mod args;

/// Error types and handling
pub mod error;

/// VCF metadata processing
pub mod metadata;

/// Parquet conversion functionality
pub mod parquet;

use crate::{
    args::Cli,
    error::VcfError,
    metadata::{parse_metadata, read_metadata, save_metadata},
    parquet::vcf_to_parquet,
};

/// Main conversion function that processes the VCF file and generates Parquet and metadata files
pub fn convert_vcf_to_parquet(args: &Cli) -> Result<(), VcfError> {
    // Validate input file
    if !args.input.exists() {
        return Err(VcfError::InvalidFormat(format!(
            "File '{}' does not exist.",
            args.input.display()
        )));
    }

    // Validate file extension
    let ext = args
        .input
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");

    if ext != "vcf" && !(ext == "gz" && args.input.to_string_lossy().ends_with(".vcf.gz")) {
        return Err(VcfError::InvalidFormat(
            "Input file must have a .vcf or .vcf.gz extension".to_string(),
        ));
    }

    // Process metadata
    let metadata = read_metadata(&args.input)?;
    let metadata_json = parse_metadata(&metadata);
    save_metadata(&metadata_json, &args.output)?;

    // Convert to Parquet
    vcf_to_parquet(&args.input, &args.output)?;

    Ok(())
}
