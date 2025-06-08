//! Error types for the VCF to Parquet converter.
//!
//! This module defines the error types used throughout the crate.

use std::path::PathBuf;
use thiserror::Error;

/// An error that can occur during VCF to Parquet conversion.
///
/// This enum uses `thiserror` to automatically implement `std::error::Error`
/// and provide detailed error messages.

#[derive(Error, Debug)]
pub enum VcfError {
    /// An error that occurred during I/O operations
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    /// An error that occurred in the Polars library
    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),
    
    /// The input file has an invalid format
    #[error("Invalid file format: {0}")]
    InvalidFormat(String),
    
    /// Failed to read metadata from a VCF file
    #[error("Failed to read VCF metadata from {path}: {source}")]
    MetadataReadError {
        /// The path to the file that caused the error
        path: PathBuf,
        /// The underlying I/O error
        source: std::io::Error,
    },
    
    /// Failed to save metadata to a file
    #[error("Failed to save metadata to {path}: {source}")]
    MetadataSaveError {
        /// The path to the file that caused the error
        path: PathBuf,
        /// The underlying I/O error
        source: std::io::Error,
    },
    
    /// Failed to convert VCF to Parquet format
    #[error("Failed to convert VCF to Parquet: {0}")]
    ConversionError(String), // Detailed error message
}
