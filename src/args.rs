//! Command line argument parsing for the VCF to Parquet converter.

use clap::Parser;
use std::path::PathBuf;

/// Command line arguments for the VCF to Parquet converter.
/// 
/// This struct uses `clap` to parse command line arguments and provides
/// a type-safe interface for accessing them.

#[derive(Parser, Debug)]
#[command(version, about = "Convert VCF files to Parquet format")]
pub struct Cli {
    /// Path to the input VCF file (.vcf or .vcf.gz)
    pub input: PathBuf,

    /// Path for the output files (will create .parquet and .metadata.json)
    pub output: PathBuf,

    /// Number of rows for each chunk that is being streamed
    #[arg(short, long, default_value_t = 500)]
    pub chunk_size: usize,
}
