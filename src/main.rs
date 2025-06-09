use clap::Parser;
use std::path::PathBuf;

/// VCF to Parquet converter
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the input VCF file
    #[arg(short = 'i', long)]
    input_vcf: PathBuf,

    /// Output prefix for generated files
    #[arg(short = 'o', long)]
    output_prefix: PathBuf,
}

fn main() {
    let args = Args::parse();

    println!("Input VCF: {:?}", args.input_vcf);
    println!("Output prefix: {:?}", args.output_prefix);
}