use clap::Parser;
use std::path::PathBuf;
use std::time::Instant;

mod vcf;
use vcf::read_vcf;

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
    let start_time = Instant::now();

    let args = Args::parse();

    println!("Input VCF: {}", args.input_vcf.display());
    println!("Output prefix: {}", args.output_prefix.display());

    read_vcf(&args.input_vcf);

    let execution_duration = start_time.elapsed();
    println!("Main function took: {:?}", execution_duration);
}