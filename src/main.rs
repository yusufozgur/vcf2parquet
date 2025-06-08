use clap::Parser;
use vcf2parquet::{args::Cli, convert_vcf_to_parquet};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    
    if let Err(e) = convert_vcf_to_parquet(&args) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    
    Ok(())
}
