# VCF to Parquet Converter

A high-performance Rust implementation for converting VCF (Variant Call Format) files to Parquet format.

## Why Convert VCF Files to Parquet?

VCF is a ubiquitous file format in genomics, essentially a formatted TSV file that's often gzipped. The columnar nature of VCF data makes it an excellent candidate for Parquet's columnar storage format, which can significantly improve query performance and reduce storage requirements.

Key benefits:
- **Faster queries**: Columnar storage allows reading only the needed columns
- **Better compression**: Parquet's efficient compression can reduce file sizes
- **Schema evolution**: Easily handle evolving data schemas
- **Interoperability**: Parquet is widely supported across data processing tools

Learn more about the performance benefits: [23andMe Genetic Datastore](https://medium.com/23andme-engineering/genetic-datastore-4b213256db31)

## Rust Implementation

This is a high-performance Rust implementation that provides:
- Fast VCF parsing with gzip support
- Efficient conversion to Parquet format
- Proper handling of VCF metadata in a separate JSON file
- Memory-efficient processing of large files

### Installation

1. Install Rust from [rustup.rs](https://rustup.rs/)
2. Clone this repository:
   ```bash
   git clone https://github.com/yusufozgur/vcf2parquet
   cd vcf2parquet
   ```
3. Build the project:
   ```bash
   cargo build --release
   ```

### Usage

```bash
# Basic usage
./target/release/vcf2parquet input.vcf output_prefix

# For gzipped VCF files
./target/release/vcf2parquet input.vcf.gz output_prefix

# Control chunk size (default: 500)
./target/release/vcf2parquet --chunk-size 1000 input.vcf output_prefix
```

This will create two files:
- `output_prefix.parquet`: The main data in Parquet format
- `output_prefix.metadata.json`: VCF metadata in JSON format

## How It Works

1. **Metadata Extraction**: The tool reads all metadata lines (starting with `##`) and saves them to a JSON file.
2. **Data Conversion**: The remaining TSV data is converted to Parquet format, preserving the structure.
3. **Efficient Processing**: The tool processes the file in chunks to handle large files with minimal memory usage.

## Python Version

For the original Python implementation, see the `vcf2parquet.py` file. The Python version can be used as follows:

```bash
uvx git+https://github.com/yusufozgur/vcf2parquet input.vcf output_prefix
```

## Development

### Building

```bash
cargo build
```

### Testing

```bash
cargo test
```

### Running Benchmarks

```bash
cargo bench
```

## License

MIT OR Apache-2.0

## Sources

- [VCF v4.2 Specification](https://samtools.github.io/hts-specs/VCFv4.2.pdf)
- [Sample VCF](https://github.com/vcflib/vcflib/blob/master/samples/sample.vcf)
- [GATK VariantsToTable](https://gatk.broadinstitute.org/hc/en-us/articles/360036896892-VariantsToTable)