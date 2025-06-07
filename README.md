# Why Should you convert VCF Files To Parquet

VCF is a ubiquitous file format in genomics, and in its essence it is a formatted tsv file that is mostly also gzipped. Apparent from its formatting (https://samtools.github.io/hts-specs/VCFv4.2.pdf), this format would benefit from columnar storage of its data, which parquet files does. If you want to learn more about performance benefits of parquet format over VCF, you can take a look at https://medium.com/23andme-engineering/genetic-datastore-4b213256db31.

# How Vcf2Parquet Processes VCFs

VCF files have metadata rows starting with "##" at the beginning. These are converted into a json file for usage. Rest of the file is in tsv format, and this tool directly converts it to parquet without any processing. If you want to convert the allele encodings such as 0/1 to allele letters such as A/AAG per sample, please 
use https://gatk.broadinstitute.org/hc/en-us/articles/360036896892-VariantsToTable.

# How to use

install uv: https://github.com/astral-sh/uv

To use, invoke the command like this:
```
uvx git+https://github.com/yusufozgur/vcf2parquet input.vcf output_prefix
```

# How to develop with it

```
git clone https://github.com/yusufozgur/vcf2parquet
uv sync
uv run python vcf2parquet.py example.vcf.gz example_output
```

# Sources

-   sample vcf: https://github.com/vcflib/vcflib/blob/master/samples/sample.vcf