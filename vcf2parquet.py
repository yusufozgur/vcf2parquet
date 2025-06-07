import json
import re
import typer
from pathlib import Path
import polars as pl

def get_metadata(vcf_path: Path) -> str:
    metadata = []
    with vcf_path.open("rt") as f:
        for line in f:
            if line.startswith("##"):
                metadata.append(line[2:])  # Remove the starting "##"
            else:
                break
    return "".join(metadata)

def parse_metadata_to_dict(metadata: str) -> dict:
    """
    Parse VCF metadata lines into a dictionary.
    """
    meta_dict = {}
    pattern = re.compile(r'(\w+)=<(.+)>')
    for line in metadata.strip().splitlines():
        match = pattern.match(line)
        if match:
            key, content = match.groups()
            # Split content into key-value pairs
            fields = {}
            for item in re.findall(r'(\w+)=("[^"]*"|[^,]+)', content):
                k, v = item
                fields[k] = v.strip('"')
            if key not in meta_dict:
                meta_dict[key] = []
            meta_dict[key].append(fields)
    return meta_dict

def save_metadata_to_json(vcf_path: Path):
    metadata = get_metadata(vcf_path)
    meta_dict = parse_metadata_to_dict(metadata)
    json_path = vcf_path.with_suffix(vcf_path.suffix + '.metadata.json')
    with open(json_path, "w") as f:
        json.dump(meta_dict, f, indent=2)
    print(f"Metadata saved to {json_path}")

def save_data_to_parquet(vcf_path: Path):
    df = pl.scan_csv(vcf_path, comment_prefix="##", separator="\t").collect()
    parquet_path = vcf_path.with_suffix(vcf_path.suffix + '.parquet')
    df.write_parquet(parquet_path)
    print(f"Data saved to {parquet_path}")

def main(vcf_path: Path):
    if not vcf_path.exists():
        print(f"File '{vcf_path}' does not exist.")
        return
    
    if not (vcf_path.name.endswith(".vcf") or vcf_path.name.endswith(".vcf.gz")):
        print("Input file must have a .vcf or .vcf.gz extension.")
        return

    save_data_to_parquet(vcf_path)
    save_metadata_to_json(vcf_path)

if __name__ == "__main__":
    typer.run(main)