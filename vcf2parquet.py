import json
import typer
from pathlib import Path
import pandas as pd
import gzip
import io
import fastparquet


def tags_to_dict(val: str):
    if val.startswith("<") and val.endswith(">"):
        val = val.removeprefix("<").removesuffix(">")
        return dict([x.split("=") for x in val.split(",")])
    else:
        #else, dont do anything
        return val
    
def save_metadata(metadata: list[str], json_path: Path):
    metadata_trim = [x.strip() for x in metadata]
    metadata_remove_doublehash = [x.removeprefix("##") for x in metadata_trim]
    metadata_keys_and_vals = ([x.split("=", 1) for x in metadata_remove_doublehash])
    # there can be multiple fields for INFO, FORMAT FILTER etc, we should accumulate them
    metadata_keys = list(set(
        [x[0] for x in metadata_keys_and_vals]
    ))
    metadata_dict = dict([(x,[]) for x in metadata_keys])
    for (k,v) in metadata_keys_and_vals:
        metadata_dict[k].append(v)
    pass

    with open(json_path, "w") as f:
        json.dump(metadata_dict, f, indent=2)
    print(f"Metadata saved to {json_path}")

def get_file_reader(vcf_path: Path) -> io.TextIOWrapper:
    if vcf_path.name.endswith(".gz"):
        return gzip.open(vcf_path, "rt")
    else:
        return vcf_path.open("rt")



def read_file(vcf_path: Path, output_path: Path):
    metadata: list[str] = []
    headers: list[str] = []
    shouldappend = False # this ensures first file creation then only appending
    parquet_path = output_path.with_suffix('.parquet')
    json_path = output_path.with_suffix('.metadata.json')

    if vcf_path.name.endswith(".gz"):
        with get_file_reader(vcf_path) as f:
            for i, line in enumerate(f):
                if line.startswith("##"):
                    metadata.append(line)
                elif line.startswith("#"):
                    headers = line.strip().split("\t")
                else:
                    list_of_values = line.strip().split("\t")
                    fastparquet.write(
                        parquet_path,
                        pd.DataFrame([list_of_values], columns=headers), 
                        append=shouldappend
                        )
                    shouldappend = True
                    if i % 10 == 0:
                        print(f"Processing line {i}", end="\r")


    print(f"Data saved to {parquet_path}")

    save_metadata(metadata, json_path)


def app(
        vcf_path: Path, 
        output_path: Path,
        ):
    if not vcf_path.exists():
        print(f"File '{vcf_path}' does not exist.")
        return
    
    if not (vcf_path.name.endswith(".vcf") or vcf_path.name.endswith(".vcf.gz")):
        print("Input file must have a .vcf or .vcf.gz extension.")
        return
    
    read_file(vcf_path, output_path)

def main():
    typer.run(app)

if __name__ == "__main__":
    main()
