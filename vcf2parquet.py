import json
import re
import typer
from pathlib import Path
import polars as pl
import polars_streaming_csv_decompression
import gzip
from typing_extensions import Annotated

def get_metadata(vcf_path: Path) -> list[str]:
    metadata = []

    if vcf_path.name.endswith(".gz"):
        with gzip.open(vcf_path, "rt") as f:
            for line in f:
                if line.startswith("##"):
                    metadata.append(line)
                else:
                    break
    else:
        with vcf_path.open("rt") as f:
            for line in f:
                if line.startswith("##"):
                    metadata.append(line)
                else:
                    break
    return metadata


def tags_to_dict(val: str):
    if val.startswith("<") and val.endswith(">"):
        val = val.removeprefix("<").removesuffix(">")
        return dict([x.split("=") for x in val.split(",")])
    else:
        #else, dont do anything
        return val
    
def save_metadata_to_json(vcf_path: Path, output_path: Path):
    metadata = get_metadata(vcf_path)

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

    json_path = output_path.with_suffix('.metadata.json')
    with open(json_path, "w") as f:
        json.dump(metadata_dict, f, indent=2)
    print(f"Metadata saved to {json_path}")

def save_data_to_parquet(vcf_path: Path, output_path: Path, chunk_size: int):
    #pl.Config.set_streaming_chunk_size(chunk_size)
    parquet_path = output_path.with_suffix('.parquet')
    lf = polars_streaming_csv_decompression.streaming_csv(
        vcf_path,
        comment_prefix="##",
        separator="\t",
        low_memory=True,
    )
    lf.sink_parquet(parquet_path, engine="streaming")
    print(f"Data saved to {parquet_path}")

def app(
        vcf_path: Path, 
        output_path: Path, 
        chunk_size: Annotated[int, typer.Option(help="Number of rows for each chunk that is being streamed. Increase this if you have more ram. Decrease it if you have less ram.")] = 500,
        ):
    if not vcf_path.exists():
        print(f"File '{vcf_path}' does not exist.")
        return
    
    if not (vcf_path.name.endswith(".vcf") or vcf_path.name.endswith(".vcf.gz")):
        print("Input file must have a .vcf or .vcf.gz extension.")
        return

    save_data_to_parquet(vcf_path, output_path, chunk_size)
    save_metadata_to_json(vcf_path, output_path)

def main():
    typer.run(app)

if __name__ == "__main__":
    main()
