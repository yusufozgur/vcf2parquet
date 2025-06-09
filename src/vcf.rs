use flate2::read::MultiGzDecoder;
use std::fs::{File};
use std::io::{BufRead, BufReader};

use std::path::PathBuf;

use crate::create_parquet::create_parquet;


pub fn read_vcf(read_path: &PathBuf, out_path: &PathBuf) {
    println!("Reading CSV from: {}", read_path.display());

    let file = File::open(read_path).expect("File could not be opened.");
    
    // normal gzdecoder does not read after # in bgzip files for some reason, hence multigzdecoder
    let reader: Box<dyn std::io::BufRead> = if read_path.ends_with(".vcf.gz") {
        Box::new(BufReader::new(MultiGzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };
    
    let mut metadata: Vec<String> = Vec::new();
    let mut header: Option<String> = None;
    let mut firstrow: Option<String> = None;
    // Loop over lines
    for line in reader.lines() {

        let line = line.expect("Line could not be read.");
        if line.starts_with("##") {
            metadata.push(line);
        }
        else if line.starts_with("#") {
            header = Some(line);
        }
        else if firstrow == None {
            firstrow = Some(line);
            if header == None {
                panic!("a header row must exist before data rows")
            }
            create_parquet(&out_path, &header, &firstrow);
        }
        else {
            println!("{:?}", line);
        }

    }
    println!("{:?}", header.expect("No header found."));
}