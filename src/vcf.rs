use flate2::read::MultiGzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader};

use std::path::PathBuf;

pub fn read_vcf(path: &PathBuf) {
    println!("Reading CSV from: {}", path.display());

    let file = File::open(path).expect("File could not be opened.");
    
    // normal gzdecoder does not read after # in bgzip files for some reason, hence multigzdecoder
    let reader: Box<dyn std::io::BufRead> = if path.ends_with(".vcf.gz") {
        Box::new(BufReader::new(MultiGzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };
    
    let mut metadata: Vec<String> = Vec::new();
    let mut header: Option<String> = None;
    // Loop over lines
    for line in reader.lines() {

        let line = line.expect("Line could not be read.");
        if line.starts_with("##") {
            metadata.push(line);
        }
        else if line.starts_with("#") {
            header = Some(line);
        }
        else {
            println!("{:?}", line);
        }

    }
    println!("{:?}", header.expect("No header found."));
}