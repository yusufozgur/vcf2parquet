use flate2::read::MultiGzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader};

use std::path::PathBuf;

pub fn read_vcf(path: &PathBuf) {
    println!("Reading CSV from: {}", path.display());

    // Open the bgzip file
    let file = File::open(path).expect("File could not be opened.");
    
    // Create a MultiGzDecoder to handle bgzip format
    let decoder = MultiGzDecoder::new(file);
    
    // Wrap in a BufReader for efficient line reading
    let reader = BufReader::new(decoder);
    
    // Loop over lines
    for line in reader.lines() {
        match line {
            Ok(line) => println!("{}", line),
            Err(e) => eprintln!("Error during reading file: {}", e)
        }
    }
}