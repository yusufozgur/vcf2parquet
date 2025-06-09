use flate2::read::MultiGzDecoder;
use std::fs::{File};
use std::io::{BufRead, BufReader};

use std::path::PathBuf;

use crate::parquet_writer::ParquetWriter;

pub fn read_vcf(read_path: &PathBuf, out_path: &PathBuf, limit: Option<i32>) {
    println!("Reading CSV from: {}", read_path.display());

    let file = File::open(read_path).expect("File could not be opened.");
    
    let file_extension =  read_path.extension().expect("extension cannot be get, is it a .vcf or .vcf.gz file?");
    
    // normal gzdecoder does not read after # in bgzip files for some reason, hence multigzdecoder
    let reader: Box<dyn std::io::BufRead> = if file_extension == "gz" {
        Box::new(BufReader::new(MultiGzDecoder::new(file)))
    } else if file_extension == "vcf" {
        Box::new(BufReader::new(file))
    } else {
        panic!("Please provide a file with extension .vcf or .vcf.gz")
    };
    
    let mut metadata: Vec<String> = Vec::new();
    let mut header: Option<String> = None;
    let mut firstrow: Option<String> = None;
    let mut writer: Option<ParquetWriter> = None;

    let mut line_count = 0;
    // Loop over lines
    for line in reader.lines() {

        line_count += 1;
        if line_count % 100 == 0 {
            println!("Processed {} lines", line_count);
        }
        if Option::is_some(&limit) && line_count > limit.unwrap() {
            break
        }

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
            writer = Some(ParquetWriter::new(&out_path, header.clone().unwrap(), firstrow.clone().unwrap()));
        }
        else {
            writer.as_mut().unwrap().write(&line)
        }

    }
    //println!("{:?}", header.expect("No header found."));
}