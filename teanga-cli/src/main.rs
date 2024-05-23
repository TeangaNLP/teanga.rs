use clap::Parser;
use teanga::TransactionCorpus;
use std::fs::File;
use flate2;
use std::io::BufReader;

// for CBOR conversion
use ciborium::into_writer;
use serde_json;
use std::collections::HashMap;
use std::io::BufWriter;
use std::io::BufRead;
use teanga::Layer;
use teanga::{write_tcf_corpus, Document, SimpleCorpus};

/// Command line arguments
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[command(subcommand)]
    subcommand: SubCommand,
}

#[derive(Parser, Debug)]
enum SubCommand {
    Load(LoadCommand),
    ToCbor(ToCborCommand),
}

/// Command to load a file into the corpus
#[derive(Parser, Debug)]
#[command(name = "load", about = "Load a file into the corpus")]
struct LoadCommand {
    /// The file to load
    file: String,

    /// The path to the DB
    db: String,

    /// The meta information, as a separate YAML file
    #[arg(long)]
    meta: Option<String>,

    /// Read the file as JSONL (one JSON object per line)
    #[arg(long)]
    jsonl: bool
}

#[derive(Parser, Debug)]
#[command(name = "to-cbor", about = "Convert a Teanga Corpus to CBOR")]
struct ToCborCommand {
    /// The path to the DB
    file: String,

    /// The output file
    output: String,

    /// Read the file as JSONL (one JSON object per line)
    #[arg(long)]
    jsonl: bool
}

impl LoadCommand {
    fn run(&self) -> Result<(), String> {
        let mut corpus = TransactionCorpus::new(&self.db)
            .map_err(|e| format!("Failed to open corpus: {}", e))?;
        if let Some(meta) = &self.meta {
            corpus.read_yaml_header(File::open(meta)
                .map_err(|e| format!("Failed to open meta file: {}", e))?)
                .map_err(|e| format!("Failed to read meta file: {}", e))?;
        }
        let mut file = if self.file.ends_with(".gz") {
            let reader = flate2::read::GzDecoder::new(File::open(&self.file)
                .map_err(|e| format!("Failed to open file: {}", e))?);
            Box::new(reader) as Box<dyn std::io::Read>
        } else {
            Box::new(File::open(&self.file)
                .map_err(|e| format!("Failed to open file: {}", e))?) as Box<dyn std::io::Read>
        };
        if self.jsonl {
            corpus.read_jsonl(&mut BufReader::new(file))
                .map_err(|e| format!("Failed to read file: {}", e))?;
        } else if self.file.ends_with(".json") || self.file.ends_with(".json.gz") {
            corpus.read_json(&mut file)
                .map_err(|e| format!("Failed to read file: {}", e))?;
        } else {
            corpus.read_yaml(&mut file)
                .map_err(|e| format!("Failed to read file: {}", e))?;
        }
        Ok(())
    }
}

impl ToCborCommand {
//    fn run(&self) -> Result<(), String> {
//        let output = BufWriter::new(File::create(&self.output)
//            .map_err(|e| format!("Failed to create output file: {}", e))?);
//        let mut all_data = Vec::new();
//        for line in BufReader::new(flate2::read::GzDecoder::new(File::open(&self.file)
//            .map_err(|e| format!("Failed to open file: {}", e))?))
//            .lines() {
//            let line = line.map_err(|e| format!("Failed to read line: {}", e))?;
//            let data : HashMap<String, Layer> = serde_json::from_str(&line)
//                .map_err(|e| format!("Failed to parse JSON: {}", e))?;
//            all_data.push(data);
//        }
//        into_writer(&all_data, output).
//            map_err(|e| format!("Failed to write CBOR: {}", e))?;
//        Ok(())
//    }
    fn run(&self) -> Result<(), String> {
        let mut output = BufWriter::new(File::create(&self.output)
            .map_err(|e| format!("Failed to create output file: {}", e))?);
        let mut all_data = Vec::new();
        let mut corpus = SimpleCorpus::new();
        corpus.read_yaml_header(File::open("c4-header.yaml")
            .map_err(|e| format!("Failed to open meta file: {}", e))?).unwrap();

        for line in BufReader::new(flate2::read::GzDecoder::new(File::open(&self.file)
            .map_err(|e| format!("Failed to open file: {}", e))?))
            .lines() {
            let line = line.map_err(|e| format!("Failed to read line: {}", e))?;
            let data : HashMap<String, Layer> = serde_json::from_str(&line)
                .map_err(|e| format!("Failed to parse JSON: {}", e))?;
            all_data.push(("".to_string(), Document{ content: data }));
        }
        let mut byte_counts = HashMap::new();
        write_tcf_corpus(output, &corpus.meta,
            all_data.into_iter(), &mut byte_counts).map_err(|e| format!("Failed to write TCF: {}", e))?;
        for (key, value) in byte_counts {
            println!("{}: {}", key, value);
        }
        Ok(())
    }
}

fn main() {
    let args = Args::parse();
    match args.subcommand {
        SubCommand::Load(load) => {
            load.run().unwrap();
        },
        SubCommand::ToCbor(to_cbor) => {
            to_cbor.run().unwrap();
        }
    }
}
