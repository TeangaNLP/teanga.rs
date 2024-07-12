use clap::{Parser, ValueEnum};
use teanga::TransactionCorpus;
use std::fs::File;
use flate2;
use std::io::{BufReader, BufRead};
use teanga::Corpus;
use teanga::TCFConfig;
use teanga::Document;
use teanga::TeangaError;

// for CBOR conversion
use std::io::BufWriter;

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
    Convert(ConvertCommand),
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

#[derive(ValueEnum, Debug, Clone, PartialEq, Eq)]
#[clap(rename_all = "lowercase")]
enum Format {
    JSON,
    JSONL,
    YAML,
    TCF,
    Guess
}

impl Format {
    fn guess(&self, file : &str) -> Format {
        match self {
            Format::Guess => {
                if file.ends_with(".json") || file.ends_with(".json.gz") {
                    Format::JSON
                } else if file.ends_with(".jsonl") {
                    Format::JSONL
                } else if file.ends_with(".yaml") || file.ends_with(".yml") || file.ends_with(".yaml.gz") {
                    Format::YAML
                } else if file.ends_with(".tcf") || file.ends_with(".tcf.gz") {
                    Format::TCF
                } else {
                    Format::YAML
                }
            }
            _ => self.clone()
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "convert", about = "Convert a Teanga Corpus")]
struct ConvertCommand {
    /// The file to convert
    input: String,

    /// The output file
    output: String,

    /// The format of the input file
    #[arg(short,long)]
    #[clap(default_value="guess")]
    input_format: Format,

    /// The format of the output file
    #[arg(short,long)]
    #[clap(default_value="guess")]
    output_format: Format,

    /// The meta information, as a separate YAML file (required for JSONL)
    #[arg(short,long)]
    meta_file: Option<String>
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

impl ConvertCommand {
    fn run(&self) -> Result<(), String> {
        let mut input = if self.input.ends_with(".gz") {
            let reader = BufReader::new(flate2::read::GzDecoder::new(File::open(&self.input)
                .map_err(|e| format!("Failed to open input file: {}", e))?));
            Box::new(reader) as Box<dyn std::io::BufRead>
        } else {
            Box::new(BufReader::new(File::open(&self.input)
                .map_err(|e| format!("Failed to open input file: {}", e))?)) as Box<dyn std::io::BufRead>
        };
        let mut output = BufWriter::new(File::create(&self.output)
            .map_err(|e| format!("Failed to create output file: {}", e))?);
        let mut corpus = teanga::SimpleCorpus::new();
        match self.meta_file {
            Some(ref meta_file) => {
                    corpus.read_yaml_header(File::open(meta_file)
                        .map_err(|e| format!("Failed to open meta file: {}", e))?).unwrap();
                        }
            None => {}
        }

        let mut progressive = false;

        match self.input_format.guess(&self.input) {
            Format::JSON => {
                teanga::serialization::read_json(&mut input, &mut corpus)
                    .map_err(|e| format!("Failed to read JSON: {}", e))?;
            }
            Format::JSONL => {
                if self.meta_file.is_none() {
                    return Err("Meta file is required for JSONL".to_string());
                }
                if self.output_format.guess(&self.output) == Format::TCF {
                    progressive = true;
                } else {
                    teanga::serialization::read_jsonl(&mut input, &mut corpus)
                        .map_err(|e| format!("Failed to read JSONL: {}", e))?;
                }
            }
            Format::YAML => {
                teanga::serialization::read_yaml(&mut input, &mut corpus)
                    .map_err(|e| format!("Failed to read YAML: {}", e))?;
            }
            Format::TCF => {
                teanga::read_tcf(&mut input, &mut corpus)
                    .map_err(|e| format!("Failed to read TCF: {}", e))?;
            }
            Format::Guess => panic!("unreachable")
        }
        match self.output_format.guess(&self.output) {
            Format::JSON => {
                teanga::serialization::write_json(&mut output, &corpus)
                    .map_err(|e| format!("Failed to write JSON: {}", e))?;
            }
            Format::JSONL => {
                teanga::serialization::write_jsonl(&mut output, &corpus)
                    .map_err(|e| format!("Failed to write JSONL: {}", e))?;
            }
            Format::YAML => {
                teanga::serialization::write_yaml(&mut output, &corpus)
                    .map_err(|e| format!("Failed to write YAML: {}", e))?;
            }
            Format::TCF => {
                let config = TCFConfig::new();
                if progressive {
                    let (mut cache, keys) = teanga::write_tcf_header(&mut output, corpus.get_meta())
                        .map_err(|e| format!("Failed to write TCF: {}", e))?;
                    let replay = std::cell::RefCell::new(Vec::new());
                    let do_replay = std::cell::RefCell::new(true);
                    let mut iter : Box<dyn Iterator<Item=Result<Document, TeangaError>>> = Box::new(
                        input.lines().map(|line| {
                            //let line = line.map_err(|e| format!("Failed to read line: {}", e))?;
                            //let doc = teanga::serialization::read_jsonl_line(line, &mut corpus).map_err(|e| format!("Failed to parse JSON: {}", e))?;
                            let line = line.expect("Failed to read line");
                            let doc = teanga::serialization::read_jsonl_line(line, &mut corpus.clone()).expect("Failed to parse JSON");
                            if *do_replay.borrow() {
                                replay.borrow_mut().push(doc.clone());
                            }
                            Ok(doc)
                        }));
                    let compressor = teanga::write_tcf_config(&mut output, &mut iter, &config)
                        .map_err(|e| format!("Failed to write TCF: {}", e))?;
                    let replay = replay.clone();
                    for doc in replay.borrow().iter() {
                        teanga::write_tcf_doc(&mut output, doc.clone(), &mut cache, &keys, &corpus, &compressor)
                            .map_err(|e| format!("Failed to write TCF: {}", e))?;
                    }
                    *do_replay.borrow_mut() = false;
                    for doc in iter {
                        let doc = doc.map_err(|e| format!("Failed to read document: {}", e))?;
                        teanga::write_tcf_doc(&mut output, doc, &mut cache, &keys, &corpus, &compressor)
                            .map_err(|e| format!("Failed to write TCF: {}", e))?;
                    }
                } else {
                    teanga::write_tcf_with_config(&mut output, &corpus, &config)
                        .map_err(|e| format!("Failed to write TCF: {}", e))?;
                }
            }
            Format::Guess => panic!("unreachable")
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
        SubCommand::Convert(to_cbor) => {
            to_cbor.run().unwrap();
        }
    }
}
