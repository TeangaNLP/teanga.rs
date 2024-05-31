use clap::{Parser, ValueEnum};
use teanga::TransactionCorpus;
use std::fs::File;
use flate2;
use std::io::BufReader;

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

#[derive(ValueEnum, Debug, Clone)]
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

        match self.input_format.guess(&self.input) {
            Format::JSON => {
                teanga::serialization::read_json(&mut input, &mut corpus, false)
                    .map_err(|e| format!("Failed to read JSON: {}", e))?;
            }
            Format::JSONL => {
                if self.meta_file.is_none() {
                    return Err("Meta file is required for JSONL".to_string());
                }
                teanga::serialization::read_jsonl(&mut input, &mut corpus)
                    .map_err(|e| format!("Failed to read JSONL: {}", e))?;
            }
            Format::YAML => {
                teanga::serialization::read_yaml(&mut input, &mut corpus, false)
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
                teanga::write_tcf(&mut output, &corpus)
                    .map_err(|e| format!("Failed to write TCF: {}", e))?;
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
