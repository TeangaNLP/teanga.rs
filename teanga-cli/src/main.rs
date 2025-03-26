use clap::{Parser, ValueEnum};
use teanga::DiskCorpus;
use std::fs::File;
use flate2;
use std::io::BufReader;
use teanga::TCFConfig;
use teanga::read_yaml_meta;
use teanga::read_json;
use teanga::read_jsonl;
use teanga::read_yaml;
use std::thread;

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

#[derive(ValueEnum, Debug, Clone, PartialEq, Eq)]
#[clap(rename_all = "lowercase")]
enum StringCompression {
    None,
    Smaz,
    Shoco,
    Generate
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

#[derive(Parser, Debug, Clone)]
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
    meta_file: Option<String>,

    /// The string compression method (for TCF output only). It is best to use
    /// `smaz` for English corpora and `generate` for other languages.
    #[arg(long)]
    #[clap(default_value="smaz")]
    compression: StringCompression,

    /// The number of bytes to use for generate string compression (for TCF output only, only used if compression is set to generate)
    #[arg(long)]
    #[clap(default_value="1000000")]
    compression_bytes: usize
}

impl LoadCommand {
    fn run(&self) -> Result<(), String> {
        let mut corpus = DiskCorpus::new(&self.db)
            .map_err(|e| format!("Failed to open corpus: {}", e))?;
        if let Some(meta) = &self.meta {
            read_yaml_meta(File::open(meta)
                .map_err(|e| format!("Failed to open meta file: {}", e))?,
                &mut corpus)
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
            read_jsonl(&mut BufReader::new(file), &mut corpus)
                .map_err(|e| format!("Failed to read file: {}", e))?;
        } else if self.file.ends_with(".json") || self.file.ends_with(".json.gz") {
            read_json(&mut file, &mut corpus)
                .map_err(|e| format!("Failed to read file: {}", e))?;
        } else {
            read_yaml(&mut file, &mut corpus)
                .map_err(|e| format!("Failed to read file: {}", e))?;
        }
        Ok(())
    }
}

impl ConvertCommand {
    fn run(&self) -> Result<(), String> {
        let (mut corpus, rx_corpus) = teanga::channel_corpus::channel_corpus();
        let command = self.clone();
        let handle1 = thread::spawn(move || {
            let mut input = if command.input.ends_with(".gz") {
                let reader = BufReader::new(flate2::read::GzDecoder::new(File::open(&command.input)
                    .map_err(|e| format!("Failed to open input file: {}", e)).unwrap()));
                Box::new(reader) as Box<dyn std::io::BufRead>
            } else {
                Box::new(BufReader::new(File::open(&command.input)
                    .map_err(|e| format!("Failed to open input file: {}", e)).unwrap())) as Box<dyn std::io::BufRead>
            };

            match command.meta_file {
                Some(ref meta_file) => {
                        corpus.read_yaml_header(File::open(meta_file)
                            .map_err(|e| format!("Failed to open meta file: {}", e)).unwrap()).unwrap();
                            }
                None => {}
            }

            match command.input_format.guess(&command.input) {
                Format::JSON => {
                    teanga::serialization::read_json(&mut input, &mut corpus)
                        .map_err(|e| format!("Failed to read JSON: {}", e)).unwrap();
                }
                Format::JSONL => {
                    if command.meta_file.is_none() {
                        panic!("Meta file is required for JSONL");
                    }
                    if command.output_format.guess(&command.output) == Format::TCF {
                    } else {
                        teanga::serialization::read_jsonl(&mut input, &mut corpus)
                            .map_err(|e| format!("Failed to read JSONL: {}", e)).unwrap();
                    }
                }
                Format::YAML => {
                    teanga::serialization::read_yaml(&mut input, &mut corpus)
                        .map_err(|e| format!("Failed to read YAML: {}", e)).unwrap();
                }
                Format::TCF => {
                    teanga::read_tcf(&mut input, &mut corpus)
                        .map_err(|e| format!("Failed to read TCF: {}", e)).unwrap();
                }
                Format::Guess => panic!("unreachable")
            };

            corpus.close();
        });
        let command = self.clone();
        let handle2 = thread::spawn(move || {
            let mut output = BufWriter::new(File::create(&command.output)
                .map_err(|e| format!("Failed to create output file: {}", e)).unwrap());

            match command.output_format.guess(&command.output) {
                Format::JSON => {
                    let rx_corpus = rx_corpus.await_meta();
                    teanga::serialization::write_json(&mut output, &rx_corpus)
                        .map_err(|e| format!("Failed to write JSON: {}", e)).unwrap();
                }
                Format::JSONL => {
                    let rx_corpus = rx_corpus.await_meta();
                    teanga::serialization::write_jsonl(&mut output, &rx_corpus)
                        .map_err(|e| format!("Failed to write JSONL: {}", e)).unwrap();
                }
                Format::YAML => {
                    let rx_corpus = rx_corpus.await_meta();
                    teanga::serialization::write_yaml(&mut output, &rx_corpus)
                        .map_err(|e| format!("Failed to write YAML: {}", e)).unwrap();
                }
                Format::TCF => {
                    let config = match command.compression {
                        StringCompression::None => TCFConfig::new().with_string_compression(teanga::StringCompressionMethod::None),
                        StringCompression::Smaz => TCFConfig::new().with_string_compression(teanga::StringCompressionMethod::Smaz),
                        StringCompression::Shoco => TCFConfig::new().with_string_compression(teanga::StringCompressionMethod::ShocoDefault),
                        StringCompression::Generate => TCFConfig::new().with_string_compression(teanga::StringCompressionMethod::GenerateShocoModel(command.compression_bytes)),
                    };
                    let rx_corpus = rx_corpus.await_meta();
                    teanga::write_tcf_with_config(&mut output, &rx_corpus, &config)
                        .map_err(|e| format!("Failed to write TCF: {}", e)).unwrap();
                }
                Format::Guess => panic!("unreachable")
            }
        });
        handle1.join().unwrap();
        handle2.join().unwrap();

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
