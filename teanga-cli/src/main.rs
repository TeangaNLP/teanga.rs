use clap::{Parser, ValueEnum};
use flate2;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::thread;
use teanga::DiskCorpus;
use teanga::SimpleCorpus;
use teanga::CuacConfig;
use teanga::Corpus;
use teanga::Document;
use teanga::LayerDesc;
use teanga::LayerType;
use teanga::TeangaData;
use teanga::read_cuac;
use teanga::read_json;
use teanga::read_jsonl;
use teanga::read_yaml;
use teanga::read_yaml_with_config;

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
    Show(ShowCommand),
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

/// Command to show one or all documents in a corpus
#[derive(Parser, Debug)]
#[command(name = "show", about = "Show one or all documents in a corpus")]
struct ShowCommand {
    /// Path to a corpus file, or to a DB directory (as used by `load`)
    path: String,

    /// Only show the document with this ID
    #[arg(long)]
    doc_id: Option<String>,

    /// The format of the input file (ignored if `path` is a DB directory)
    #[arg(short, long)]
    #[clap(default_value = "guess")]
    format: Format,

    /// Meta information as a separate YAML file (required for JSONL input)
    #[arg(long)]
    meta: Option<String>,
}

#[derive(ValueEnum, Debug, Clone, PartialEq, Eq)]
#[clap(rename_all = "lowercase")]
enum Format {
    JSON,
    JSONL,
    YAML,
    Cuac,
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
                } else if file.ends_with(".cuac") || file.ends_with(".cuac.gz")  || file.ends_with(".tcf") || file.ends_with(".tcf.gz") {
                    Format::Cuac
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

    /// The string compression method (for Cuac output only). It is best to use
    /// `smaz` for English corpora and `generate` for other languages.
    #[arg(long)]
    #[clap(default_value="smaz")]
    compression: StringCompression,

    /// The number of bytes to use for generate string compression (for Cuac output only, only used if compression is set to generate)
    #[arg(long)]
    #[clap(default_value="1000000")]
    compression_bytes: usize,

    /// Ignore incorrect document IDs
    #[arg(long)]
    #[clap(default_value="false")]
    ignore_id_errors: bool
}

impl LoadCommand {
    fn run(&self) -> Result<(), String> {
        let mut corpus = DiskCorpus::new(&self.db)
            .map_err(|e| format!("Failed to open corpus: {}", e))?;
        if let Some(meta) = &self.meta {
            read_yaml_with_config(File::open(meta)
                .map_err(|e| format!("Failed to open meta file: {}", e))?,
                &mut corpus,
                teanga::SerializationSettings::new().header_only())
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
        let settings = if self.ignore_id_errors {
            teanga::SerializationSettings::new().ignore_id_errors()
        } else {
            teanga::SerializationSettings::new()
        };

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
                    teanga::serialization::read_json_with_config(&mut input, &mut corpus, settings)
                        .map_err(|e| format!("Failed to read JSON: {}", e)).unwrap();
                    }
                Format::JSONL => {
                    if command.meta_file.is_none() {
                        panic!("Meta file is required for JSONL");
                    }
                    if command.output_format.guess(&command.output) == Format::Cuac {
                    } else {
                        teanga::serialization::read_jsonl(&mut input, &mut corpus)
                            .map_err(|e| format!("Failed to read JSONL: {}", e)).unwrap();
                    }
                }
                Format::YAML => {
                    teanga::serialization::read_yaml_with_config(&mut input, &mut corpus, settings)
                        .map_err(|e| format!("Failed to read YAML: {}", e)).unwrap();
                    }
                Format::Cuac => {
                    teanga::read_cuac(&mut input, &mut corpus)
                        .map_err(|e| format!("Failed to read Cuac: {}", e)).unwrap();
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
                Format::Cuac => {
                    let config = match command.compression {
                        StringCompression::None => CuacConfig::new().with_string_compression(teanga::StringCompressionMethod::None),
                        StringCompression::Smaz => CuacConfig::new().with_string_compression(teanga::StringCompressionMethod::Smaz),
                        StringCompression::Shoco => CuacConfig::new().with_string_compression(teanga::StringCompressionMethod::ShocoDefault),
                        StringCompression::Generate => CuacConfig::new().with_string_compression(teanga::StringCompressionMethod::GenerateShocoModel(command.compression_bytes)),
                    };
                    let rx_corpus = rx_corpus.await_meta();
                    teanga::write_cuac_with_config(&mut output, &rx_corpus, &config)
                        .map_err(|e| format!("Failed to write Cuac: {}", e)).unwrap();
                    }
                Format::Guess => panic!("unreachable")
            }
        });
        handle1.join().unwrap();
        handle2.join().unwrap();

        Ok(())
    }
}

impl ShowCommand {
    fn run(&self) -> Result<(), String> {
        if Path::new(&self.path).is_dir() {
            let corpus = DiskCorpus::new(&self.path)
                .map_err(|e| format!("Failed to open corpus: {}", e))?;
            show_corpus(&corpus, &self.doc_id)
        } else {
            let corpus = load_file_corpus(&self.path, &self.format, &self.meta)?;
            show_corpus(&corpus, &self.doc_id)
        }
    }
}

/// Load a corpus file (guessing the format if necessary) into an in-memory `SimpleCorpus`
fn load_file_corpus(path: &str, format: &Format, meta: &Option<String>) -> Result<SimpleCorpus, String> {
    let mut corpus = SimpleCorpus::new();
    if let Some(meta_file) = meta {
        read_yaml_with_config(File::open(meta_file)
            .map_err(|e| format!("Failed to open meta file: {}", e))?,
            &mut corpus,
            teanga::SerializationSettings::new().header_only())
            .map_err(|e| format!("Failed to read meta file: {}", e))?;
    }
    let mut file = if path.ends_with(".gz") {
        let reader = flate2::read::GzDecoder::new(File::open(path)
            .map_err(|e| format!("Failed to open file: {}", e))?);
        Box::new(reader) as Box<dyn std::io::Read>
    } else {
        Box::new(File::open(path)
            .map_err(|e| format!("Failed to open file: {}", e))?) as Box<dyn std::io::Read>
    };
    match format.guess(path) {
        Format::JSON => {
            read_json(&mut file, &mut corpus)
                .map_err(|e| format!("Failed to read JSON: {}", e))?;
        }
        Format::JSONL => {
            if meta.is_none() {
                return Err("A --meta file is required to show JSONL input".to_string());
            }
            read_jsonl(&mut BufReader::new(file), &mut corpus)
                .map_err(|e| format!("Failed to read JSONL: {}", e))?;
        }
        Format::YAML => {
            read_yaml(&mut file, &mut corpus)
                .map_err(|e| format!("Failed to read YAML: {}", e))?;
        }
        Format::Cuac => {
            read_cuac(&mut file, &mut corpus)
                .map_err(|e| format!("Failed to read Cuac: {}", e))?;
        }
        Format::Guess => unreachable!()
    }
    Ok(corpus)
}

/// Show one document (if `doc_id` is set) or every document in the corpus
fn show_corpus<C: Corpus>(corpus: &C, doc_id: &Option<String>) -> Result<(), String> {
    let meta = corpus.get_meta();
    let width = terminal_width();
    let ids = match doc_id {
        Some(id) => vec![id.clone()],
        None => corpus.get_docs(),
    };
    for id in ids {
        let doc = corpus.get_doc_by_id(&id)
            .map_err(|e| format!("Failed to get document {}: {}", id, e))?;
        print_document(&id, &doc, meta, width);
    }
    Ok(())
}

/// The width to wrap displayed lines at: the current terminal width if it can
/// be detected (e.g. not redirected to a file/pipe), otherwise 80 columns
fn terminal_width() -> usize {
    terminal_size::terminal_size()
        .map(|(terminal_size::Width(w), _)| w as usize)
        .unwrap_or(80)
}

/// The "depth" of a layer: 0 for a characters layer, otherwise 1 + the depth of its base
fn layer_depth(name: &str, meta: &HashMap<String, LayerDesc>) -> usize {
    match meta.get(name) {
        Some(desc) => match &desc.base {
            Some(base) => 1 + layer_depth(base, meta),
            None => 0
        },
        None => 0
    }
}

/// Format a single annotation value for inline display next to its token
fn format_data(data: &TeangaData) -> Option<String> {
    match data {
        TeangaData::None => None,
        TeangaData::String(s) => Some(s.clone()),
        TeangaData::Link(i) => Some(format!("->{}", i)),
        TeangaData::TypedLink(i, t) => Some(format!("->{}:{}", i, t)),
    }
}

/// Cells for a standalone layer with no siblings sharing its tokenization:
/// each token, with its own annotation value inlined as `token/value`
fn inline_cells(name: &str, doc: &Document, meta: &HashMap<String, LayerDesc>) -> Vec<String> {
    let text = match doc.text(name, meta) { Ok(t) => t, Err(_) => return Vec::new() };
    let data = doc.data(name, meta);
    text.iter().enumerate().map(|(i, t)| {
        match data.as_ref().and_then(|d| d.get(i)).and_then(format_data) {
            Some(v) => format!("{}/{}", t, v),
            None => t.to_string()
        }
    }).collect()
}

/// Cells for an annotation layer rendered as its own row underneath a shared
/// token row: just the (formatted) value per token, blank if there is none
fn value_cells(name: &str, doc: &Document, meta: &HashMap<String, LayerDesc>) -> Vec<String> {
    match doc.data(name, meta) {
        Some(data) => data.iter().map(|d| format_data(d).unwrap_or_default()).collect(),
        None => doc.text(name, meta).map(|t| t.iter().map(|s| s.to_string()).collect()).unwrap_or_default()
    }
}

/// Print a raw `characters` layer, word-wrapped at `width`
fn print_wrapped(label: &str, text: &str, width: usize) {
    let prefix = format!("{}: ", label);
    let indent = " ".repeat(prefix.len());
    let avail = width.saturating_sub(prefix.len()).max(1);
    let mut line = String::new();
    let mut first = true;
    for word in text.split_whitespace() {
        if !line.is_empty() && line.len() + 1 + word.len() > avail {
            println!("{}{}", if first { &prefix } else { &indent }, line);
            line.clear();
            first = false;
        }
        if !line.is_empty() { line.push(' '); }
        line.push_str(word);
    }
    println!("{}{}", if first { &prefix } else { &indent }, line);
}

/// Print a group of aligned rows (a shared token row plus its annotation
/// rows, or a single standalone row) as a column-aligned table, wrapping to
/// a new set of rows whenever the next column would overflow `width`
fn print_block(rows: &[(String, Vec<String>)], width: usize) {
    if rows.is_empty() { return; }
    let n = rows[0].1.len();
    let label_width = rows.iter().map(|(l, _)| l.len()).max().unwrap_or(0);
    if n == 0 {
        for (label, _) in rows {
            println!("{:lw$}:", label, lw = label_width);
        }
        println!();
        return;
    }
    let col_width: Vec<usize> = (0..n).map(|i| {
        rows.iter().map(|(_, cells)| cells.get(i).map_or(0, |c| c.len())).max().unwrap_or(0)
    }).collect();
    let prefix_width = label_width + 2;
    let mut i = 0;
    while i < n {
        let mut j = i;
        let mut used = prefix_width;
        while j < n {
            let w = col_width[j] + 1;
            if j > i && used + w > width { break; }
            used += w;
            j += 1;
        }
        if j == i { j = i + 1; }
        for (label, cells) in rows {
            let mut line = format!("{:lw$}: ", label, lw = label_width);
            for k in i..j {
                let cell = cells.get(k).map(|s| s.as_str()).unwrap_or("");
                line.push_str(&format!("{:cw$} ", cell, cw = col_width[k]));
            }
            println!("{}", line.trim_end());
        }
        println!();
        i = j;
    }
}

/// Pretty-print a single document: raw text first, then each group of
/// annotation layers that share a common tokenization as an aligned,
/// width-wrapped table, one row per layer
fn print_document(id: &str, doc: &Document, meta: &HashMap<String, LayerDesc>, width: usize) {
    println!("=== {} ===", id);
    let mut layers: Vec<String> = doc.keys().into_iter()
        .filter(|k| !k.starts_with('_'))
        .collect();
    layers.sort_by_key(|name| (layer_depth(name, meta), name.clone()));

    for name in &layers {
        if let Some(desc) = meta.get(name) {
            if desc.layer_type == LayerType::characters {
                if let Ok(text) = doc.text(name, meta) {
                    print_wrapped(name, &text.concat(), width);
                }
            }
        }
    }
    println!();

    // Group `seq` layers together with the layer they annotate, so e.g. `pos`
    // and `lemma` both based on `words` are rendered as aligned rows under
    // one shared `words` token row
    let mut base_groups: HashMap<String, Vec<String>> = HashMap::new();
    for name in &layers {
        if let Some(desc) = meta.get(name) {
            if desc.layer_type == LayerType::seq {
                if let Some(base) = &desc.base {
                    if layers.contains(base) {
                        base_groups.entry(base.clone()).or_default().push(name.clone());
                    }
                }
            }
        }
    }
    for children in base_groups.values_mut() {
        children.sort();
    }

    let mut rendered: std::collections::HashSet<String> = std::collections::HashSet::new();
    for name in &layers {
        if rendered.contains(name) { continue; }
        let Some(desc) = meta.get(name) else { continue };
        if desc.layer_type == LayerType::characters {
            rendered.insert(name.clone());
            continue;
        }

        if let Some(children) = base_groups.get(name) {
            let mut rows = Vec::new();
            let head_text = doc.text(name, meta).unwrap_or_default();
            rows.push((name.clone(), head_text.iter().map(|s| s.to_string()).collect::<Vec<_>>()));
            rendered.insert(name.clone());
            for child in children {
                rows.push((child.clone(), value_cells(child, doc, meta)));
                rendered.insert(child.clone());
            }
            print_block(&rows, width);
        } else {
            let cells = inline_cells(name, doc, meta);
            rendered.insert(name.clone());
            print_block(&[(name.clone(), cells)], width);
        }
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
        },
        SubCommand::Show(show) => {
            if let Err(e) = show.run() {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
