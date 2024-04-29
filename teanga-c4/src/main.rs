use clap::Parser;
use teanga::TransactionCorpus;
use flate2;
use serde_json::Value;
use teanga::{Corpus, build_layer, LayerType, Layer, TeangaResult, DataType};
use std::io::BufRead;
use indicatif::ProgressBar;

#[derive(Parser, Debug)]
#[command(name = "teanga_c4", version, about)]
struct Opts {
    #[arg(long, default_value="0")]
    min: usize,
    
    #[arg(long, default_value="1024")]
    max: usize,
}

fn init_corpus<C : Corpus>(corpus: &mut C) -> TeangaResult<()> {
    build_layer(corpus, "text")
        .add()?;
    build_layer(corpus, "document")
        .base("text")
        .layer_type(LayerType::span)
        .default(Layer::L1(vec![0u32]))
        .add()?;
    build_layer(corpus, "url")
        .base("document")
        .layer_type(LayerType::div)
        .data(DataType::String)
        .add()?;
    build_layer(corpus, "timestamp")
        .base("document")
        .layer_type(LayerType::div)
        .data(DataType::String)
        .add()?;
    Ok(())
}

fn download_c4_frag<C: Corpus>(corpus: &mut C, url: &str) -> Result<(), String> {
    eprintln!("Processing {}", url);
    let response = reqwest::blocking::get(url)
        .map_err(|e| format!("Failed to download {}: {}", url, e))?;
    if !response.status().is_success() {
        return Err(format!("Failed to download {}: {}", url, response.status()));
    }

    let reader = flate2::read::GzDecoder::new(response);
    let reader = std::io::BufReader::new(reader);
    let bar = ProgressBar::new(356000);
    let mut docs = Vec::new();
    for line in reader.lines() {
        let json: Value = serde_json::from_str(&line
            .map_err(|e| format!("Failed to read line: {}", e))?)
            .map_err(|e| format!("Failed to parse JSON: {}", e))?;
        docs.push(
            vec![("text".to_string(), json["text"].as_str().
             ok_or_else(|| format!("No text field"))?.to_string()),
             ("url".to_string(), json["url"].as_str().
             ok_or_else(|| format!("No url field"))?.to_string()),
             ("timestamp".to_string(), json["timestamp"].as_str().
             ok_or_else(|| format!("No timestamp field"))?.to_string())]);
        bar.inc(1);
    }
    bar.finish();
    eprintln!("Adding docs to corpus");
    corpus.add_docs(docs)
        .map_err(|e| format!("Failed to add docs: {}", e))?;
    Ok(())
}

fn main() {
    let opts = Opts::parse();

    let mut corpus = TransactionCorpus::new("c4").unwrap();

    init_corpus(&mut corpus).unwrap();

    for i in opts.min..opts.max {
        download_c4_frag(&mut corpus,
            &format!("https://huggingface.co/datasets/allenai/c4/resolve/main/en/c4-train.{:05}-of-01024.json.gz?download=true", i)).unwrap();
    }
    
    corpus.commit().unwrap();
}
