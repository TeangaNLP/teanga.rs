//! Serialization support for Teanga
use crate::{Corpus, DiskCorpus, WriteableCorpus, LayerDesc, Layer, TransactionCorpus, TeangaJsonError, Document};
use itertools::Itertools;
use serde::Deserializer;
use serde::Serialize;
use serde::de::Visitor;
use serde::ser::{Serializer, SerializeMap};
use std::cmp::min;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use thiserror::Error;
use reqwest;
use flate2;

struct TeangaVisitor2<'a, C : WriteableCorpus>(&'a mut C, bool);

impl <'de,'a, C: WriteableCorpus> Visitor<'de> for TeangaVisitor2<'a, C> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string representing a corpus")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where A: serde::de::MapAccess<'de>
    {
         while let Some(ref key) = map.next_key::<String>()? {
            if key == "_meta" {
                let data = map.next_value::<HashMap<String, LayerDesc>>()?;
                self.0.set_meta(data);
            } else if !self.1 && key == "_order" {
                let data = map.next_value::<Vec<String>>()?;
                self.0.set_order(data);
            } else if !self.1 {
                let doc = map.next_value::<HashMap<String, Layer>>()?;
                let id = self.0.add_doc(doc).map_err(serde::de::Error::custom)?;
                if id[..min(id.len(), key.len())] != key[..min(id.len(), key.len())] {
                    return Err(serde::de::Error::custom(format!("Document fails hash check: {} != {}", id, key)))
                }
            }
        }
        Ok(())
    }
}

fn corpus_serialize<C : Corpus, S>(c : &C, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer, C::Content : Serialize
{
    let mut map = serializer.serialize_map(Some(3))?;
    map.serialize_entry("_meta", c.get_meta())?;
    for id in c.get_order() {
        map.serialize_entry(id, &c.get_doc_by_id(id).map_err(serde::ser::Error::custom)?)?;
    }
    map.end()
}

/// Write a corpus as pretty YAML
///
/// # Arguments
///
/// * `corpus` - The corpus to write
/// * `writer` - The writer to write to
///
/// # Returns
///
/// A result indicating success or failure
pub fn pretty_yaml_serialize<W : Write, C: Corpus>(corpus: &C, mut writer: W) -> Result<(), SerializeError> {
    writer.write_all(b"_meta:\n")?;
    for name in corpus.get_meta().keys().sorted() {
        let meta = &corpus.get_meta()[name];
        writer.write_all(b"    ")?;
        writer.write_all(name.as_bytes())?;
        writer.write_all(b":\n")?;
        writer.write_all(b"        type: ")?;
        writer.write_all(serde_yaml::to_string(&meta.layer_type)?.as_bytes())?;
        if meta.base != Some("".to_string()) && meta.base != None {
            writer.write_all(b"        base: ")?;
            writer.write_all(serde_yaml::to_string(&meta.base)?.as_bytes())?;
        }
        if let Some(ref data) = meta.data {
            writer.write_all(b"        data: ")?;
            writer.write_all(serde_yaml::to_string(data)?.as_bytes())?;
        }
        if let Some(ref values) = meta.link_types {
            writer.write_all(b"        link_types: ")?;
            writer.write_all(serde_json::to_string(values)?.as_bytes())?;
            writer.write_all(b"\n")?;
        }
        if let Some(ref target) = meta.target {
            writer.write_all(b"        target: ")?;
            writer.write_all(serde_yaml::to_string(target)?.as_bytes())?;
        }
        if let Some(ref default) = meta.default {
            writer.write_all(b"        default: ")?;
            writer.write_all(serde_json::to_string(default)?.as_bytes())?;
            writer.write_all(b"\n")?;
        }
    }
    for id in corpus.get_order() {
        writer.write_all(id.as_bytes())?;
        writer.write_all(b":\n")?;
        let doc = corpus.get_doc_by_id(id)?;
        for name in doc.keys().iter().sorted() {
            let layer = &doc[name];
            if let Layer::Characters(_) = layer {
                writer.write_all(b"    ")?;
                writer.write_all(name.as_bytes())?;
                writer.write_all(b": ")?;
                writer.write_all(serde_yaml::to_string(layer)?.as_bytes())?;
            } else {
                writer.write_all(b"    ")?;
                writer.write_all(name.as_bytes())?;
                writer.write_all(b": ")?;
                writer.write_all(serde_json::to_string(layer)?.as_bytes())?;
                writer.write_all(b"\n")?;
            }
        }
    }
    Ok(())
}

/// Read a corpus from JSON
///
/// # Arguments
///
/// * `reader` - The reader to read from
/// * `corpus` - The corpus to read into
/// * `meta_only` - Whether to read only the metadata
pub fn read_json<'de, R: Read, C: WriteableCorpus>(reader: R, corpus : &mut C, meta_only : bool) -> Result<(), serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    deserializer.deserialize_any(TeangaVisitor2(corpus, meta_only))
}

/// Read a corpus from YAML
///
/// # Arguments
///
/// * `reader` - The reader to read from
/// * `corpus` - The corpus to read into
/// * `meta_only` - Whether to read only the metadata
pub fn read_yaml<'de, R: Read, C: WriteableCorpus>(reader: R, corpus : &mut C, meta_only : bool) -> Result<(), serde_yaml::Error> {
    let deserializer = serde_yaml::Deserializer::from_reader(reader);
    deserializer.deserialize_any(TeangaVisitor2(corpus, meta_only))
}

/// Read a corpus from JSONL. That is a file with one JSON document per line. 
/// As this format does not have metadata, the corpus must have already been
/// initialized with metadata.
///
/// # Arguments
///
/// * `reader` - The reader to read from
/// * `corpus` - The corpus to read into
pub fn read_jsonl<'de, R: BufRead, C : WriteableCorpus>(reader: R, corpus : &mut C) -> Result<(), TeangaJsonError> {
    for line in reader.lines() {
        let doc : HashMap<String, Layer> = serde_json::from_str(&line?)?;
        corpus.add_doc(doc)?;
    }
    Ok(())
}

/// Read a single line of JSON as a JSON-L document
///
/// # Arguments
///
/// * `line` - The line to read
/// * `corpus` - The corpus to read into
pub fn read_jsonl_line<'de, C : WriteableCorpus>(line: String,
    corpus : &mut C) -> Result<Document, TeangaJsonError> {
        let doc : HashMap<String, Layer> = serde_json::from_str(&line)?;
        Ok(Document::new(doc, corpus.get_meta())?)
}

/// Write a corpus as JSON
///
/// # Arguments
///
/// * `writer` - The writer to write to
/// * `corpus` - The corpus to write
pub fn write_json<W : Write, C : Corpus>(mut writer : W, corpus : &C) -> Result<(), serde_json::Error> 
    where C::Content : Serialize {
    let mut ser = serde_json::Serializer::new(&mut writer);
    corpus_serialize(corpus, &mut ser)
}

/// Write a corpus as YAML
///
/// # Arguments
///
/// * `writer` - The writer to write to
/// * `corpus` - The corpus to write
pub fn write_yaml<W : Write, C : Corpus>(mut writer : W, corpus : &C) -> Result<(), serde_yaml::Error> 
    where C::Content : Serialize {
    let mut ser = serde_yaml::Serializer::new(&mut writer);
    corpus_serialize(corpus, &mut ser)
}


/// Write a corpus as JSONL. This will not write the metadata of the corpus.
///
/// # Arguments
///
/// * `writer` - The writer to write to
/// * `corpus` - The corpus to write
pub fn write_jsonl<W : Write, C : Corpus>(mut writer : W, corpus : &C) -> Result<(), SerializeError>
    where C::Content : Serialize {
    for id in corpus.get_order() {
        let doc = corpus.get_doc_by_id(id)?;
        serde_json::to_writer(&mut writer, &doc)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}

pub fn read_corpus_from_json_string(s: &str, path : &str) -> Result<DiskCorpus, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_str(s);
    let mut corpus = TransactionCorpus::new(path).map_err(serde::de::Error::custom)?;
    deserializer.deserialize_any(TeangaVisitor2(&mut corpus, false))?;
    Ok(corpus.commit().map_err(serde::de::Error::custom)?)
}

pub fn read_corpus_from_json_file<P: AsRef<Path>>(json_file : P, path: &str) -> Result<DiskCorpus, SerializeError> {
    let file = File::open(json_file)?;
    let mut corpus = TransactionCorpus::new(path)?;
    let mut deserializer = serde_json::Deserializer::from_reader(file);
    deserializer.deserialize_any(TeangaVisitor2(&mut corpus, false))?;
    Ok(corpus.commit()?)
}

pub fn read_corpus_from_yaml_string(s: &str, path : &str) -> Result<DiskCorpus, SerializeError> {
    let deserializer = serde_yaml::Deserializer::from_str(s);
    let mut corpus = TransactionCorpus::new(path)?;
    deserializer.deserialize_any(TeangaVisitor2(&mut corpus, false))?;
    Ok(corpus.commit()?)
}

pub fn read_corpus_from_yaml_file<P: AsRef<Path>>(yaml_file : P, path: &str) -> Result<DiskCorpus, SerializeError> {
    let file = File::open(yaml_file)?;
    let mut corpus = TransactionCorpus::new(path)?;
    let deserializer = serde_yaml::Deserializer::from_reader(file);
    deserializer.deserialize_any(TeangaVisitor2(&mut corpus, false))?;
    Ok(corpus.commit()?)
}

pub fn read_corpus_from_yaml_url(url: &str, path: &str) -> Result<DiskCorpus, SerializeError> {
    let response = reqwest::blocking::get(url)?;
    let mut corpus = TransactionCorpus::new(path)?;
    if url.ends_with(".gz") {
        let mut decompressor = flate2::read::GzDecoder::new(response);
        let deserializer = serde_yaml::Deserializer::from_reader(&mut decompressor);
        deserializer.deserialize_any(TeangaVisitor2(&mut corpus, false))?;
    } else {
        let deserializer = serde_yaml::Deserializer::from_reader(response);
        deserializer.deserialize_any(TeangaVisitor2(&mut corpus, false))?;
    }
    Ok(corpus.commit()?)
}

pub fn write_corpus_to_json<P: AsRef<Path>, C : Corpus>(corpus: &C, path: P) -> Result<(), serde_json::Error> 
    where C::Content : Serialize {
    let mut file = File::create(path)
        .expect("Could not create file");
    let mut ser = serde_json::Serializer::new(&mut file);
    corpus_serialize(corpus, &mut ser)
}

pub fn write_corpus_to_json_string<C : Corpus>(corpus: &C) -> Result<String, SerializeError> 
    where C::Content : Serialize {
    let mut ser = serde_json::Serializer::new(Vec::new());
    corpus_serialize(corpus, &mut ser)?;
    Ok(String::from_utf8(ser.into_inner())?)
}

#[cfg(test)] // Only used for testing ATM
fn write_corpus_to_yaml_file(corpus: &DiskCorpus, mut file : File) -> Result<(), serde_yaml::Error> {
    let mut ser = serde_yaml::Serializer::new(&mut file);
    corpus_serialize(corpus, &mut ser)
}

/// A serialization error
#[derive(Error,Debug)]
pub enum SerializeError {
    /// An error occurred during JSON serialization
    #[error("Json error: {0}")]
    Json(#[from] serde_json::Error),
    /// An error occurred during YAML serialization
    #[error("Yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// A generic I/O Error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// An error with the data was encountered
    #[error("Teanga model error: {0}")]
    Teanga(#[from] crate::TeangaError),
    /// An error when formatting was encountered
    #[error("IO error: {0}")]
    Fmt(#[from] std::fmt::Error),
    /// An error in decoding UTF-8
    #[error("UTF8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    /// An error reading from a URL
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    //#[error("Flate2 error: {0}")]
    //Flate2(#[from] flate2::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_yaml() {
        let doc = "_meta:
    text:
        type: characters
    tokens:
        type: span
        base: text
_order: [\"ecWc\"]
ecWc:
    text: This is an example
    tokens: [[0, 4], [5, 7], [8, 10], [11, 18]]
";
        let file = tempfile::tempdir().expect("Cannot create temp folder")
            .path().to_str().unwrap().to_owned();
        read_corpus_from_yaml_string(doc, &file).unwrap();
    }

    #[test]
    fn test_deserialize_json() {
        let doc = r#"{
    "_meta": {
        "text": {
            "type": "characters"
        },
        "tokens": {
            "type": "span",
            "base": "text"
        }
    },
    "_order": [
        "ecWc"
    ],
    "ecWc": {
        "text": "This is an example",
        "tokens": [
            [
                0,
                4
            ],
            [
                5,
                7
            ],
            [
                8,
                10
            ],
            [
                11,
                18
            ]
        ]
    }
}"#;
        let file = tempfile::tempdir().expect("Cannot create temp folder")
            .path().to_str().unwrap().to_owned();
        read_corpus_from_json_string(doc, &file).unwrap();
    }

    #[test]
    fn test_serialize_yaml() {
        let file = tempfile::tempdir().expect("Cannot create temp folder")
            .path().to_str().unwrap().to_owned();
        let mut corpus = DiskCorpus::new(&file).expect("Cannot load corpus");
        corpus.add_layer_meta("text".to_string(), crate::LayerType::characters,
           None, None, None, None, None, HashMap::new()).unwrap();
        corpus.add_layer_meta("tokens".to_string(), crate::LayerType::span,
            Some("text".to_string()), None, None, None, None, HashMap::new()).unwrap();
        let doc = HashMap::from_iter(vec![("text".to_string(), Layer::Characters("This is an example".to_string())),
                                           ("tokens".to_string(), Layer::L2(vec![(0, 4), (5, 7), (8, 10), (11, 18)]))]);
        corpus.add_doc(doc).unwrap();
        let outfile = tempfile::tempfile().expect("Cannot create temp file");
        write_corpus_to_yaml_file(&corpus, outfile).unwrap();
    }

    #[test]
    fn test_pretty_yaml() {
        let file = tempfile::tempdir().expect("Cannot create temp folder")
            .path().to_str().unwrap().to_owned();
        let mut corpus = DiskCorpus::new(&file).expect("Cannot load corpus");
        corpus.add_layer_meta("text".to_string(), crate::LayerType::characters,
           None, None, None, None, None, HashMap::new()).unwrap();
        corpus.add_layer_meta("tokens".to_string(), crate::LayerType::span,
            Some("text".to_string()), None, None, None, None, HashMap::new()).unwrap();
        let doc = HashMap::from_iter(vec![("text".to_string(), Layer::Characters("This is an example".to_string())),
                                           ("tokens".to_string(), Layer::L2(vec![(0, 4), (5, 7), (8, 10), (11, 18)]))]);
        corpus.add_doc(doc).unwrap();
        let mut out = Vec::new();
        pretty_yaml_serialize(&corpus, &mut out).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(),
            "_meta:\n    text:\n        type: characters\n    tokens:\n        type: span\n        base: text\necWc:\n    text: This is an example\n    tokens: [[0,4],[5,7],[8,10],[11,18]]\n");
    }
 
    #[test]
    fn test_1() {
        let file = tempfile::tempdir().expect("Cannot create temp folder")
            .path().to_str().unwrap().to_owned();
        read_corpus_from_yaml_string("_meta:\n  text:\n    type: characters\nKjco:\n   text: This is a document.\n", &file).unwrap();
    }

    #[test]
    fn test_2() {
        let data = "_meta:
  text:
    type: characters
  document:
    type: div
    base: characters".to_string();
 
        read_yaml(data.as_bytes(), &mut TransactionCorpus::new("test").unwrap(), true).unwrap();
    }
}

