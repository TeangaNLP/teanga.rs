// Serialization support for Teanga DB
// -----------------------------------------------------------------------------
use serde::de::Visitor;
use crate::{Corpus, LayerDesc, PyLayer, CorpusTransaction};
use std::collections::HashMap;
use serde::Deserializer;
use std::cmp::min;
use std::path::Path;
use std::fs::File;
use serde::Serialize;
use serde::ser::{Serializer, SerializeMap};
use thiserror::Error;
use std::io::Write;
use itertools::Itertools;

struct TeangaVisitor(String);

impl<'de> Visitor<'de> for TeangaVisitor {
    type Value = Corpus;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string representing a corpus")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where A: serde::de::MapAccess<'de>
    {
        let mut corpus = Corpus::new(&self.0).map_err(serde::de::Error::custom)?;
        let mut trans = CorpusTransaction::new(&mut corpus).map_err(serde::de::Error::custom)?;
        while let Some(ref key) = map.next_key::<String>()? {
            if key == "_meta" {
                let data = map.next_value::<HashMap<String, LayerDesc>>()?;
                trans.set_meta(data).map_err(serde::de::Error::custom)?;
            } else if key == "_order" {
                let data = map.next_value::<Vec<String>>()?;
                trans.set_order(data).map_err(serde::de::Error::custom)?;
            } else {
                let doc = map.next_value::<HashMap<String, PyLayer>>()?;
                let id = trans.add_doc(doc).map_err(serde::de::Error::custom)?;
                if id[..min(id.len(), key.len())] != key[..min(id.len(), key.len())] {
                    return Err(serde::de::Error::custom(format!("Document fails hash check: {} != {}", id, key)))
                }
            }
        }
        Ok(corpus)
    }
}

impl Serialize for Corpus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("_meta", &self.meta)?;
        map.serialize_entry("_order", &self.order)?;
        for id in &self.order {
            eprintln!("Serializing {}", id);
            map.serialize_entry(id, &self.get_doc_by_id(id).map_err(serde::ser::Error::custom)?)?;
        }
        map.end()
    }
}

pub fn pretty_yaml_serialize<W : Write>(corpus: &Corpus, mut writer: W) -> Result<(), SerializeError> {
    writer.write_all(b"_meta:\n")?;
    for name in corpus.meta.keys().sorted() {
        let meta = &corpus.meta[name];
        writer.write_all(b"    ")?;
        writer.write_all(name.as_bytes())?;
        writer.write_all(b":\n")?;
        writer.write_all(b"        type: ")?;
        writer.write_all(serde_yaml::to_string(&meta.layer_type)?.as_bytes())?;
        if meta.on != "" {
            writer.write_all(b"        on: ")?;
            writer.write_all(serde_yaml::to_string(&meta.on)?.as_bytes())?;
        }
        if let Some(ref data) = meta.data {
            writer.write_all(b"        data: ")?;
            writer.write_all(serde_yaml::to_string(data)?.as_bytes())?;
        }
        if let Some(ref values) = meta.values {
            writer.write_all(b"        values: ")?;
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
    writer.write_all(b"_order: ")?;
    writer.write_all(serde_json::to_string(&corpus.order)?.as_bytes())?;
    writer.write_all(b"\n")?;
    for id in &corpus.order {
        writer.write_all(id.as_bytes())?;
        writer.write_all(b":\n")?;
        let doc = corpus.get_doc_by_id(id)?;
        for name in doc.keys().sorted() {
            let layer = &doc[name];
            if let PyLayer::CharacterLayer(_) = layer {
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

pub fn read_corpus_from_json_string(s: &str, path : &str) -> Result<Corpus, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_str(s);
    deserializer.deserialize_any(TeangaVisitor(path.to_owned()))
}

pub fn read_corpus_from_json_file<P: AsRef<Path>>(json_file : P, path: &str) -> Result<Corpus, SerializeError> {
    let file = File::open(json_file)?;
    let mut deserializer = serde_json::Deserializer::from_reader(file);
    Ok(deserializer.deserialize_any(TeangaVisitor(path.to_owned()))?)
}

pub fn read_corpus_from_yaml_string(s: &str, path : &str) -> Result<Corpus, serde_yaml::Error> {
    let deserializer = serde_yaml::Deserializer::from_str(s);
    deserializer.deserialize_any(TeangaVisitor(path.to_owned()))
}

pub fn read_corpus_from_yaml_file<P: AsRef<Path>>(yaml_file : P, path: &str) -> Result<Corpus, SerializeError> {
    let file = File::open(yaml_file)?;
    let deserializer = serde_yaml::Deserializer::from_reader(file);
    Ok(deserializer.deserialize_any(TeangaVisitor(path.to_owned()))?)
}

pub fn write_corpus_to_json<P: AsRef<Path>>(corpus: &Corpus, path: P) -> Result<(), serde_json::Error> {
    let mut file = File::create(path)
        .expect("Could not create file");
    let mut ser = serde_json::Serializer::new(&mut file);
    corpus.serialize(&mut ser)
}

pub fn write_corpus_to_json_string(corpus: &Corpus) -> Result<String, SerializeError> {
    let mut ser = serde_json::Serializer::new(Vec::new());
    corpus.serialize(&mut ser)?;
    Ok(String::from_utf8(ser.into_inner())?)
}

#[cfg(test)] // Only used for testing ATM
fn write_corpus_to_yaml_file(corpus: &Corpus, mut file : File) -> Result<(), serde_yaml::Error> {
    let mut ser = serde_yaml::Serializer::new(&mut file);
    corpus.serialize(&mut ser)
}

#[derive(Error,Debug)]
pub enum SerializeError {
    #[error("Json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Teanga model error: {0}")]
    Teanga(#[from] crate::TeangaError),
    #[error("IO error: {0}")]
    Fmt(#[from] std::fmt::Error),
    #[error("UTF8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
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
        on: text
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
            "on": "text"
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
        let mut corpus = Corpus::new(&file).expect("Cannot load corpus");
        corpus.add_layer_meta("text".to_string(), crate::LayerType::characters,
           String::new(), None, None, None, None, None).unwrap();
        corpus.add_layer_meta("tokens".to_string(), crate::LayerType::span,
            "text".to_string(), None, None, None, None, None).unwrap();
        let doc = HashMap::from_iter(vec![("text".to_string(), PyLayer::CharacterLayer("This is an example".to_string())),
                                           ("tokens".to_string(), PyLayer::L2(vec![(0, 4), (5, 7), (8, 10), (11, 18)]))]);
        corpus.add_doc(doc).unwrap();
        let outfile = tempfile::tempfile().expect("Cannot create temp file");
        write_corpus_to_yaml_file(&corpus, outfile).unwrap();
    }

    #[test]
    fn test_pretty_yaml() {
        let file = tempfile::tempdir().expect("Cannot create temp folder")
            .path().to_str().unwrap().to_owned();
        let mut corpus = Corpus::new(&file).expect("Cannot load corpus");
        corpus.add_layer_meta("text".to_string(), crate::LayerType::characters,
           String::new(), None, None, None, None, None).unwrap();
        corpus.add_layer_meta("tokens".to_string(), crate::LayerType::span,
            "text".to_string(), None, None, None, None, None).unwrap();
        let doc = HashMap::from_iter(vec![("text".to_string(), PyLayer::CharacterLayer("This is an example".to_string())),
                                           ("tokens".to_string(), PyLayer::L2(vec![(0, 4), (5, 7), (8, 10), (11, 18)]))]);
        corpus.add_doc(doc).unwrap();
        let mut out = Vec::new();
        pretty_yaml_serialize(&corpus, &mut out).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(),
            "_meta:\n    text:\n        type: characters\n    tokens:\n        type: span\n        on: text\n_order: [\"ecWc\"]\necWc:\n    text: This is an example\n    tokens: [[0,4],[5,7],[8,10],[11,18]]\n");
    }
 
    #[test]
    fn test_1() {
        let file = tempfile::tempdir().expect("Cannot create temp folder")
            .path().to_str().unwrap().to_owned();
        read_corpus_from_yaml_string("_meta:\n  text:\n    type: characters\nKjco:\n   text: This is a document.\n", &file).unwrap();
    }
}

