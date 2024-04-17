// Purpose: Rust implementation of the TeangaDB Python module.
// Author: John P. McCrae
// License: Apache 2.0
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use sha2::{Digest, Sha256};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use itertools::Itertools;
use serde::{Serialize,Deserialize};
use thiserror::Error;
use std::fs::File;

pub mod serialization;
pub mod layer_builder;
pub mod disk_corpus;
pub mod simple_corpus;
pub mod transaction_corpus;

use disk_corpus::DiskCorpus;

pub use layer_builder::build_layer;

const DOCUMENT_PREFIX : u8 = 0x00;
const ID2STR_PREFIX : u8 = 0x01;
const STR2ID_PREFIX : u8 = 0x02;
const META_PREFIX : u8 = 0x03;
const ORDER_BYTES : [u8;1] = [0x04];
const STRIDX_SIZE : [u8;1] = [0x05];

/// Trait that defines a corpus according to the Teanga Data Model
pub trait Corpus {
    type LayerStorage : IntoLayer;
    type Content : DocumentContent<Self::LayerStorage>;
    fn add_layer_meta(&mut self, name: String, layer_type: LayerType, 
        base: Option<String>, data: Option<DataType>, link_types: Option<Vec<String>>, 
        target: Option<String>, default: Option<Layer>,
        meta: HashMap<String, Value>) -> TeangaResult<()>;
    fn add_doc<D : IntoLayer, DC : DocumentContent<D>>(&mut self, content : DC) -> TeangaResult<String>;
    fn update_doc<D : IntoLayer, DC: DocumentContent<D>>(&mut self, id : &str, content : DC) -> TeangaResult<String>;
    fn remove_doc(&mut self, id : &str) -> TeangaResult<()>;
    fn get_doc_by_id(&self, id : &str) -> TeangaResult<Self::Content>;
    fn get_docs(&self) -> Vec<String>;
    fn get_meta(&self) -> &HashMap<String, LayerDesc>;
    fn get_meta_mut(&mut self) -> &mut HashMap<String, LayerDesc>;
    fn get_order(&self) -> &Vec<String>;
    fn add_docs<D : IntoLayer, DC : DocumentContent<D>>(&mut self, content : Vec<DC>) -> TeangaResult<Vec<String>> {
        let mut ids = Vec::new();
        for doc in content {
            ids.push(self.add_doc(doc)?);
        }
        Ok(ids)
    }
}

pub trait DocumentContent<D> : IntoIterator<Item=(String, D)> where D : IntoLayer {
    fn keys(&self) -> Vec<String>;
}

impl<D: IntoLayer> DocumentContent<D> for HashMap<String, D> {
    fn keys(&self) -> Vec<String> {
        self.keys().cloned().collect()
    }
}

impl<D: IntoLayer> DocumentContent<D> for Vec<(String, D)> {
    fn keys(&self) -> Vec<String> {
        self.iter().map(|(k, _)| k.clone()).collect()
    }
}

pub trait IntoLayer {
    fn into_layer(self, meta : &LayerDesc) -> TeangaResult<Layer>;
}

impl IntoLayer for Layer {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(self)
    }
}

impl IntoLayer for String {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::Characters(self))
    }
}

impl IntoLayer for &str {
    fn into_layer(self, _meta : &LayerDesc) -> TeangaResult<Layer> {
        Ok(Layer::Characters(self.to_string()))
    }
}

#[derive(Debug,Clone,Serialize,Deserialize)]
/// A layer description
pub struct LayerDesc {
    #[serde(rename = "type")]
    pub layer_type: LayerType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<DataType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_types: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Layer>,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub meta: HashMap<String, Value>, 
}

impl LayerDesc {
    fn new(name: String, layer_type: LayerType, 
        base: Option<String>, data: Option<DataType>, link_types: Option<Vec<String>>, 
        target: Option<String>, default: Option<Layer>,
        meta: HashMap<String, Value>) -> TeangaResult<LayerDesc> {
        if layer_type == LayerType::characters && base != Some("".to_string()) && base != None {
            return Err(TeangaError::ModelError(
                format!("Layer {} of type characters cannot be based on another layer", name)))
        }

        if layer_type != LayerType::characters && (base == Some("".to_string()) || base == None) {
            return Err(TeangaError::ModelError(
                format!("Layer {} of type {} must be based on another layer", name, layer_type)))
        }

        Ok(LayerDesc {
            layer_type,
            base,
            data,
            link_types,
            target,
            default,
            meta
         })
    }
}

#[derive(Debug,Clone,Serialize,Deserialize)]
/// A document object
pub struct Document {
    id : String,
    content: HashMap<String, Layer>
}

impl Document {
    fn new(id : String, content : HashMap<String, Layer>) -> Document {
        Document {
            id, content
        }
    }

    fn from_content<D : IntoLayer, DC : DocumentContent<D>>(
        order : &Vec<String>,
        dc: DC,
        meta : &HashMap<String, LayerDesc>) -> TeangaResult<Document> {
        let mut content : HashMap<String, Layer> = HashMap::new();
        for (k, v) in dc {
            content.insert(k.clone(), v.into_layer(meta.get(&k)
                    .ok_or(TeangaError::ModelError(format!("Layer {} does not exist", k)))?)?);
                }
        Ok(Document {
            id: teanga_id(order, &content),
            content
        })
    }
}

impl IntoIterator for Document {
    type Item = (String, Layer);
    type IntoIter = std::collections::hash_map::IntoIter<String, Layer>;

    fn into_iter(self) -> Self::IntoIter {
        self.content.into_iter()
    }
}

impl DocumentContent<Layer> for Document {
    fn keys(&self) -> Vec<String> {
        self.content.keys().cloned().collect()
    }
}

#[derive(Debug,Clone,PartialEq, Serialize,Deserialize)]
/// Any valid JSON/YAML value
pub enum Value {
    Bool(bool),
    Int(i32),
    Float(f64),
    String(String),
    Array(Vec<Value>),
    Object(HashMap<String, Value>)
}

#[derive(Debug,Clone,PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Layer {
    Characters(String),
    L1(Vec<u32>),
    L2(Vec<(u32,u32)>),
    L3(Vec<(u32,u32,u32)>),
    LS(Vec<String>),
    L1S(Vec<(u32,String)>),
    L2S(Vec<(u32,u32,String)>),
    L3S(Vec<(u32,u32,u32,String)>),
    MetaLayer(Vec<HashMap<String, Value>>)
}

fn teanga_id(existing_keys : &Vec<String>, doc : &HashMap<String, Layer>) -> String {
    let mut hasher = Sha256::new();
    for key in doc.keys().sorted() {
        match doc.get(key).unwrap() {
            Layer::Characters(val) => {
                hasher.update(key.as_bytes());
                hasher.update(vec![0u8]);
                hasher.update(val.as_bytes());
                hasher.update(vec![0u8]);
            }
            _ => ()
        }
    }
    let code = STANDARD.encode(hasher.finalize().as_slice());
    let mut n = 4;
    while existing_keys.contains(&code[..n].to_string()) && n < code.len() {
        n += 1;
    }
    return code[..n].to_string();
}

#[allow(non_camel_case_types)]
#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub enum LayerType {
    characters,
    seq,
    div,
    element,
    span
}

impl Display for LayerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LayerType::characters => write!(f, "characters"),
            LayerType::seq => write!(f, "seq"),
            LayerType::div => write!(f, "div"),
            LayerType::element => write!(f, "element"),
            LayerType::span => write!(f, "span")
        }
    }
}

#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub enum DataType {
    String,
    Enum(Vec<String>),
    Link
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DataType::String => write!(f, "string"),
            DataType::Enum(vals) => write!(f, "enum({})", vals.iter().join(",")),
            DataType::Link => write!(f, "link"),
        }
    }
}

pub fn read_corpus_from_json_string(s : &str, path : &str) -> Result<DiskCorpus, TeangaJsonError> {
    Ok(serialization::read_corpus_from_json_string(s, path)?)
}

pub fn read_corpus_from_yaml_string(s : &str, path: &str) -> Result<DiskCorpus, TeangaYamlError> {
    Ok(serialization::read_corpus_from_yaml_string(s, path)?)
}

pub fn read_corpus_from_yaml_file(yaml : &str, path: &str) -> Result<DiskCorpus, TeangaYamlError> {
    Ok(serialization::read_corpus_from_yaml_file(yaml, path)?)
}

pub fn write_corpus_to_yaml(corpus : &DiskCorpus, path : &str) -> Result<(), TeangaYamlError> {
    let f = File::create(path)?;
    Ok(serialization::pretty_yaml_serialize(corpus, f)?)
}

pub fn write_corpus_to_json(corpus : &DiskCorpus, path : &str) -> Result<(), TeangaJsonError> {
    Ok(serialization::write_corpus_to_json(corpus, path)?)
}

pub fn write_corpus_to_json_string(corpus : &DiskCorpus) -> Result<String, TeangaJsonError> {
    Ok(serialization::write_corpus_to_json_string(corpus)?)
}

pub fn write_corpus_to_yaml_string(corpus : &DiskCorpus) -> Result<String, TeangaYamlError> {
    let mut v = Vec::new();
    serialization::pretty_yaml_serialize(corpus, &mut v)?;
    Ok(String::from_utf8(v)?)
}

pub fn read_corpus_from_json_file(json : &str, path: &str) -> Result<DiskCorpus, TeangaYamlError> {
    Ok(serialization::read_corpus_from_json_file(json, path)?)
}

#[derive(Error, Debug)]
pub enum TeangaError {
    #[error("DB error: {0}")]
    DBError(#[from] polodb_core::Error),
    #[error("Data read error: UTF-8 String could not be decoded")]
    UTFDataError,
    #[error("Teanga model error: {0}")]
    ModelError(String),
}

pub type TeangaResult<T> = Result<T, TeangaError>;

#[derive(Error, Debug)]
pub enum TeangaJsonError {
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Serialization error: {0}")]
    SerdeError(#[from] crate::serialization::SerializeError)
}

#[derive(Error, Debug)]
pub enum TeangaYamlError {
    #[error("YAML error: {0}")]
    YamlError(#[from] serde_yaml::Error),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("UTF-8 error: {0}")]
    UTFError(#[from] std::string::FromUtf8Error),
    #[error("Serialization error: {0}")]
    SerdeError(#[from] crate::serialization::SerializeError)
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::disk_corpus::DiskCorpus;

    #[test]
    fn test_teanga_id_1() {
        let existing_keys = Vec::new();
        let doc = 
             vec![("text".to_string(), 
                         Layer::Characters("This is a document.".to_string()))].into_iter().collect();
        let expected = "Kjco";
        assert_eq!(teanga_id(&existing_keys, &doc), expected);
    }


    #[test]
    fn test_reopen_corpus() {
        let mut corpus = DiskCorpus::new("tmp").unwrap();
        corpus.add_layer_meta("text".to_string(), LayerType::characters, None, Some(DataType::String), None, None, None, HashMap::new()).unwrap();
        corpus.add_doc(vec![("text".to_string(), "test")]).unwrap();
        let _corpus = DiskCorpus::new("tmp").unwrap();
    }
}
