// Purpose: Rust implementation of the TeangaDB Python module.
// Author: John P. McCrae
// License: Apache 2.0
use std::collections::HashMap;
use sled;
use ciborium::{from_reader, into_writer};
use sha2::{Digest, Sha256};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use itertools::Itertools;
use serde::{Serialize,Deserialize};
use thiserror::Error;
use std::fs::File;

pub mod disk_corpus;
pub mod document;
pub mod layer;
pub mod layer_builder;
pub mod serialization;
pub mod match_condition;
pub mod transaction_corpus;
mod tcf;

pub use document::{Document, DocumentContent};
pub use disk_corpus::DiskCorpus;
pub use transaction_corpus::TransactionCorpus;
pub use layer::{IntoLayer, Layer, LayerDesc, DataType, LayerType, TeangaData};
pub use layer_builder::build_layer;
pub use tcf::{write_tcf_corpus, write_tcf, read_tcf, doc_content_to_bytes, bytes_to_doc, Index, IndexResult};
pub use match_condition::{TextMatchCondition, DataMatchCondition};

const DOCUMENT_PREFIX : u8 = 0x00;
const META_PREFIX : u8 = 0x03;
const ORDER_BYTES : [u8;1] = [0x04];

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
    fn get_doc_by_id(&self, id : &str) -> TeangaResult<Document>;
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
    fn text_freq<C: TextMatchCondition>(&self, layer : &str, condition : C) -> TeangaResult<HashMap<String, u32>> {
        let mut freq = HashMap::new();
        for doc_id in self.get_docs() {
            let doc = self.get_doc_by_id(&doc_id)?;
            if let Some(text) = doc.text(layer, self.get_meta()) {
                for word in text {
                    if condition.matches(word) {
                        *freq.entry(word.to_string()).or_insert(0) += 1;
                    }
                }
            }
        }
        Ok(freq)
    }

    fn val_freq<C: DataMatchCondition>(&self, layer : &str, condition : C) -> TeangaResult<HashMap<TeangaData, u32>> {
        let mut freq = HashMap::new();
        for doc_id in self.get_docs() {
            let doc = self.get_doc_by_id(&doc_id)?;
            if let Some(data) = doc.data(layer, self.get_meta()) {
                for val in data {
                    if condition.matches(&val) {
                        *freq.entry(val).or_insert(0) += 1;
                    }
                }
            }
        }
        Ok(freq)
    } 
}

pub trait WriteableCorpus : Corpus {
    fn set_meta(&mut self, meta : HashMap<String, LayerDesc>);
    fn set_order(&mut self, order : Vec<String>);
}


#[derive(Debug, Clone)]
/// An in-memory corpus object
pub struct SimpleCorpus {
    pub meta: HashMap<String, LayerDesc>,
    pub order: Vec<String>,
    pub content: HashMap<String, Document>
}

impl SimpleCorpus {
    pub fn new() -> SimpleCorpus {
        SimpleCorpus {
            meta: HashMap::new(),
            order: Vec::new(),
            content: HashMap::new(),
        }
    }

    pub fn read_yaml_header<'de, R: std::io::Read>(&mut self, r: R) -> Result<(), TeangaYamlError> {
        Ok(crate::serialization::read_yaml(r, self, true)?)
    }

}

impl Corpus for SimpleCorpus {
    type LayerStorage = Layer;
    type Content = Document;

    /// Add a layer to the corpus
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the layer
    /// * `layer_type` - The type of the layer
    /// * `base` - The layer that this layer is on
    /// * `data` - The data file for this layer
    /// * `values` - The values for this layer
    /// * `target` - The target layer for this layer
    /// * `default` - The default values for this layer
    /// * `uri` - The URI of metadata about this layer
    fn add_layer_meta(&mut self, name: String, layer_type: LayerType, 
        base: Option<String>, data: Option<DataType>, link_types: Option<Vec<String>>, 
        target: Option<String>, default: Option<Layer>,
        meta : HashMap<String, Value>) -> TeangaResult<()> {
        self.meta.insert(name.clone(), LayerDesc {
            layer_type,
            base,
            data,
            link_types,
            target,
            default,
            meta
        });
        Ok(())
    }

    /// Add or update a document in the corpus
    ///
    /// # Arguments
    ///
    /// * `content` - The content of the document
    ///
    /// # Returns
    ///
    /// The ID of the document
    fn add_doc<D : IntoLayer, DC : DocumentContent<D>>(&mut self, content : DC) -> TeangaResult<String> {
        let doc = Document::new(content, &self.meta)?;
        let id = teanga_id(&self.order, &doc);
        self.order.push(id.clone());
        self.content.insert(id.clone(), doc);
        Ok(id)
    }

    /// Update a document
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    /// * `content` - The content of the document
    ///
    /// # Returns
    ///
    /// The new ID of the document (if no text layers are changed this will be the same as input)
    fn update_doc<D : IntoLayer, DC: DocumentContent<D>>(&mut self, id : &str, content : DC) -> TeangaResult<String> {
        let doc = Document::new(content, &self.meta)?;
        let new_id = teanga_id(&self.order, &doc);
        if id != new_id {
            let n = self.order.iter().position(|x| x == id).ok_or_else(|| TeangaError::ModelError(
                format!("Cannot find document in order vector: {}", id)))?;
            self.order.remove(n);
            self.order.insert(n, new_id.clone());
            self.content.remove(id);
            self.content.insert(new_id.clone(), doc);
        } else {
            self.content.insert(id.to_string(), doc);
        }
        Ok(new_id)
    }

    fn remove_doc(&mut self, id : &str) -> TeangaResult<()> {
        self.content.remove(id);
        self.order.retain(|x| x != id);
        Ok(())
    }

    fn get_doc_by_id(&self, id : &str) -> TeangaResult<Document> {
        match self.content.get(id) {
            Some(doc) => {
                Ok(doc.clone())
            },
            None => Err(TeangaError::ModelError(
                format!("Document not found")))
        }
    }

    fn get_docs(&self) -> Vec<String> {
        self.order.clone()
    }

    fn get_meta(&self) -> &HashMap<String, LayerDesc> {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut HashMap<String, LayerDesc> {
        &mut self.meta
    }

    fn get_order(&self) -> &Vec<String> {
        &self.order
    }
}

impl WriteableCorpus for SimpleCorpus {
    fn set_meta(&mut self, meta : HashMap<String, LayerDesc>) {
        self.meta = meta;
    }

    fn set_order(&mut self, order : Vec<String>) {
        self.order = order;
    }
}

fn to_stdvec<T : Serialize>(t : &T) -> TeangaResult<Vec<u8>> {
    let mut v = Vec::new();
    into_writer(t,  &mut v).map_err(|e| TeangaError::DataError(e))?;
    Ok(v)
}

fn from_bytes<T : serde::de::DeserializeOwned>(bytes : &[u8]) -> TeangaResult<T> {
    from_reader(bytes).map_err(|e| TeangaError::DataError2(e))
}

fn open_db(path : &str) -> TeangaResult<sled::Db> {
    sled::open(path).map_err(|e| TeangaError::DBError(e))
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

/// Generate a unique ID for a document
pub fn teanga_id(existing_keys : &Vec<String>, doc : &Document) -> String {
    let mut hasher = Sha256::new();
    for key in doc.content.keys().sorted() {
        match doc.content.get(key).unwrap() {
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
    #[error("DB read error: {0}")]
    DBError(#[from] sled::Error),
    #[error("DB transaction error: {0}")]
    DBTXError(#[from] sled::transaction::TransactionError<sled::Error>),
    #[error("Data error: {0}")]
    DataError(#[from] ciborium::ser::Error<std::io::Error>),
    #[error("Data error: {0}")]
    DataError2(#[from] ciborium::de::Error<std::io::Error>),
    #[error("Data read error: UTF-8 String could not be decoded")]
    UTFDataError,
    #[error("Teanga model error: {0}")]
    ModelError(String),
    #[error("TCF Corpora cannot be mutated")]
    TCFMutError,
    #[error("TCF Read Error: {0}")]
    TCFReadError(#[from] crate::tcf::TCFError),
    #[error("Document key {0} not in meta")]
    DocumentKeyError(String)
}

pub type TeangaResult<T> = Result<T, TeangaError>;

#[derive(Error, Debug)]
pub enum TeangaJsonError {
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Serialization error: {0}")]
    SerdeError(#[from] crate::serialization::SerializeError),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Teanga error: {0}")]
    TeangaError(#[from] TeangaError)
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

    #[test]
    fn test_teanga_id_1() {
        let existing_keys = Vec::new();
        let doc = Document {
            content: vec![("text".to_string(), 
                         Layer::Characters("This is a document.".to_string()))].into_iter().collect()
        };
        let expected = "Kjco";
        assert_eq!(teanga_id(&existing_keys, &doc), expected);
    }

    #[test]
    fn test_reopen_corpus() {
        let mut corpus = DiskCorpus::new("tmp").unwrap();
        corpus.add_layer_meta("text".to_string(), LayerType::characters, None, Some(DataType::Enum(vec!["a".to_string(),"b".to_string()])), None, None, None, HashMap::new()).unwrap();
        corpus.add_doc(vec![("text".to_string(), "test")]).unwrap();
        let _corpus = DiskCorpus::new("tmp");
    }

    #[test]
    fn test_serialize_layer() {
        let layer = Layer::L1S(vec![(1,"a".to_string()),(2,"b".to_string())]);
        let s = serde_json::to_string(&layer).unwrap();
        assert_eq!(s, r#"[[1,"a"],[2,"b"]]"#);
        let layer2 : Layer = serde_json::from_str(&s).unwrap();
        assert_eq!(layer, layer2);
        let layer3 = Layer::L1(vec![0]);
        let layer4 : Layer = serde_json::from_str("[0]").unwrap();
        assert_eq!(layer3, layer4);
    }
}
