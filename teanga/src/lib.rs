//! # Teanga 
//!
//! Teanga is a datamodel for text corpora that is designed to be simple to use and efficient to store.
//! This implementation provides both in-memory and disk-based corpora that can be used to store and query text corpora.
//!
//! ## Example
//!
//! ```rust
//! use teanga::*;
//! let mut corpus = SimpleCorpus::new();
//! corpus.build_layer("text").add();
//! corpus.build_doc().layer("text", "This is a test document").unwrap().add();
//! ```
//
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
pub mod query;
pub mod serialization;
pub mod match_condition;
pub mod transaction_corpus;
mod tcf;

pub use document::{Document, DocumentContent, DocumentBuilder};
pub use disk_corpus::DiskCorpus;
pub use transaction_corpus::TransactionCorpus;
pub use layer::{IntoLayer, Layer, LayerDesc, DataType, LayerType, TeangaData};
pub use layer_builder::build_layer;
pub use query::Query;
pub use tcf::{write_tcf, read_tcf, write_tcf_header, write_tcf_doc, doc_content_to_bytes, bytes_to_doc, Index, IndexResult, TCFReadError, TCFWriteError};
pub use match_condition::{TextMatchCondition, DataMatchCondition};

const DOCUMENT_PREFIX : u8 = 0x00;
const META_PREFIX : u8 = 0x03;
const ORDER_BYTES : [u8;1] = [0x04];

/// Trait that defines a corpus according to the Teanga Data Model
pub trait Corpus {
    /// The type of the layer storage
    type LayerStorage : IntoLayer;
    /// The type of the content
    type Content : DocumentContent<Self::LayerStorage>;
    
    /// Add a meta layer to the corpus
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the layer
    /// * `layer_type` - The type of the layer
    /// * `base` - The layer that this layer is on
    /// * `data` - The data file for this layer
    /// * `link_types` - The link types for this layer
    /// * `target` - The target layer for this layer (if using link data)
    /// * `default` - The default values for this layer
    /// * `meta` - The metadata for this layer
    fn add_layer_meta(&mut self, name: String, layer_type: LayerType, 
        base: Option<String>, data: Option<DataType>, link_types: Option<Vec<String>>, 
        target: Option<String>, default: Option<Layer>,
        meta: HashMap<String, Value>) -> TeangaResult<()>;
    /// Build a layer using a builder
    ///
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the layer
    ///
    /// # Returns
    ///
    /// A builder object
    fn build_layer(&mut self, name: &str) -> crate::layer_builder::LayerBuilderImpl<Self::LayerStorage, Self::Content, Self> where Self: Sized {
        build_layer(self, name)
    }
    /// Add a document to this corpus
    ///
    /// # Arguments
    ///
    /// * `content` - The content of the document
    ///
    /// # Returns
    ///
    /// The ID of the document
    fn add_doc<D : IntoLayer, DC : DocumentContent<D>>(&mut self, content : DC) -> TeangaResult<String>;

    /// Build a document using a builder
    ///
    /// # Returns
    ///
    /// A builder object
    fn build_doc<'a>(&'a mut self) -> DocumentBuilder<'a, Self> where Self : Sized {
        DocumentBuilder::new(self)
    }

    /// Update the content of a document. This preserves the order of the documents
    /// in the corpus
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    /// * `content` - The content of the document
    ///
    /// # Returns
    ///
    /// The new ID of the document (if no text layers are changed this will be the same as input)
    fn update_doc<D : IntoLayer, DC: DocumentContent<D>>(&mut self, id : &str, content : DC) -> TeangaResult<String>;
    
    /// Remove a single document from the corpus
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    fn remove_doc(&mut self, id : &str) -> TeangaResult<()>;

    /// Get a document object by its ID
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    fn get_doc_by_id(&self, id : &str) -> TeangaResult<Document>;

    /// Get the IDs of all documents in the corpus
    fn get_docs(&self) -> Vec<String>;
    
    /// Get the layer metadata
    fn get_meta(&self) -> &HashMap<String, LayerDesc>;

    /// Get the layer metadata as a mutable reference
    fn get_meta_mut(&mut self) -> &mut HashMap<String, LayerDesc>;

    /// Get the order of the documents in the corpus
    fn get_order(&self) -> &Vec<String>;

    /// Add multiple documents to the corpus. This can be more efficient than
    /// calling add_doc multiple times as it may use a single DB transaction
    fn add_docs<D : IntoLayer, DC : DocumentContent<D>>(&mut self, content : Vec<DC>) -> TeangaResult<Vec<String>> {
        let mut ids = Vec::new();
        for doc in content {
            ids.push(self.add_doc(doc)?);
        }
        Ok(ids)
    }

    /// Calculate the frequency of words in the text layers of the corpus
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer to calculate the frequency of
    /// * `condition` - A condition that must be met for a word to be counted
    ///
    /// # Returns
    ///
    /// A map from words to their frequency
    fn text_freq<C: TextMatchCondition>(&self, layer : &str, condition : C) -> TeangaResult<HashMap<String, u32>> {
        let mut freq = HashMap::new();
        for doc_id in self.get_docs() {
            let doc = self.get_doc_by_id(&doc_id)?;
            let text = doc.text(layer, self.get_meta())?;
            for word in text {
                if condition.matches(word) {
                    *freq.entry(word.to_string()).or_insert(0) += 1;
                }
            }
        }
        Ok(freq)
    }

    /// Calculate the frequency of values in a data layer of the corpus
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer to calculate the frequency of
    /// * `condition` - A condition that must be met for a value to be counted
    ///
    /// # Returns
    ///
    /// A map from values to their frequency
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
    /// Iterate over all documents in the corpus
    fn iter_docs<'a>(&'a self) -> Box<dyn Iterator<Item=TeangaResult<Document>> + 'a> {
        Box::new(self.get_docs().into_iter().map(move |x| self.get_doc_by_id(&x)))
    }
    /// Iterate over all documents in the corpus with their IDs
    fn iter_doc_ids<'a>(&'a self) -> Box<dyn Iterator<Item=TeangaResult<(String, Document)>> + 'a> {
        Box::new(self.get_docs().into_iter().map(move |x| self.get_doc_by_id(&x).map(|d| (x, d))))
    }

    /// Search the corpus for documents that match a query
    ///
    /// # Arguments
    ///
    /// * `query` - The query to match
    ///
    /// # Returns
    ///
    /// An iterator of IDs and documents that match the query
    fn search<'a>(&'a self, query : Query) -> Box<dyn Iterator<Item=TeangaResult<(String, Document)>> + 'a> {
        Box::new(self.iter_doc_ids().filter(move |x| match x {
            Ok((_, doc)) => query.matches(doc, self.get_meta()),
            Err(_) => false
        }))
    }
}

/// A corpus where the metadata and order can be changed
pub trait WriteableCorpus : Corpus {
    /// Set the metadata of the corpus
    fn set_meta(&mut self, meta : HashMap<String, LayerDesc>);
    /// Set the order of the documents in the corpus
    fn set_order(&mut self, order : Vec<String>);
}


#[derive(Debug, Clone, PartialEq)]
/// An in-memory corpus object
pub struct SimpleCorpus {
    meta: HashMap<String, LayerDesc>,
    order: Vec<String>,
    content: HashMap<String, Document>
}

impl SimpleCorpus {
    /// Create an empty corpus
    pub fn new() -> SimpleCorpus {
        SimpleCorpus {
            meta: HashMap::new(),
            order: Vec::new(),
            content: HashMap::new(),
        }
    }

    /// Read the metadata from a YAML file
    pub fn read_yaml_header<'de, R: std::io::Read>(&mut self, r: R) -> Result<(), TeangaYamlError> {
        Ok(crate::serialization::read_yaml(r, self, true)?)
    }

}

impl Corpus for SimpleCorpus {
    type LayerStorage = Layer;
    type Content = Document;

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

    fn add_doc<D : IntoLayer, DC : DocumentContent<D>>(&mut self, content : DC) -> TeangaResult<String> {
        let doc = Document::new(content, &self.meta)?;
        let id = teanga_id(&self.order, &doc);
        self.order.push(id.clone());
        self.content.insert(id.clone(), doc);
        Ok(id)
    }

    fn update_doc<D : IntoLayer, DC: DocumentContent<D>>(&mut self, id : &str, content : DC) -> TeangaResult<String> {
        let doc = match self.get_doc_by_id(id) {
            Ok(mut doc) => {
                for (key, layer) in content {
                    let layer_desc = self.meta.get(&key).ok_or_else(|| TeangaError::ModelError(
                        format!("Layer {} does not exist", key)))?;
                    doc.set(&key, layer.into_layer(layer_desc)?);
                }
                doc
            },
            Err(TeangaError::DocumentNotFoundError) => Document::new(content, &self.meta)?,
            Err(e) => return Err(e)
        };
        let new_id = teanga_id_update(id, &self.order, &doc);
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
            None => Err(TeangaError::DocumentNotFoundError)
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
///
/// # Arguments
///
/// * `existing_keys` - The existing keys in the corpus
/// * `doc` - The document
///
/// # Returns
///
/// A unique ID for the document
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

/// Generate a new unique ID for a document. 
/// This is useful when updating a document
/// as it treats `prev_val` as if it did not occur in existing_keys.
///
/// # Arguments
///
/// * `prev_val` - The previous value of the ID
/// * `existing_keys` - The existing keys in the corpus
/// * `doc` - The document
///
/// # Returns
///
/// A unique ID for the document
pub fn teanga_id_update(prev_val : &str, existing_keys: &Vec<String>, doc : &Document) -> String {
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
    while *prev_val != code[..n] && existing_keys.contains(&code[..n].to_string()) && n < code.len() {
        n += 1;
    }
    return code[..n].to_string();
}

// TODO: Reorganize these functions into members of corpus

/// Read a corpus from a JSON string to a DB corpus
///
/// # Arguments
///
/// * `s` - The JSON string
/// * `path` - The path of the DB
pub fn read_corpus_from_json_string(s : &str, path : &str) -> Result<DiskCorpus, TeangaJsonError> {
    Ok(serialization::read_corpus_from_json_string(s, path)?)
}

/// Read a corpus from a YAML string to a DB corpus
///
/// # Arguments
///
/// * `s` - The YAML string
/// * `path` - The path of the DB
pub fn read_corpus_from_yaml_string(s : &str, path: &str) -> Result<DiskCorpus, TeangaYamlError> {
    Ok(serialization::read_corpus_from_yaml_string(s, path)?)
}

/// Read a corpys from a YAML string to a DB corpus
///
/// # Arguments
///
/// * `yaml` - The path of the YAML file
/// * `path` - The path of the DB
pub fn read_corpus_from_yaml_file(yaml : &str, path: &str) -> Result<DiskCorpus, TeangaYamlError> {
    Ok(serialization::read_corpus_from_yaml_file(yaml, path)?)
}

/// Read a corpus from a YAML URL to a DB corpus
///
/// # Arguments
///
/// * `url` - The URL of the YAML file
/// * `path` - The path of the DB
pub fn read_corpus_from_yaml_url(url : &str, path: &str) -> Result<DiskCorpus, TeangaYamlError> {
    Ok(serialization::read_corpus_from_yaml_url(url, path)?)
}

/// Write a corpus to a YAML file
///
/// # Arguments
///
/// * `corpus` - The corpus
/// * `path` - The path of the YAML file
pub fn write_corpus_to_yaml(corpus : &DiskCorpus, path : &str) -> Result<(), TeangaYamlError> {
    let f = File::create(path)?;
    Ok(serialization::pretty_yaml_serialize(corpus, f)?)
}

/// Write a corpus to a JSON file
///
/// # Arguments
///
/// * `corpus` - The corpus
/// * `path` - The path of the JSON file
pub fn write_corpus_to_json(corpus : &DiskCorpus, path : &str) -> Result<(), TeangaJsonError> {
    Ok(serialization::write_corpus_to_json(corpus, path)?)
}

/// Write a corpus to a JSON string
///
/// # Arguments
///
/// * `corpus` - The corpus
///
/// # Returns
///
/// The JSON string
pub fn write_corpus_to_json_string(corpus : &DiskCorpus) -> Result<String, TeangaJsonError> {
    Ok(serialization::write_corpus_to_json_string(corpus)?)
}

/// Write a corpus to a YAML string
///
/// # Arguments
///
/// * `corpus` - The corpus
///
/// # Returns
///
/// The YAML string
pub fn write_corpus_to_yaml_string(corpus : &DiskCorpus) -> Result<String, TeangaYamlError> {
    let mut v = Vec::new();
    serialization::pretty_yaml_serialize(corpus, &mut v)?;
    Ok(String::from_utf8(v)?)
}

/// Read a corpus from a JSON file to a DB corpus
///
/// # Arguments
///
/// * `json` - The path of the JSON file
/// * `path` - The path of the DB
pub fn read_corpus_from_json_file(json : &str, path: &str) -> Result<DiskCorpus, TeangaYamlError> {
    Ok(serialization::read_corpus_from_json_file(json, path)?)
}

/// An error type for Teanga
#[derive(Error, Debug)]
pub enum TeangaError {
    /// Errors from the DB
    #[error("DB read error: {0}")]
    DBError(#[from] sled::Error),
    /// Errors from DB Transactions
    #[error("DB transaction error: {0}")]
    DBTXError(#[from] sled::transaction::TransactionError<sled::Error>),
    /// Errors in serializing data
    #[error("Data error: {0}")]
    DataError(#[from] ciborium::ser::Error<std::io::Error>),
    /// Errors in deserializing data
    #[error("Data error: {0}")]
    DataError2(#[from] ciborium::de::Error<std::io::Error>),
    /// Errors due to string encoding
    #[error("Data read error: UTF-8 String could not be decoded")]
    UTFDataError,
    /// Errors with the data for the model
    #[error("Teanga model error: {0}")]
    ModelError(String),
    /// Errors in changing an immutable corpus
    #[error("TCF Corpora cannot be mutated")]
    TCFMutError,
    /// Errors readings a file
    #[error("TCF Read Error: {0}")]
    TCFReadError(#[from] crate::tcf::TCFError),
    /// A document does not exist in the corpus
    #[error("Document not found")]
    DocumentNotFoundError,
    /// The layer was not found in the document or meta
    #[error("Layer {0} does not exist")]
    LayerNotFoundError(String),
    /// An index between layers was out of bounds
    #[error("Indexing error for layer {0} targetting {0}")]
    IndexingError(String, String),
}

pub type TeangaResult<T> = Result<T, TeangaError>;

/// Errors when reading or writing JSON
#[derive(Error, Debug)]
pub enum TeangaJsonError {
    /// JSON format error
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
    /// Serde error
    #[error("Serialization error: {0}")]
    SerdeError(#[from] crate::serialization::SerializeError),
    /// Generic I/O error
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    /// Model or other error
    #[error("Teanga error: {0}")]
    TeangaError(#[from] TeangaError)
}

/// Errors when reading or writing YAML
#[derive(Error, Debug)]
pub enum TeangaYamlError {
    /// YAML format error
    #[error("YAML error: {0}")]
    YamlError(#[from] serde_yaml::Error),
    /// Generic I/O error
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    /// Error decoding a UTF-8 string
    #[error("UTF-8 error: {0}")]
    UTFError(#[from] std::string::FromUtf8Error),
    /// Errors from Serde
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

    #[test]
    fn test_update_doc() {
        let mut corpus = SimpleCorpus::new();
        corpus.add_layer_meta("text".to_string(), LayerType::characters, None, None, None, None, None, HashMap::new()).unwrap();
        corpus.add_layer_meta("words".to_string(), LayerType::span, Some("text".to_string()), None, None, None, None, HashMap::new()).unwrap();
        corpus.add_layer_meta("pos".to_string(), LayerType::seq, Some("words".to_string()), None, None, None, None, HashMap::new()).unwrap();
        let id = corpus.add_doc(vec![("text".to_string(), "test")]).unwrap();
        corpus.update_doc(&id, vec![("words".to_string(), vec![(0,1)])]).unwrap();
        corpus.update_doc(&id, vec![("pos".to_string(), vec!["N"])]).unwrap();
        let doc = corpus.get_doc_by_id(&id).unwrap();
        assert!(doc.get("words").is_some());
        assert!(doc.get("pos").is_some());

    }

}
