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
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
#[cfg(feature = "fjall")]
use fjall;
use itertools::Itertools;
#[cfg(feature = "redb")]
use redb;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(feature = "sled")]
use sled;
use std::collections::HashMap;
use thiserror::Error;

pub mod channel_corpus;
mod cuac;
#[cfg(any(feature = "sled", feature = "fjall", feature = "redb"))]
pub mod disk_corpus;
pub mod document;
pub mod layer;
pub mod layer_builder;
pub mod match_condition;
pub mod query;
pub mod serialization;

pub use cuac::{
    bytes_to_doc, doc_content_to_bytes, read_cuac, write_cuac, write_cuac_config, write_cuac_doc,
    write_cuac_header, write_cuac_with_config, CuacConfig, CuacReadError, CuacWriteError, Index,
    IndexResult, NoCompression, ShocoCompression, SmazCompression, StringCompression,
    StringCompressionError, StringCompressionMethod,
};
#[cfg(any(feature = "sled", feature = "fjall", feature = "redb"))]
pub use disk_corpus::{DiskCorpus, PathAsDB};
pub use document::{Document, DocumentBuilder, DocumentContent};
pub use layer::{DataType, IntoLayer, Layer, LayerDesc, LayerType, TeangaData};
pub use layer_builder::build_layer;
pub use match_condition::{DataMatchCondition, TextMatchCondition};
pub use query::Query;
pub use serialization::{
    read_json, read_json_with_config, read_jsonl, read_yaml, read_yaml_with_config, write_json,
    write_yaml, SerializationSettings,
};

/// Trait that defines a corpus according to the Teanga Data Model
pub trait Corpus: WriteableCorpus + ReadableCorpus {
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
    fn add_layer_meta(
        &mut self,
        name: String,
        layer_type: LayerType,
        base: Option<String>,
        data: Option<DataType>,
        link_types: Option<Vec<String>>,
        target: Option<String>,
        default: Option<Layer>,
        meta: HashMap<String, Value>,
    ) -> TeangaResult<()>;
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
    fn build_layer(&mut self, name: &str) -> crate::layer_builder::LayerBuilderImpl<Self>
    where
        Self: Sized,
    {
        build_layer(self, name)
    }
    /// Build a document using a builder
    ///
    /// # Returns
    ///
    /// A builder object
    fn build_doc<'a>(&'a mut self) -> DocumentBuilder<'a, Self>
    where
        Self: Sized,
    {
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
    fn update_doc<D: IntoLayer, DC: DocumentContent<D>>(
        &mut self,
        id: &str,
        content: DC,
    ) -> TeangaResult<String>;

    /// Remove a single document from the corpus
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    fn remove_doc(&mut self, id: &str) -> TeangaResult<()>;

    /// Get a document object by its ID
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    fn get_doc_by_id(&self, id: &str) -> TeangaResult<Document>;

    /// Get the IDs of all documents in the corpus
    fn get_docs(&self) -> Vec<String>;

    /// Clone the layer metadata
    fn clone_meta(&self) -> HashMap<String, LayerDesc> {
        self.get_meta().clone()
    }

    /// Get the order of the documents in the corpus
    fn get_order(&self) -> &Vec<String>;

    /// Add multiple documents to the corpus. This can be more efficient than
    /// calling add_doc multiple times as it may use a single DB transaction
    fn add_docs<D: IntoLayer, DC: DocumentContent<D>>(
        &mut self,
        content: Vec<DC>,
    ) -> TeangaResult<Vec<String>> {
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
    fn text_freq<C: TextMatchCondition>(
        &self,
        layer: &str,
        condition: C,
    ) -> TeangaResult<HashMap<String, u32>> {
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
    fn val_freq<C: DataMatchCondition>(
        &self,
        layer: &str,
        condition: C,
    ) -> TeangaResult<HashMap<TeangaData, u32>> {
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
    /// Search the corpus for documents that match a query
    ///
    /// If an index (see [`Corpus::create_index`]) exists for a layer
    /// referenced by an equality condition in the query, it is used to
    /// narrow down the documents that need to be checked instead of
    /// scanning the whole corpus.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to match
    ///
    /// # Returns
    ///
    /// An iterator of IDs and documents that match the query
    fn search<'a>(
        &'a self,
        query: Query,
    ) -> Box<dyn Iterator<Item = TeangaResult<(String, Document)>> + 'a>
    where
        Self: Sized,
    {
        match query.candidate_ids(self) {
            Some(ids) => Box::new(
                ids.into_iter()
                    .map(move |id| self.get_doc_by_id(&id).map(|doc| (id, doc)))
                    .filter(move |x| match x {
                        Ok((_, doc)) => query.matches(doc, self.get_meta()),
                        Err(_) => false,
                    }),
            ),
            None => Box::new(self.iter_doc_ids().filter(move |x| match x {
                Ok((_, doc)) => query.matches(doc, self.get_meta()),
                Err(_) => false,
            })),
        }
    }

    /// Get the estimated number of documents that match a query. This is an estimate
    /// that gives the maximum number of documents that could match the query, and
    /// so can be used to quickly estimate whether a query is likely to return any results without actually executing it.
    fn estimate_query_count(&self, query: Query) -> TeangaResult<usize>
    where
        Self: Sized,
    {
        match query.candidate_ids(self) {
            Some(ids) => Ok(ids.len()),
            None => Ok(self.get_docs().len()),
        }
    }

    /// Create an inverted index for a layer: a mapping from each distinct
    /// data value in the layer (as returned by [`Document::data`]) to the
    /// IDs of the documents that contain it. This is analogous to SQL's
    /// `CREATE INDEX` and can be used by [`Corpus::search`] to avoid
    /// scanning every document in the corpus for queries that test a
    /// layer's value for equality.
    ///
    /// # Arguments
    ///
    /// * `layer` - The name of the layer to index
    fn create_index(&mut self, layer: &str) -> TeangaResult<()>
    where
        Self: Sized,
    {
        if !self.get_meta().contains_key(layer) {
            return Err(TeangaError::LayerNotFoundError(layer.to_string()));
        }
        let mut index: HashMap<TeangaData, Vec<String>> = HashMap::new();
        for doc_id in self.get_docs() {
            let doc = self.get_doc_by_id(&doc_id)?;
            if let Some(data) = doc.data(layer, self.get_meta()) {
                let mut seen = std::collections::HashSet::new();
                for val in data {
                    if seen.insert(val.clone()) {
                        index
                            .entry(val)
                            .or_insert_with(Vec::new)
                            .push(doc_id.clone());
                    }
                }
            }
        }
        self.set_index(layer.to_string(), index)
    }

    /// Remove the index on a layer, if one exists. This does not fail if
    /// no index exists for the layer.
    ///
    /// # Arguments
    ///
    /// * `layer` - The name of the layer to remove the index for
    fn drop_index(&mut self, layer: &str) -> TeangaResult<()>;

    /// Check whether a layer currently has an index
    ///
    /// # Arguments
    ///
    /// * `layer` - The name of the layer to check
    fn has_index(&self, layer: &str) -> bool {
        self.get_index(layer).is_some()
    }

    /// Get the inverted index for a layer, if one has been created
    ///
    /// # Arguments
    ///
    /// * `layer` - The name of the layer to get the index for
    fn get_index(&self, layer: &str) -> Option<&HashMap<TeangaData, Vec<String>>>;

    /// Directly set the inverted index for a layer. This is used
    /// internally by [`Corpus::create_index`], but may also be used to
    /// install a precomputed index.
    ///
    /// # Arguments
    ///
    /// * `layer` - The name of the layer to set the index for
    /// * `index` - The mapping from data values to document IDs
    fn set_index(
        &mut self,
        layer: String,
        index: HashMap<TeangaData, Vec<String>>,
    ) -> TeangaResult<()>;

    /// Get a mutable reference to the inverted index for a layer, if one
    /// has been created. This is used internally to keep indexes in sync
    /// as documents are added, updated or removed.
    ///
    /// # Arguments
    ///
    /// * `layer` - The name of the layer to get the index for
    fn get_index_mut(&mut self, layer: &str) -> Option<&mut HashMap<TeangaData, Vec<String>>>;
}

/// Update all indexes to reflect a document that has just been added (or
/// whose content has changed). Only layers that already have an index are
/// affected.
pub(crate) fn index_add_doc<C: Corpus>(
    corpus: &mut C,
    id: &str,
    doc: &Document,
) -> TeangaResult<()> {
    let layers: Vec<String> = corpus
        .get_meta()
        .keys()
        .filter(|l| corpus.has_index(l))
        .cloned()
        .collect();
    for layer in layers {
        if let Some(data) = doc.data(&layer, corpus.get_meta()) {
            let mut seen = std::collections::HashSet::new();
            if let Some(index) = corpus.get_index_mut(&layer) {
                for val in data {
                    if seen.insert(val.clone()) {
                        index
                            .entry(val)
                            .or_insert_with(Vec::new)
                            .push(id.to_string());
                    }
                }
            }
        }
    }
    Ok(())
}

/// Update all indexes to remove a document that is about to be removed (or
/// whose previous content is being replaced). Only layers that already
/// have an index are affected.
pub(crate) fn index_remove_doc<C: Corpus>(
    corpus: &mut C,
    id: &str,
    doc: &Document,
) -> TeangaResult<()> {
    let layers: Vec<String> = corpus
        .get_meta()
        .keys()
        .filter(|l| corpus.has_index(l))
        .cloned()
        .collect();
    for layer in layers {
        if let Some(data) = doc.data(&layer, corpus.get_meta()) {
            if let Some(index) = corpus.get_index_mut(&layer) {
                for val in data {
                    if let Some(ids) = index.get_mut(&val) {
                        ids.retain(|x| x != id);
                    }
                }
            }
        }
    }
    Ok(())
}

/// A corpus where the metadata and order can be changed
pub trait WriteableCorpus {
    /// Set the metadata of the corpus
    fn set_meta(&mut self, meta: HashMap<String, LayerDesc>) -> TeangaResult<()>;
    /// Set the order of the documents in the corpus
    fn set_order(&mut self, order: Vec<String>) -> TeangaResult<()>;
    /// Add a document to this corpus
    ///
    /// # Arguments
    ///
    /// * `content` - The content of the document
    ///
    /// # Returns
    ///
    /// The ID of the document
    fn add_doc<D: IntoLayer, DC: DocumentContent<D>>(
        &mut self,
        content: DC,
    ) -> TeangaResult<String>;
}

pub trait ReadableCorpus {
    /// Iterate over all documents in the corpus
    fn iter_docs<'a>(&'a self) -> Box<dyn Iterator<Item = TeangaResult<Document>> + 'a>;
    /// Iterate over all documents in the corpus with their IDs
    fn iter_doc_ids<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = TeangaResult<(String, Document)>> + 'a>;
    /// Get the layer metadata
    fn get_meta(&self) -> &HashMap<String, LayerDesc>;
}

#[derive(Debug, Clone, PartialEq)]
/// An in-memory corpus object
pub struct SimpleCorpus {
    meta: HashMap<String, LayerDesc>,
    order: Vec<String>,
    content: HashMap<String, Document>,
    indexes: HashMap<String, HashMap<TeangaData, Vec<String>>>,
}

impl SimpleCorpus {
    /// Create an empty corpus
    pub fn new() -> SimpleCorpus {
        SimpleCorpus {
            meta: HashMap::new(),
            order: Vec::new(),
            content: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    /// Read the metadata from a YAML file
    pub fn read_yaml_header<'de, R: std::io::Read>(&mut self, r: R) -> Result<(), TeangaYamlError> {
        Ok(crate::serialization::read_yaml_with_config(
            r,
            self,
            SerializationSettings::new().header_only(),
        )?)
    }
}

impl Corpus for SimpleCorpus {
    fn add_layer_meta(
        &mut self,
        name: String,
        layer_type: LayerType,
        base: Option<String>,
        data: Option<DataType>,
        link_types: Option<Vec<String>>,
        target: Option<String>,
        default: Option<Layer>,
        meta: HashMap<String, Value>,
    ) -> TeangaResult<()> {
        self.meta.insert(
            name.clone(),
            LayerDesc {
                layer_type,
                base,
                data,
                link_types,
                target,
                default,
                meta,
            },
        );
        Ok(())
    }

    fn update_doc<D: IntoLayer, DC: DocumentContent<D>>(
        &mut self,
        id: &str,
        content: DC,
    ) -> TeangaResult<String> {
        let old_doc = self.content.get(id).cloned();
        let doc = match self.get_doc_by_id(id) {
            Ok(mut doc) => {
                for (key, layer) in content {
                    if key.starts_with("_") {
                        doc.set(&key, layer.into_meta_layer()?);
                    } else {
                        let layer_desc = self.meta.get(&key).ok_or_else(|| {
                            TeangaError::ModelError(format!(
                                "Layer {} is not described in meta",
                                key
                            ))
                        })?;
                        doc.set(&key, layer.into_layer(layer_desc)?);
                    }
                }
                doc
            }
            Err(TeangaError::DocumentNotFoundError) => Document::new(content, &self.meta)?,
            Err(e) => return Err(e),
        };
        let new_id = teanga_id_update(id, &self.order, &doc);
        if let Some(old_doc) = &old_doc {
            index_remove_doc(self, id, old_doc)?;
        }
        index_add_doc(self, &new_id, &doc)?;
        if id != new_id {
            let n = self.order.iter().position(|x| x == id).ok_or_else(|| {
                TeangaError::ModelError(format!("Cannot find document in order vector: {}", id))
            })?;
            self.order.remove(n);
            self.order.insert(n, new_id.clone());
            self.content.remove(id);
            self.content.insert(new_id.clone(), doc);
        } else {
            self.content.insert(id.to_string(), doc);
        }
        Ok(new_id)
    }

    fn remove_doc(&mut self, id: &str) -> TeangaResult<()> {
        if let Some(doc) = self.content.get(id).cloned() {
            index_remove_doc(self, id, &doc)?;
        }
        self.content.remove(id);
        self.order.retain(|x| x != id);
        Ok(())
    }

    fn get_doc_by_id(&self, id: &str) -> TeangaResult<Document> {
        match self.content.get(id) {
            Some(doc) => Ok(doc.clone()),
            None => Err(TeangaError::DocumentNotFoundError),
        }
    }

    fn get_docs(&self) -> Vec<String> {
        self.order.clone()
    }

    fn get_order(&self) -> &Vec<String> {
        &self.order
    }

    fn drop_index(&mut self, layer: &str) -> TeangaResult<()> {
        self.indexes.remove(layer);
        Ok(())
    }

    fn get_index(&self, layer: &str) -> Option<&HashMap<TeangaData, Vec<String>>> {
        self.indexes.get(layer)
    }

    fn set_index(
        &mut self,
        layer: String,
        index: HashMap<TeangaData, Vec<String>>,
    ) -> TeangaResult<()> {
        self.indexes.insert(layer, index);
        Ok(())
    }

    fn get_index_mut(&mut self, layer: &str) -> Option<&mut HashMap<TeangaData, Vec<String>>> {
        self.indexes.get_mut(layer)
    }
}

impl WriteableCorpus for SimpleCorpus {
    fn set_meta(&mut self, meta: HashMap<String, LayerDesc>) -> TeangaResult<()> {
        self.meta = meta;
        Ok(())
    }

    fn set_order(&mut self, order: Vec<String>) -> TeangaResult<()> {
        self.order = order;
        Ok(())
    }
    fn add_doc<D: IntoLayer, DC: DocumentContent<D>>(
        &mut self,
        content: DC,
    ) -> TeangaResult<String> {
        let doc = Document::new(content, &self.meta)?;
        let id = teanga_id(&self.order, &doc);
        self.order.push(id.clone());
        index_add_doc(self, &id, &doc)?;
        self.content.insert(id.clone(), doc);
        Ok(id)
    }
}

impl ReadableCorpus for SimpleCorpus {
    /// Iterate over all documents in the corpus
    fn iter_docs<'a>(&'a self) -> Box<dyn Iterator<Item = TeangaResult<Document>> + 'a> {
        Box::new(
            self.get_docs()
                .into_iter()
                .map(move |x| self.get_doc_by_id(&x)),
        )
    }
    /// Iterate over all documents in the corpus with their IDs
    fn iter_doc_ids<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = TeangaResult<(String, Document)>> + 'a> {
        Box::new(
            self.get_docs()
                .into_iter()
                .map(move |x| self.get_doc_by_id(&x).map(|d| (x, d))),
        )
    }

    fn get_meta(&self) -> &HashMap<String, LayerDesc> {
        &self.meta
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
/// Any valid JSON/YAML value
pub enum Value {
    Bool(bool),
    Int(i32),
    Float(f64),
    String(String),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
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
pub fn teanga_id(existing_keys: &Vec<String>, doc: &Document) -> String {
    let mut hasher = Sha256::new();
    for key in doc.content.keys().sorted() {
        match doc.content.get(key).unwrap() {
            Layer::Characters(val) => {
                hasher.update(key.as_bytes());
                hasher.update(vec![0u8]);
                hasher.update(val.as_bytes());
                hasher.update(vec![0u8]);
            }
            _ => (),
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
pub fn teanga_id_update(prev_val: &str, existing_keys: &Vec<String>, doc: &Document) -> String {
    let mut hasher = Sha256::new();
    for key in doc.content.keys().sorted() {
        match doc.content.get(key).unwrap() {
            Layer::Characters(val) => {
                hasher.update(key.as_bytes());
                hasher.update(vec![0u8]);
                hasher.update(val.as_bytes());
                hasher.update(vec![0u8]);
            }
            _ => (),
        }
    }
    let code = STANDARD.encode(hasher.finalize().as_slice());
    let mut n = 4;
    while *prev_val != code[..n] && existing_keys.contains(&code[..n].to_string()) && n < code.len()
    {
        n += 1;
    }
    return code[..n].to_string();
}

/// An error type for Teanga
#[derive(Error, Debug)]
pub enum TeangaError {
    /// Errors from the DB
    #[cfg(feature = "sled")]
    #[error("DB read error: {0}")]
    SledError(#[from] sled::Error),
    /// Errors from DB Transactions
    #[cfg(feature = "sled")]
    #[error("DB transaction error: {0}")]
    DBTXError(#[from] sled::transaction::TransactionError<sled::Error>),
    #[cfg(feature = "fjall")]
    #[error("DB read error: {0}")]
    FjallError(#[from] fjall::Error),
    /// Errors from the DB
    #[cfg(feature = "redb")]
    #[error("DB read error: {0}")]
    ReDBError(#[from] redb::DatabaseError),
    /// Errors from DB  transcation
    #[cfg(feature = "redb")]
    #[error("DB read error: {0}")]
    DBTransError(#[from] redb::TransactionError),
    /// Errors from DB table
    #[cfg(feature = "redb")]
    #[error("DB table error: {0}")]
    DBTableError(#[from] redb::TableError),
    /// Errors from DB storage
    #[cfg(feature = "redb")]
    #[error("DB storage error: {0}")]
    DBStorageError(#[from] redb::StorageError),
    /// Errors for DB commit
    #[cfg(feature = "redb")]
    #[error("DB commit error: {0}")]
    DBCommitError(#[from] redb::CommitError),
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
    #[error("Cuac Corpora cannot be mutated")]
    CuacMutError,
    /// Errors readings a file
    #[error("Cuac Read Error: {0}")]
    CuacReadError(#[from] crate::cuac::CuacError),
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
    TeangaError(#[from] TeangaError),
}

/// Errors when reading or writing YAML
#[derive(Error, Debug)]
pub enum TeangaYamlError {
    /// YAML format error
    #[error("YAML error: {0}")]
    YamlError(#[from] serde_yml::Error),
    /// Generic I/O error
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    /// Error decoding a UTF-8 string
    #[error("UTF-8 error: {0}")]
    UTFError(#[from] std::string::FromUtf8Error),
    /// Errors from Serde
    #[error("Serialization error: {0}")]
    SerdeError(#[from] crate::serialization::SerializeError),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_teanga_id_1() {
        let existing_keys = Vec::new();
        let doc = Document {
            content: vec![(
                "text".to_string(),
                Layer::Characters("This is a document.".to_string()),
            )]
            .into_iter()
            .collect(),
        };
        let expected = "Kjco";
        assert_eq!(teanga_id(&existing_keys, &doc), expected);
    }

    #[test]
    fn test_teanga_id_2() {
        let existing_keys = Vec::new();
        let doc = Document {
            content: vec![
                (
                    "text".to_string(),
                    Layer::Characters("This is a document.".to_string()),
                ),
                ("fileid".to_string(), Layer::Characters("doc1".to_string())),
            ]
            .into_iter()
            .collect(),
        };
        let expected = "fexV";
        assert_eq!(teanga_id(&existing_keys, &doc), expected);
    }

    #[test]
    fn test_serialize_layer() {
        let layer = Layer::L1S(vec![(1, "a".to_string()), (2, "b".to_string())]);
        let s = serde_json::to_string(&layer).unwrap();
        assert_eq!(s, r#"[[1,"a"],[2,"b"]]"#);
        let layer2: Layer = serde_json::from_str(&s).unwrap();
        assert_eq!(layer, layer2);
        let layer3 = Layer::L1(vec![0]);
        let layer4: Layer = serde_json::from_str("[0]").unwrap();
        assert_eq!(layer3, layer4);
    }

    #[test]
    fn test_update_doc() {
        let mut corpus = SimpleCorpus::new();
        corpus
            .add_layer_meta(
                "text".to_string(),
                LayerType::characters,
                None,
                None,
                None,
                None,
                None,
                HashMap::new(),
            )
            .unwrap();
        corpus
            .add_layer_meta(
                "words".to_string(),
                LayerType::span,
                Some("text".to_string()),
                None,
                None,
                None,
                None,
                HashMap::new(),
            )
            .unwrap();
        corpus
            .add_layer_meta(
                "pos".to_string(),
                LayerType::seq,
                Some("words".to_string()),
                None,
                None,
                None,
                None,
                HashMap::new(),
            )
            .unwrap();
        let id = corpus.add_doc(vec![("text".to_string(), "test")]).unwrap();
        corpus
            .update_doc(&id, vec![("words".to_string(), vec![(0, 1)])])
            .unwrap();
        corpus
            .update_doc(&id, vec![("pos".to_string(), vec!["N"])])
            .unwrap();
        let doc = corpus.get_doc_by_id(&id).unwrap();
        assert!(doc.get("words").is_some());
        assert!(doc.get("pos").is_some());
    }
}
