// Purpose: Rust implementation of the TeangaDB Python module.
// Author: John P. McCrae
// License: Apache 2.0
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use sled;
use ciborium::{from_reader, into_writer};
use sha2::{Digest, Sha256};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use itertools::Itertools;
use serde::{Serialize,Deserialize};
use thiserror::Error;
use std::fs::File;

pub mod serialization;
pub mod layer_builder;

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
        target: Option<String>, default: Option<RawLayer>,
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
    fn into_layer<F : StringIndex>(self, meta : &LayerDesc, str2idx : &mut F) -> TeangaResult<Layer>;
}

impl IntoLayer for Layer {
    fn into_layer<F : StringIndex>(self, _meta : &LayerDesc, _str2idx : &mut F) -> TeangaResult<Layer> {
        Ok(self)
    }
}

impl IntoLayer for String {
    fn into_layer<F: StringIndex>(self, _meta : &LayerDesc, _str2idx : &mut F) -> TeangaResult<Layer> {
        Ok(Layer::Characters(self))
    }
}

impl IntoLayer for &str {
fn into_layer<F: StringIndex>(self, _meta : &LayerDesc, _str2idx : &mut F) -> TeangaResult<Layer> {
        Ok(Layer::Characters(self.to_string()))
    }
}


pub trait StringIndex {
    fn get_id(&mut self, s : &str) -> u32;
    fn get_str(&mut self, id : u32) -> String;
}

#[derive(Debug,Clone)]
/// A corpus object
pub struct DiskCorpus {
    pub meta: HashMap<String, LayerDesc>,
    pub order: Vec<String>,
    path: String
}

#[derive(Debug, Clone)]
/// An in-memory corpus object
pub struct SimpleCorpus {
    meta: HashMap<String, LayerDesc>,
    order: Vec<String>,
    content: HashMap<String, Document>,
    str2idx: HashMap<String, u32>,
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
    pub default: Option<RawLayer>,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub meta: HashMap<String, Value>, 
}


impl DiskCorpus {
    /// Create a new corpus
    ///
    /// # Arguments
    /// * `path` - The path to the database
    ///
    /// # Returns
    /// A new corpus object
    ///
    pub fn new(path : &str) -> TeangaResult<DiskCorpus> {
        let db = open_db(path)?;
        let mut meta = HashMap::new();
        for m in db.scan_prefix(&[META_PREFIX]) {
            let (name, v) = m.map_err(|e| TeangaError::DBError(e))?;
            let layer_desc = from_bytes::<LayerDesc>(v.as_ref())?;
            let name = std::str::from_utf8(name[1..].as_ref())
                .map_err(|_| TeangaError::UTFDataError)?.to_string();
            meta.insert(name, layer_desc);
        }
        let order = match db.get(ORDER_BYTES.to_vec())
            .map_err(|e| TeangaError::DBError(e))? {
            Some(bytes) => from_bytes::<Vec<String>>(bytes.as_ref())?,
            None => Vec::new()
        };
        Ok(DiskCorpus {
            meta,
            order,
            path: path.to_string()
        })
    }

    pub fn add_layer_meta(&mut self, name: String, layer_type: LayerType, 
        base: Option<String>, data: Option<DataType>, link_types: Option<Vec<String>>, 
        target: Option<String>, default: Option<RawLayer>,
        meta: HashMap<String, Value>) -> TeangaResult<()> {
        CorpusTransaction::new(self)?.add_layer_meta(name, layer_type, base, data, link_types, target, default, meta)
    }

    pub fn transaction<'a>(&'a mut self) -> TeangaResult<impl Corpus + 'a> {
        CorpusTransaction::new(self)
    }
}


impl Corpus for DiskCorpus {
    type LayerStorage = RawLayer;
    type Content = HashMap<String, RawLayer>;

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
        target: Option<String>, default: Option<RawLayer>,
        meta: HashMap<String, Value>) -> TeangaResult<()> {
        CorpusTransaction::new(self)?.add_layer_meta(name, layer_type, base, data, link_types, target, default, meta)
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
        CorpusTransaction::new(self)?.add_doc(content)
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
    fn update_doc<D : IntoLayer, DC : DocumentContent<D>>(&mut self, id : &str, content : DC) -> TeangaResult<String> {
        CorpusTransaction::new(self)?.update_doc(id, content)
    }

 
    /// Remove a document from the corpus
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    fn remove_doc(&mut self, id : &str) -> TeangaResult<()> {
        CorpusTransaction::new(self)?.remove_doc(id)
    }

    /// Get a document by its ID
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    ///
    /// # Returns
    ///
    /// The document as a map from layers names to layers
    fn get_doc_by_id(&self, id : &str) -> TeangaResult<HashMap<String, RawLayer>> {
        let db = open_db(&self.path)?;
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(id.as_bytes());
        let data = db.get(id_bytes)
            .map_err(|e| TeangaError::DBError(e))?
            .ok_or_else(|| TeangaError::ModelError(
                format!("Document not found")))?;
        let doc = from_bytes::<Document>(data.as_ref())?;
        let mut result = HashMap::new();
        for (key, layer) in doc.content {
            let layer_desc : &LayerDesc = self.meta.get(&key).
                    ok_or_else(|| TeangaError::ModelError(
                        format!("Serialized document contains undeclared layer {}", key)))?;
            result.insert(key, layer.into_raw(
                    layer_desc,
                    &|u| {
                        let mut id_bytes = Vec::new();
                        id_bytes.push(ID2STR_PREFIX);
                        id_bytes.extend(u.to_be_bytes());
                        String::from_utf8(
                            db.get(id_bytes)
                            .expect("Error reading string index")
                            .unwrap_or_else(|| panic!("String index not found"))
                            .as_ref().to_vec())
                            .expect("Unicode error in string index")
                    })?);

        }
        Ok(result)
    }

    /// Get the documents in the corpus
    ///
    /// # Returns
    ///
    /// The documents IDs in order
    fn get_docs(&self) -> Vec<String> {
        self.order.clone()
    }

    /// Get the meta information for the corpus
    ///
    /// # Returns
    ///
    /// The meta information for the corpus
    fn get_meta(&self) -> &HashMap<String, LayerDesc> {
        &self.meta
    }

    /// Get the meta information for the corpus
    ///
    /// # Returns
    ///
    /// The meta information for the corpus
    fn get_meta_mut(&mut self) -> &mut HashMap<String, LayerDesc> {
        &mut self.meta
    }


    /// Get the order of the documents in the corpus
    ///
    /// # Returns
    ///
    /// The order of the documents in the corpus
    fn get_order(&self) -> &Vec<String> {
        &self.order
    }
}

impl SimpleCorpus {
    pub fn new() -> SimpleCorpus {
        SimpleCorpus {
            meta: HashMap::new(),
            order: Vec::new(),
            content: HashMap::new(),
            str2idx: HashMap::new()
        }
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
        target: Option<String>, default: Option<RawLayer>,
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
        for key in content.keys() {
            if !self.meta.contains_key(&key) {
                return Err(TeangaError::ModelError(
                    format!("Layer {} does not exist", key)))
            }
        }
        let mut doc_content = HashMap::new();
        for (k, v) in content {
            doc_content.insert(k.clone(), v.into_layer(&self.meta[&k].clone(), self)?);
        }
        let doc = Document::new(doc_content);
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
        for key in content.keys() {
            if !self.meta.contains_key(&key) {
                return Err(TeangaError::ModelError(
                    format!("Layer {} does not exist", key)))
            }
        }
        let mut doc_content = HashMap::new();
        for (k, v) in content {
            doc_content.insert(k.clone(), v.into_layer(&self.meta[&k].clone(), self)?);
        }
        let doc = Document::new(doc_content);
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

impl StringIndex for SimpleCorpus {
    fn get_id(&mut self, u : &str) -> u32 {
        match self.str2idx.get(u) {
            Some(id) => *id,
            None => {
                let id = self.str2idx.len() as u32;
                self.str2idx.insert(u.to_string(), id);
                id
            }
        }
    }

    fn get_str(&mut self, id : u32) -> String {
        self.str2idx.iter().find(|(_, &v)| v == id).unwrap().0.clone()
    }
}

/// A corpus with an open database connection to implement multiple changes
/// without closing the database
struct CorpusTransaction<'a> {
    corpus : &'a mut DiskCorpus, 
    db : sled::Db
}

impl<'a> CorpusTransaction<'a> {
    /// Connect to the database
    fn new(corpus : &'a mut DiskCorpus) -> TeangaResult<CorpusTransaction> {
        let db = open_db(&corpus.path)?;
        Ok(CorpusTransaction {
            corpus,
            db
        })
    }

    fn set_meta(&mut self, meta : HashMap<String, LayerDesc>) -> TeangaResult<()> {
        self.corpus.meta = meta;
        for (name, layer_desc) in self.corpus.meta.iter() {
            let mut id_bytes = Vec::new();
            id_bytes.push(META_PREFIX);
            id_bytes.extend(name.clone().as_bytes());
            self.db.insert(id_bytes, to_stdvec(&layer_desc)?).
                map_err(|e| TeangaError::DBError(e))?;
        }
        Ok(())
    }

    fn set_order(&mut self, order : Vec<String>) -> TeangaResult<()> {
        self.corpus.order = order;
        self.db.insert(ORDER_BYTES.to_vec(), to_stdvec(&self.corpus.order)?).
            map_err(|e| TeangaError::DBError(e))?;
        Ok(())
    }

}

impl<'a> Corpus for CorpusTransaction<'a> {
    type LayerStorage = RawLayer;
    type Content = HashMap<String, RawLayer>;

    /// Add a layer to the corpus
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the layer
    /// * `layer_type` - The type of the layer
    /// * `base` - The layer that this layer is on
    /// * `data` - The data file for this layer
    /// * `link_types` - The types for links in this layer
    /// * `target` - The target layer for this layer
    /// * `default` - The default values for this layer
    /// * `meta` - The metadata for this layer
    fn add_layer_meta(&mut self, name: String, layer_type: LayerType, 
        base: Option<String>, data: Option<DataType>, link_types: Option<Vec<String>>, 
        target: Option<String>, default: Option<RawLayer>,
        meta: HashMap<String, Value>) -> TeangaResult<()> {
        if layer_type == LayerType::characters && base != Some("".to_string()) && base != None {
            return Err(TeangaError::ModelError(
                format!("Layer {} of type characters cannot be based on another layer", name)))
        }

        if layer_type != LayerType::characters && (base == Some("".to_string()) || base == None) {
            return Err(TeangaError::ModelError(
                format!("Layer {} of type {} must be based on another layer", name, layer_type)))
        }

        let layer_desc = LayerDesc {
            layer_type,
            base,
            data,
            link_types,
            target,
            default,
            meta
         };

        let mut id_bytes = Vec::new();
        id_bytes.push(META_PREFIX);
        id_bytes.extend(name.clone().as_bytes());
        self.db.insert(id_bytes, to_stdvec(&layer_desc)?)
            .map_err(|e| TeangaError::DBError(e))?;

        self.corpus.meta.insert(name.clone(), layer_desc);
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
        for key in content.keys() {
            if !self.corpus.meta.contains_key(&key) {
                return Err(TeangaError::ModelError(
                    format!("Layer {} does not exist", key)))
            }
        }
        let mut doc_content = HashMap::new();
        for (k, v) in content {
            let layer_meta = self.corpus.meta.get(&k).ok_or_else(|| TeangaError::ModelError(
                format!("No meta information for layer {}", k)))?;
            doc_content.insert(k, 
                v.into_layer(layer_meta, &mut self.db)?);
        }
        let doc = Document::new(doc_content);
        let id = teanga_id(&self.corpus.order, &doc);
    
        self.corpus.order.push(id.clone());

        self.db.insert(ORDER_BYTES.to_vec(), to_stdvec(&self.corpus.order)?)
            .map_err(|e| TeangaError::DBError(e))?;

        let data = to_stdvec(&doc)?;
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(id.as_bytes());
        self.db.insert(id_bytes, data).map_err(|e| TeangaError::DBError(e))?;
        Ok(id)
    }

    fn add_docs<D : IntoLayer, DC : DocumentContent<D>>(&mut self, contents : Vec<DC>) -> TeangaResult<Vec<String>> {
        let mut docs = Vec::new();
        let mut ids = Vec::new();
        for content in contents {
            for key in content.keys() {
                if !self.corpus.meta.contains_key(&key) {
                    return Err(TeangaError::ModelError(
                        format!("Layer {} does not exist", key)))
                }
            }

            let mut doc_content = HashMap::new();
            for (k, v) in content {
                let layer_meta = self.corpus.meta.get(&k).ok_or_else(|| TeangaError::ModelError(
                    format!("No meta information for layer {}", k)))?;
                doc_content.insert(k, 
                    v.into_layer(layer_meta, &mut self.db)?);
            }
            let doc = Document::new(doc_content);
            let id = teanga_id(&self.corpus.order, &doc);
        
            self.corpus.order.push(id.clone());
            docs.push((id.clone(), to_stdvec(&doc)?));
            ids.push(id);
        }

        let order_bytes = to_stdvec(&self.corpus.order)?;

        self.db.transaction(move |tx| {
            tx.insert(ORDER_BYTES.to_vec(), order_bytes.clone())?;
            for (id, data) in &docs {


                let mut id_bytes = Vec::new();
                id_bytes.push(DOCUMENT_PREFIX);
                id_bytes.extend(id.as_bytes());
                tx.insert(id_bytes, data.clone())?;
            }
            Ok(())
        }).map_err(|e| TeangaError::DBTXError(e))?;
        Ok(ids)
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
    fn update_doc<D : IntoLayer, DC : DocumentContent<D>>(&mut self, id : &str, content : DC) -> TeangaResult<String> {
        for key in content.keys() {
            if !self.corpus.meta.contains_key(&key) {
                return Err(TeangaError::ModelError(
                    format!("Layer {} does not exist", key)))
            }
        }

        let mut doc_content = HashMap::new();
        for (k, v) in content {
            let layer_meta = self.corpus.meta.get(&k).ok_or_else(|| TeangaError::ModelError(
                format!("No meta information for layer {}", k)))?;
            doc_content.insert(k, 
                v.into_layer(layer_meta, &mut self.db)?);
        }

        let doc = Document::new(doc_content);
        let new_id = teanga_id(&self.corpus.order, &doc);
        if id != new_id {
        
            let n = self.corpus.order.iter().position(|x| x == id).ok_or_else(|| TeangaError::ModelError(
                format!("Cannot find document in order vector: {}", id)))?;
            self.corpus.order.remove(n);
            self.corpus.order.insert(n, new_id.clone());

            self.db.insert(ORDER_BYTES.to_vec(), to_stdvec(&self.corpus.order)?).
                map_err(|e| TeangaError::DBError(e))?;

            let mut old_id_bytes = Vec::new();
            old_id_bytes.push(DOCUMENT_PREFIX);
            old_id_bytes.extend(id.as_bytes());
            self.db.remove(old_id_bytes).map_err(|e| TeangaError::ModelError(
                format!("Cannot remove document {}", e)))?;
        }

        let data = to_stdvec(&doc)?;
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(new_id.as_bytes());
        self.db.insert(id_bytes, data).map_err(|e| TeangaError::DBError(e))?;
        Ok(new_id)
    }

 
    /// Remove a document from the corpus
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    fn remove_doc(&mut self, id : &str) -> TeangaResult<()> {
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(id.as_bytes());
        self.db.remove(id_bytes).map_err(|e| TeangaError::ModelError(
            format!("Cannot remove document {}", e)))?;
        self.corpus.order.retain(|x| x != id);
        self.db.insert(ORDER_BYTES.to_vec(), to_stdvec(&self.corpus.order)?).
            map_err(|e| TeangaError::DBError(e))?;
        Ok(())
    }

    fn get_doc_by_id(&self, id : &str) -> TeangaResult<HashMap<String, RawLayer>> {
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(id.as_bytes());
        let data = self.db.get(id_bytes)
            .map_err(|e| TeangaError::DBError(e))?
            .ok_or_else(|| TeangaError::ModelError(
                format!("Document not found")))?;
        let doc = from_bytes::<Document>(data.as_ref())?;
        let mut result = HashMap::new();
        for (key, layer) in doc.content {
            let layer_desc : &LayerDesc = self.corpus.meta.get(&key).
                    ok_or_else(|| TeangaError::ModelError(
                        format!("Serialized document contains undeclared layer {}", key)))?;
            result.insert(key, layer.into_raw(
                    layer_desc,
                    &|u| {
                        let mut id_bytes = Vec::new();
                        id_bytes.push(ID2STR_PREFIX);
                        id_bytes.extend(u.to_be_bytes());
                        String::from_utf8(
                            self.db.get(id_bytes)
                            .expect("Error reading string index")
                            .unwrap_or_else(|| panic!("String index not found"))
                            .as_ref().to_vec())
                            .expect("Unicode error in string index")
                    })?);

        }
        Ok(result)
    }

    /// Get the documents in the corpus
    ///
    /// # Returns
    ///
    /// The documents IDs in order
    fn get_docs(&self) -> Vec<String> {
        self.corpus.get_docs()
    }

    /// Get the meta information for the corpus
    ///
    /// # Returns
    ///
    /// The meta information for the corpus
    fn get_meta(&self) -> &HashMap<String, LayerDesc> {
        self.corpus.get_meta()
    }

    /// Get the meta information for the corpus
    ///
    /// # Returns
    ///
    /// The meta information for the corpus
    fn get_meta_mut(&mut self) -> &mut HashMap<String, LayerDesc> {
        self.corpus.get_meta_mut()
    }


    /// Get the order of the documents in the corpus
    ///
    /// # Returns
    ///
    /// The order of the documents in the corpus
    fn get_order(&self) -> &Vec<String> {
        self.corpus.get_order()
    }

}

impl StringIndex for sled::Db {
    fn get_id(&mut self, u : &str) -> u32 {
        let mut id_bytes = Vec::new();
        id_bytes.push(STR2ID_PREFIX);
        id_bytes.extend(u.as_bytes());
        match self.get(id_bytes).expect("Error reading string index") {
            Some(b) => {
                if b.len() != 4 {
                    panic!("String index is not 4 bytes");
                }
                u32::from_be_bytes(b.as_ref().try_into().unwrap())
            },
            None => {
                gen_next_id(&self, u)
            }
        }
    }

    fn get_str(&mut self, id : u32) -> String {
        let mut id_bytes = Vec::new();
        id_bytes.push(ID2STR_PREFIX);
        id_bytes.extend(id.to_be_bytes());
        String::from_utf8(
            self.get(id_bytes)
            .expect("Error reading string index")
            .unwrap_or_else(|| panic!("String index not found"))
            .as_ref().to_vec())
            .expect("Unicode error in string index")
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

fn gen_next_id(db : &sled::Db, u : &str) -> u32 {
    let mut n = db.get(STRIDX_SIZE.to_vec())
        .expect("Error reading string index size")
        .map(|b| {
            if b.len() != 4 {
                panic!("String index size is not 4 bytes");
            }
            u32::from_be_bytes(b.as_ref().try_into().unwrap())
        }).unwrap_or(0);

    while let Err(_) = db.compare_and_swap(STRIDX_SIZE.to_vec(), 
        if n == 0 { None } else { Some(n.to_be_bytes().to_vec()) }, 
        Some((n+1).to_be_bytes().to_vec()))
        .expect("Error reading DB") {
        n = db.get(STRIDX_SIZE.to_vec())
            .expect("Error reading string index size")
            .map(|b| {
                if b.len() != 4 {
                    panic!("String index size is not 4 bytes");
                }
                u32::from_be_bytes(b.as_ref().try_into().unwrap())
            }).unwrap_or(0);
    }
    n = n + 1;
    let mut id_bytes = Vec::new();
    id_bytes.push(STR2ID_PREFIX);
    id_bytes.extend(u.as_bytes());
    db.insert(id_bytes, n.to_be_bytes().to_vec()).expect("Error reading DB");
    let mut id_bytes = Vec::new();
    id_bytes.push(ID2STR_PREFIX);
    id_bytes.extend(n.to_be_bytes().to_vec());
    db.insert(id_bytes, u.as_bytes().to_vec()).expect("Error reading DB");
    n
}

#[derive(Debug,Clone,Serialize,Deserialize)]
/// A document object
pub struct Document {
    content: HashMap<String, Layer>
}

impl Document {
    fn new(content : HashMap<String, Layer>) -> Document {
        Document {
            content
        }
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

#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum Layer {
    Characters(String),
    Seq(Vec<u32>),
    Div(Vec<(u32,u32)>),
    DivNoData(Vec<u32>),
    Element(Vec<(u32,u32)>),
    ElementNoData(Vec<u32>),
    Span(Vec<(u32,u32,u32)>),
    SpanNoData(Vec<(u32,u32)>),
    MetaLayer(Vec<HashMap<String, Value>>)
}

impl Layer {
    fn into_raw<F>(&self, meta : &LayerDesc, idx2str : &F) -> TeangaResult<RawLayer> 
        where F : Fn(u32) -> String {
        match self {
            Layer::Characters(val) => Ok(RawLayer::CharacterLayer(val.clone())),
            Layer::Seq(val) => {
                match meta.data {
                    None => Err(TeangaError::ModelError(
                        format!("Layer contains data but not data type"))),
                    Some(DataType::String) => {
                        let mut result = Vec::new();
                        for id in val {
                            result.push(u32_into_raw_str(*id, &DataType::String, idx2str)?);
                        }
                        Ok(RawLayer::LS(result))
                    },
                    Some(DataType::Enum(ref vals)) => {
                        let mut result = Vec::new();
                        for id in val {
                            result.push(vals[*id as usize].clone());
                        }
                        Ok(RawLayer::LS(result))
                    },
                    Some(DataType::Link) => {
                        match meta.link_types {
                            None => Ok(RawLayer::L1(val.clone())),
                            Some(ref vals) => {
                                let mut result = Vec::new();
                                for id in val {
                                    result.push(u32_into_raw_u32_str(*id, &vals)?);
                                }
                                Ok(RawLayer::L1S(result))
                            }
                        }
                    }
                }
            },
            Layer::Div(val) => {
                match meta.data {
                    None => Err(TeangaError::ModelError(
                        format!("Layer contains data but no data type"))),
                    Some(DataType::String) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            result.push((*start, u32_into_raw_str(*data, &DataType::String, idx2str)?));
                        }
                        Ok(RawLayer::L1S(result))
                    },
                    Some(DataType::Enum(ref vals)) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            result.push((*start, vals[*data as usize].clone()));
                        }
                        Ok(RawLayer::L1S(result))
                    },
                    Some(DataType::Link) => {
                        match meta.link_types {
                            None => { Ok(RawLayer::L2(val.clone())) },
                            Some(ref vals) => {
                                let mut result = Vec::new();
                                for (start, data) in val {
                                    let tl = u32_into_raw_u32_str(*data, &vals)?;
                                    result.push((*start, tl.0, tl.1));
                                }
                                Ok(RawLayer::L2S(result))
                            }
                        }
                    }
                }
            },
            Layer::Element(val) => {
                match meta.data {
                    None => Err(TeangaError::ModelError(
                        format!("Layer contains data but no data type"))),
                    Some(DataType::String) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            result.push((*start, u32_into_raw_str(*data, &DataType::String, idx2str)?));
                        }
                        Ok(RawLayer::L1S(result))
                    },
                    Some(DataType::Enum(ref vals)) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            result.push((*start, vals[*data as usize].clone()));
                        }
                        Ok(RawLayer::L1S(result))
                    },
                    Some(DataType::Link) => {
                        match meta.link_types {
                            None => { Ok(RawLayer::L2(val.clone())) },
                            Some(ref vals) => {
                                let mut result = Vec::new();
                                for (start, data) in val {
                                    let tl = u32_into_raw_u32_str(*data, &vals)?;
                                    result.push((*start, tl.0, tl.1));
                                }
                                Ok(RawLayer::L2S(result))
                            }
                        }
                    }
                }
            },
            Layer::Span(val) => {
                match meta.data {
                    None => Err(TeangaError::ModelError(
                        format!("Layer contains data but no data type"))),
                    Some(DataType::String) => {
                        let mut result = Vec::new();
                        for (start, end, data) in val {
                            result.push((*start, *end, u32_into_raw_str(*data, &DataType::String, idx2str)?));
                        }
                        Ok(RawLayer::L2S(result))
                    },
                    Some(DataType::Enum(ref vals)) => {
                        let mut result = Vec::new();
                        for (start, end, data) in val {
                            result.push((*start, *end, vals[*data as usize].clone()));
                        }
                        Ok(RawLayer::L2S(result))
                    },
                    Some(DataType::Link) => {
                        match meta.link_types {
                            None => { Ok(RawLayer::L3(val.clone())) },
                            Some(ref vals) => {
                                let mut result = Vec::new();
                                for (start, end, data) in val {
                                    let tl = u32_into_raw_u32_str(*data, &vals)?;
                                    result.push((*start, *end, tl.0, tl.1));
                                }
                                Ok(RawLayer::L3S(result))
                            }
                        }
                    }
                }
            },
            Layer::DivNoData(val) => {
                let mut result = Vec::new();
                for start in val {
                    result.push(*start);
                }
                Ok(RawLayer::L1(result))
            },
            Layer::ElementNoData(val) => {
                let mut result = Vec::new();
                for start in val {
                    result.push(*start);
                }
                Ok(RawLayer::L1(result))
            },
            Layer::SpanNoData(val) => {
                let mut result = Vec::new();
                for (start, end) in val {
                    result.push((*start, *end));
                }
                Ok(RawLayer::L2(result))
            },
            Layer::MetaLayer(val) => {
                Ok(RawLayer::MetaLayer(val.clone()))
            }
        }
    }
}

impl IntoLayer for RawLayer {

    fn into_layer<F : StringIndex>(self, meta : &LayerDesc, str2idx : &mut F) -> TeangaResult<Layer> {
        match self {
            RawLayer::CharacterLayer(val) => Ok(Layer::Characters(val)),
            RawLayer::L1(val) => {
                match meta.data {
                    Some(_) => {
                        Ok(Layer::Seq(val))
                    },
                    None => {
                        match meta.layer_type {
                            LayerType::div => Ok(Layer::DivNoData(val)),
                            LayerType::element => Ok(Layer::ElementNoData(val)),
                            _ => Err(TeangaError::ModelError(
                                format!("Cannot convert data layer to {}", meta.layer_type)))
                        }
                    }
                }
            },
            RawLayer::L2(val) => {   
                match meta.data {
                    Some(_) => {
                        match meta.layer_type {
                            LayerType::div => Ok(Layer::Div(val)),
                            LayerType::element => Ok(Layer::Element(val)),
                            _ => Err(TeangaError::ModelError(
                                format!("Cannot convert data layer to {}", meta.layer_type)))
                        }
                    },
                    None => {
                        Ok(Layer::SpanNoData(val))
                    }
                }
            },
            RawLayer::L3(val) => {
                Ok(Layer::Span(val))
            },
            RawLayer::LS(val) => {
                let mut result = Vec::new();
                for data in val {
                    result.push(raw_str_into_u32(&data, &DataType::String, str2idx)?);
                }
                Ok(Layer::Seq(result))
            },
            RawLayer::L1S(val) => {
                match meta.data {
                    Some(DataType::Link) if meta.link_types.is_some() => {
                        let mut result = Vec::new();
                        for (idx, link) in val {
                            result.push(raw_u32_str_into_u32(idx, link, &meta.link_types.as_ref().unwrap())?);
                        }
                        Ok(Layer::Seq(result))
                    },
                    Some(ref metadata) => {
                        let mut result = Vec::new();
                        for (start, data) in val {
                            result.push((start, raw_str_into_u32(&data, metadata, str2idx)?));
                        }
                        match meta.layer_type {
                            LayerType::div => Ok(Layer::Div(result)),
                            LayerType::element => Ok(Layer::Element(result)),
                            _ => Err(TeangaError::ModelError(
                                format!("Cannot convert data layer to {}", meta.layer_type)))
                        }
                    },
                    None => Err(TeangaError::ModelError(
                        format!("String in data, but data type is none")))
                }
            },
            RawLayer::L2S(val) => {
                let metadata = meta.data.as_ref().ok_or_else(|| TeangaError::ModelError(
                    format!("Cannot convert data layer to {}", meta.layer_type)))?;
                match meta.data {
                    Some(DataType::Link) => {
                        if let Some(ref vals) = meta.link_types {
                            let mut result = Vec::new();
                            for (start, idx, link) in val {
                                result.push((start, raw_u32_str_into_u32(idx, link, vals)?));
                            }
                            match meta.layer_type {
                                LayerType::div => Ok(Layer::Div(result)),
                                LayerType::element => Ok(Layer::Element(result)),
                                _ => Err(TeangaError::ModelError(
                                    format!("Cannot convert data layer to {}", meta.layer_type)))
                            }
                        } else {
                            let mut result = Vec::new();
                            for (start, end, data) in val {
                                result.push((start, end, raw_str_into_u32(&data, metadata, str2idx)?));
                            }
                            Ok(Layer::Span(result))
                        }
 
                    },
                    _ => {
                        let mut result = Vec::new();
                        for (start, end, data) in val {
                            result.push((start, end, raw_str_into_u32(&data, metadata, str2idx)?));
                        }
                        Ok(Layer::Span(result))
                    }
                }
            },
            RawLayer::L3S(val) => {
                let mut result = Vec::new();
                for (start, end, idx, link) in val {
                    if let Some(ref vals) = meta.link_types {
                        result.push((start, end, raw_u32_str_into_u32(idx, link, vals)?));
                    } else {
                        return Err(TeangaError::ModelError(
                            format!("Cannot convert link layer without link types")));
               
                    }
                }
                Ok(Layer::Span(result))
            },
            RawLayer::MetaLayer(vals) => {
                Ok(Layer::MetaLayer(vals))
            }
        }
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
pub enum RawLayer {
    CharacterLayer(String),
    L1(Vec<u32>),
    L2(Vec<(u32,u32)>),
    L3(Vec<(u32,u32,u32)>),
    LS(Vec<String>),
    L1S(Vec<(u32,String)>),
    L2S(Vec<(u32,u32,String)>),
    L3S(Vec<(u32,u32,u32,String)>),
    MetaLayer(Vec<HashMap<String, Value>>)
}

fn u32_into_raw_str<F>(val : u32, layer_type : &DataType, f : &F) -> TeangaResult<String> 
    where F : Fn(u32) -> String {
    match layer_type {
        DataType::String => Ok(f(val)),
        DataType::Enum(vals) => {
            if val < vals.len() as u32 {
                Ok(vals[val as usize].clone())
            } else {
                Err(TeangaError::ModelError(
                        format!("Enum data is out of range of enum")))
            }
        }
        _ => Err(TeangaError::ModelError(
                format!("Cannot convert {} to string", layer_type)))
    }
}

fn u32_into_raw_u32_str(val : u32, vals : &Vec<String>) -> TeangaResult<(u32,String)> {
    let n = (vals.len() as f64).log2().ceil() as u32;
    let link_targ = val >> n;
    let link_type = val & ((1 << n) - 1);
    if link_type < vals.len() as u32 {
        Ok((link_targ, vals[link_type as usize].clone()))
    } else {
        Err(TeangaError::ModelError(
                format!("Link type is out of range of enum")))
    }
}

fn raw_str_into_u32<F : StringIndex>(val : &str, layer_type : &DataType, f : &mut F) -> TeangaResult<u32> {
    match layer_type {
        DataType::String => Ok(f.get_id(val)),
        DataType::Enum(vals) => {
            match vals.iter().position(|x| x == val) {
                Some(idx) => Ok(idx as u32),
                None => Err(TeangaError::ModelError(
                        format!("Cannot convert enum {} to {}", val, vals.iter().join(","))))
            }
        },
        _ => Err(TeangaError::ModelError( 
                format!("Cannot convert string to {}", layer_type)))
    }
}

fn raw_u32_str_into_u32(link_targ : u32, link_type : String, vals : &Vec<String>) -> TeangaResult<u32> {
    match vals.iter().position(|x| *x == link_type) {
        Some(idx) => Ok((idx as u32) << ((vals.len() as f64).log2().ceil() as u32) | link_targ),
        None => Err(TeangaError::ModelError(
                format!("Cannot convert link type {} to {}", link_type, vals.iter().join(","))))
    }
}

fn teanga_id(existing_keys : &Vec<String>, doc : &Document) -> String {
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
    fn test_gen_next_id() {
        let db = sled::Config::new().temporary(true).open().unwrap();
        assert_eq!(1, gen_next_id(&db, "A"));
        assert_eq!(2, gen_next_id(&db, "B"));
    }

    #[test]
    fn test_reopen_corpus() {
        let mut corpus = DiskCorpus::new("tmp").unwrap();
        corpus.add_layer_meta("text".to_string(), LayerType::characters, None, Some(DataType::String), None, None, None, HashMap::new()).unwrap();
        corpus.add_doc(vec![("text".to_string(), "test")]).unwrap();
        let _corpus = DiskCorpus::new("tmp");
    }
}
