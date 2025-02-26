//! Documents in the corpus.
use std::collections::HashMap;
use crate::layer::{Layer, IntoLayer, LayerDesc, TeangaData};
use serde::{Deserialize, Serialize};
use crate::{Corpus, TeangaResult, TeangaError};
use std::ops::Index;

/// Anything that can be understood as a document content
pub trait DocumentContent<D> : IntoIterator<Item=(String, D)> where D : IntoLayer {
    /// The keys of the layers in the document
    fn keys(&self) -> Vec<String>;
    /// Convert the content into a map of layers
    fn as_map(self, meta : &HashMap<String, LayerDesc>) -> TeangaResult<HashMap<String, Layer>> where Self : Sized {
        let mut map = HashMap::new();
        for (k, v) in self.into_iter() {
            if let Some(meta) = meta.get(&k) {
                map.insert(k, v.into_layer(meta)?);
            } else if k.starts_with("_") {
                map.insert(k, v.into_meta_layer()?);
            } else {
                return Err(TeangaError::LayerNotFoundError(k))
            }
        }
        Ok(map)
    }

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

#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
/// A document object
pub struct Document {
    #[serde(flatten)]
    pub content: HashMap<String, Layer>
}

impl Document {
    /// Create a new document from its content
    ///
    /// # Arguments
    ///
    /// * `content` - The content of the document
    /// * `meta` - The metadata for the document
    ///
    /// # Returns
    ///
    /// A new document object
    pub fn new<D : IntoLayer, DC : DocumentContent<D>>(content : DC, meta: &HashMap<String, LayerDesc>) -> TeangaResult<Document> {
       for key in content.keys() {
            if !key.starts_with("_") &&
                !meta.contains_key(&key) {
                return Err(TeangaError::ModelError(
                    format!("Layer {} does not exist", key)))
            }
        }
        let mut doc_content = HashMap::new();
        for (k, v) in content {
            if k.starts_with("_") {
                doc_content.insert(k,
                    v.into_meta_layer()?);
            } else {
                let layer_meta = meta.get(&k).ok_or_else(|| TeangaError::ModelError(
                    format!("No meta information for layer {}", k)))?;
                doc_content.insert(k, 
                    v.into_layer(layer_meta)?);
            }
        }
        Ok(Document {
            content: doc_content
        })
    }

    /// Get the text that is indexed by a particular layer
    /// divided by the annotations in this layer
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer to get the text from
    /// * `meta` - The metadata for the document
    ///
    /// # Returns
    ///
    /// A vector of strings, each string representing a span of text
    pub fn text(&self, layer: &str, 
        meta : &HashMap<String, LayerDesc>)
        -> TeangaResult<Vec<&str>> {
        if let Some(layer_desc) = meta.get(layer) {
            let mut char_layer = layer;
            let mut char_layer_desc = layer_desc;
            while char_layer_desc.base.is_some() {
                char_layer = char_layer_desc.base.as_ref().unwrap();
                char_layer_desc = meta.get(char_layer).unwrap();
            }
            if let Some(character_layer) = self.content.get(char_layer) {
                if char_layer == layer {
                    return Ok(character_layer.characters().into_iter().collect());
                } else if let Some(characters) = character_layer.characters() {
                    let indexes = self.indexes(layer, char_layer, meta)?;
                    let mut text = Vec::new();
                    for (start, end) in indexes {
                        text.push(&characters[start as usize..end as usize]);
                    }
                    Ok(text)
                } else {
                    Err(TeangaError::LayerNotFoundError(char_layer.to_string()))
                }
            } else {
                Err(TeangaError::LayerNotFoundError(char_layer.to_string()))
            }
        } else {
            Err(TeangaError::LayerNotFoundError(layer.to_string()))
        }
    }

    /// Get the data that is contained in this layer
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer to get the data from
    /// * `meta` - The metadata for the document
    ///
    /// # Returns
    ///
    /// A vector of data objects
    pub fn data(&self, layer: &str, 
        meta : &HashMap<String, LayerDesc>)
        -> Option<Vec<TeangaData>> {
        if let Some(layer_val) = self.content.get(layer) {
            if let Some(layer_desc) = meta.get(layer) {
                return Some(layer_val.data(layer_desc));
            }
        }
        None
    }

    /// Get the indexes that this layer refers to in the target layer
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer to get the indexes from
    /// * `target_layer` - The layer to get the indexes in
    /// * `meta` - The metadata for the document
    ///
    /// # Returns
    ///
    /// A vector of tuples, each tuple representing a span of text
    pub fn indexes(&self, layer: &str, target_layer: &str,
        meta : &HashMap<String, LayerDesc>)
        -> TeangaResult<Vec<(usize, usize)>> {
        if let Some(layer_val) = self.content.get(layer) {
            layer_val.indexes(layer, target_layer, &self, meta)
        } else {
            Err(TeangaError::LayerNotFoundError(layer.to_string()))
        }
    }

    /// Get the indexes and the data associated with it
    ///
    /// # Arguments
    ///
    /// * `layer` - The layer to get the indexes from
    /// * `target_layer` - The layer to get the indexes in
    /// * `meta` - The metadata for the document
    ///
    /// # Returns
    ///
    /// A vector of tuples, each tuple representing a span of text and the data
    pub fn indexes_data(&self, layer: &str, target_layer: &str,
        meta : &HashMap<String, LayerDesc>)
        -> TeangaResult<Vec<(usize, usize, TeangaData)>> {
        if let Some(layer_val) = self.content.get(layer) {
            layer_val.indexes_data(layer, target_layer, &self, meta)
        } else {
            Err(TeangaError::LayerNotFoundError(layer.to_string()))
        }
    }

    /// Get the names of layers in this document
    pub fn keys(&self) -> Vec<String> {
        self.content.keys().cloned().collect()
    }

    /// Get a single layer
    pub fn get(&self, key: &str) -> Option<&Layer> {
        self.content.get(key)
    }

    /// Get a mutable reference to a single layer
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Layer> {
        self.content.get_mut(key)
    }

    /// Set a layer value.
    ///
    /// **Note**: If you set a character layer this may change the identifier
    /// of the document
    pub fn set(&mut self, key: &str, value: Layer) {
        self.content.insert(key.to_string(), value);
    }
}

impl IntoIterator for Document {
    type Item = (String, Layer);
    type IntoIter = std::collections::hash_map::IntoIter<String, Layer>;

    fn into_iter(self) -> Self::IntoIter {
        self.content.into_iter()
    }
}

impl Index<&str> for Document {
    type Output = Layer;

    fn index(&self, key: &str) -> &Layer {
        &self.content[key]
    }
}

impl DocumentContent<Layer> for Document {
    fn keys(&self) -> Vec<String> {
        self.content.keys().cloned().collect()
    }
}

/// Builder interface for creating documents
pub struct DocumentBuilder<'a, C : Corpus>(&'a mut C, HashMap<String, Layer>);

impl<'a, C : Corpus> DocumentBuilder<'a, C> {
    /// Create a new builder. This can also be done using the `Corpus.build_doc()`
    /// method
    pub fn new(corpus : &'a mut C) -> DocumentBuilder<'a, C> {
        DocumentBuilder(corpus, HashMap::new())
    }

    /// Add a layer to the document
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the layer
    /// * `layer` - The layer content
    ///
    /// # Returns
    ///
    /// The same builder object passed or an error if the layer does not exist
    /// or the data provided is not valid for that layer's metadata
    pub fn layer<I : IntoLayer>(mut self, name: &str, layer: I) -> TeangaResult<DocumentBuilder<'a, C>> {
        if name.starts_with("_") {
            self.1.insert(name.to_string(), layer.into_meta_layer()?);
        } else {
            let layer_desc = self.0.get_meta().get(name)
                    .ok_or_else(|| TeangaError::ModelError(
                        format!("Layer {} does not exist", name)))?;
            self.1.insert(name.to_string(), layer.into_layer(layer_desc)?);
        }
        Ok(self)
    }

    /// Finalize the builder and add this document to the corpus
    ///
    /// # Returns
    ///
    /// The ID of the document
    pub fn add(self) -> TeangaResult<String> {
        self.0.add_doc(self.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimpleCorpus;
    use crate::layer::{LayerType, DataType};

    #[test]
    fn test_corpus_layers() {
        let mut corpus = SimpleCorpus::new();
        corpus.build_layer("text").add().unwrap();
        corpus.build_layer("tokens")
            .base("text")
            .layer_type(LayerType::span)
            .add().unwrap();
        corpus.build_layer("entities")
            .base("tokens")
            .layer_type(LayerType::span)
            .data(DataType::String)
            .add().unwrap();
        let doc = corpus.build_doc()
            .layer("text", "The White House is in Washington.").unwrap()
            .layer("tokens", vec![
                (0, 3), (4, 9), (10, 15), (16, 18), (19,21), (22,32), (32,33)]).unwrap()
            .layer("entities", vec![
                (1,3,"LOC"), (5,6,"ORG")]).unwrap()
            .add().unwrap();
        let doc = corpus.get_doc_by_id(&doc).unwrap();
        assert_eq!(doc.text("text", corpus.get_meta()).unwrap(), vec!["The White House is in Washington."]);
        assert_eq!(doc.text("tokens", corpus.get_meta()).unwrap(), vec!["The", "White", "House", "is", "in", "Washington", "."]);
        eprintln!("{:?}", doc.indexes("entities", "text", corpus.get_meta()));
        assert_eq!(doc.text("entities", corpus.get_meta()).unwrap(), vec!["White House", "Washington"]);
    }
}

