use std::collections::HashMap;
use crate::layer::{Layer, IntoLayer, LayerDesc};
use serde::{Deserialize, Serialize};
use crate::{TeangaResult, TeangaError};

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

#[derive(Debug,Clone,Serialize,Deserialize)]
/// A document object
pub struct Document {
    pub content: HashMap<String, Layer>
}

impl Document {
    pub fn new<D : IntoLayer, DC : DocumentContent<D>>(content : DC, meta: &HashMap<String, LayerDesc>) -> TeangaResult<Document> {
       for key in content.keys() {
            if !meta.contains_key(&key) {
                return Err(TeangaError::ModelError(
                    format!("Layer {} does not exist", key)))
            }
        }
        let mut doc_content = HashMap::new();
        for (k, v) in content {
            let layer_meta = meta.get(&k).ok_or_else(|| TeangaError::ModelError(
                format!("No meta information for layer {}", k)))?;
            doc_content.insert(k, 
                v.into_layer(layer_meta)?);
        }
        Ok(Document {
            content: doc_content
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


