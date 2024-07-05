use std::collections::HashMap;
use crate::layer::{Layer, IntoLayer, LayerDesc, TeangaData};
use serde::{Deserialize, Serialize};
use crate::{Corpus, TeangaResult, TeangaError};
use std::ops::Index;

pub trait DocumentContent<D> : IntoIterator<Item=(String, D)> where D : IntoLayer {
    fn keys(&self) -> Vec<String>;
    fn as_map(self, meta : &HashMap<String, LayerDesc>) -> TeangaResult<HashMap<String, Layer>> where Self : Sized {
        let mut map = HashMap::new();
        for (k, v) in self.into_iter() {
            if let Some(meta) = meta.get(&k) {
                map.insert(k, v.into_layer(meta)?);
            } else {
                return Err(TeangaError::DocumentKeyError(k))
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

    /// Get the text that is indexed by a particular layer
    /// divided by the annotations in this layer
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
            let indexes = self.indexes(layer, char_layer, meta)?;
            eprintln!("Indexes: {:?}", indexes);
            if let Some(character_layer) = self.content.get(char_layer) {
                eprintln!("Characters: {:?}", character_layer.characters());
                if let Some(characters) = character_layer.characters() {
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

    pub fn indexes(&self, layer: &str, target_layer: &str,
        meta : &HashMap<String, LayerDesc>)
        -> TeangaResult<Vec<(usize, usize)>> {
        if let Some(layer_val) = self.content.get(layer) {
            layer_val.indexes(layer, target_layer, &self, meta)
        } else {
            Err(TeangaError::LayerNotFoundError(layer.to_string()))
        }
    }
//        if let Some(layer_val) = self.content.get(layer) {
//            if let Some(layer_desc) = meta.get(layer) {
//                if let Some(ref direct_base_layer) = layer_desc.base {
//                    if let Some(direct_base_layer_val) = self.content.get(direct_base_layer) {
//                        let indexes = layer_val.indexes(layer_desc, 
//                            direct_base_layer_val.len() as u32);
//                        if layer == base_layer {
//                            Ok(indexes)
//                        } else {
//                            let base_indexes = self.indexes(direct_base_layer, base_layer, meta)?;
//                            let mut mapped_indexes = Vec::new();
//                            for (start, end) in indexes { 
//                                mapped_indexes.push((base_indexes[start as usize].0, base_indexes[end as usize].1))
//                            }
//                            Ok(mapped_indexes)
//                        }
//                    } else {
//                        Err(TeangaError::LayerNotFoundError(direct_base_layer.to_string()))
//                    }
//                } else {
//                    if layer == base_layer {
//                        let indexes = layer_val.indexes(layer_desc, 
//                            (layer_val.len() + 1) as u32);
//                        Ok(indexes)
//                    } else {
//                        Err(TeangaError::ModelError(
//                            format!("Requested base layer {} is not a base layer of layer {}", base_layer, layer)))
//                    }
//                }
//            } else {
//                Err(TeangaError::LayerNotFoundError(layer.to_string()))
//            }
//        } else {
//            Err(TeangaError::LayerNotFoundError(layer.to_string()))
//        }

    pub fn keys(&self) -> Vec<String> {
        self.content.keys().cloned().collect()
    }

    pub fn get(&self, key: &str) -> Option<&Layer> {
        self.content.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Layer> {
        self.content.get_mut(key)
    }

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

pub struct DocumentBuilder<'a, C : Corpus>(&'a mut C, HashMap<String, Layer>);

impl<'a, C : Corpus> DocumentBuilder<'a, C> {
    pub fn new(corpus : &'a mut C) -> DocumentBuilder<'a, C> {
        DocumentBuilder(corpus, HashMap::new())
    }

    pub fn layer<I : IntoLayer>(mut self, name: &str, layer: I) -> TeangaResult<DocumentBuilder<'a, C>> {
        let layer_desc = self.0.get_meta().get(name)
                .ok_or_else(|| TeangaError::ModelError(
                    format!("Layer {} does not exist", name)))?;
        self.1.insert(name.to_string(), layer.into_layer(layer_desc)?);
        Ok(self)
    }

    pub fn add(self) -> TeangaResult<String> {
        self.0.add_doc(self.1)
    }
}
