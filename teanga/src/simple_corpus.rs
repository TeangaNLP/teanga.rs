use std::collections::HashMap;
use crate::*;

#[derive(Debug, Clone)]
/// An in-memory corpus object
pub struct SimpleCorpus {
    meta: HashMap<String, LayerDesc>,
    order: Vec<String>,
    content: HashMap<String, Document>,
    str2idx: HashMap<String, u32>,
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
        target: Option<String>, default: Option<Layer>,
        meta : HashMap<String, Value>) -> TeangaResult<()> {
        self.meta.insert(name.clone(), LayerDesc::new(name, layer_type, base, data, link_types, target, default, meta)?);
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
        let doc = Document::from_content(&self.order, content, &self.meta)?;
        let id = doc.id.clone();
        self.order.push(doc.id.clone());
        self.content.insert(doc.id.clone(), doc);
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
            doc_content.insert(k.clone(), v.into_layer(&self.meta[&k].clone())?);
        }
        let doc = Document::from_content(&self.order, doc_content, &self.meta)?;
        let new_id = doc.id.clone();
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


