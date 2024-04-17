// Module for working with corpora in transactions
use std::collections::HashMap;
use crate::*;
use crate::disk_corpus::{NameLayerDesc, Order};
use polodb_core::Database;
use polodb_core::bson::doc;

/// A corpus object
pub struct TransactionCorpus {
    pub meta: HashMap<String, LayerDesc>,
    pub order: Vec<String>,
    pub path: String,
    pub db: Database
}

impl TransactionCorpus {
    pub fn new(path : &str) -> TeangaResult<TransactionCorpus> {
        let db = Database::open_file(&path)?;
        let collection = db.collection::<NameLayerDesc>("meta");
        let mut meta = HashMap::new();
        for nld in collection.find(None)? {
            let nld = nld?;
            meta.insert(nld.name, nld.desc);
        }
        let collection = db.collection::<Order>("order");
        let order = if let Some(o) = collection.find(None)?.next() {
            o?.order
        } else {
            Vec::new()
        };
        Ok(TransactionCorpus {
            meta,
            order,
            path: path.to_string(),
            db
        })
    }

    pub fn set_meta(&mut self, meta : HashMap<String, LayerDesc>) -> TeangaResult<()> {
        self.meta = meta;
        Ok(())
    }

    pub fn set_order(&mut self, order : Vec<String>) -> TeangaResult<()> {
        self.order = order;
        Ok(())
    }

    pub fn commit(self) -> TeangaResult<DiskCorpus> {
        let collection = self.db.collection::<NameLayerDesc>("meta");
        for (k, v) in self.meta.iter() {
            collection.insert_one(NameLayerDesc {
                name: k.clone(),
                desc: v.clone()
            })?;
        }
        let collection = self.db.collection::<Order>("order");
        collection.insert_one(Order::new(self.order.clone()))?;

        Ok(DiskCorpus::new_unchecked(self.meta, self.order, self.path))
    }
}

impl Corpus for TransactionCorpus {
    type LayerStorage = Layer;
    type Content = HashMap<String, Layer>;

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
        meta: HashMap<String, Value>) -> TeangaResult<()> {
        let layer_desc = LayerDesc::new(name.clone(), layer_type, base, data, link_types, 
                target, default, meta)?;
        self.meta.insert(name, layer_desc);
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
        let collection = self.db.collection::<Document>("corpus");
        let doc = Document::from_content(&self.order, content, &self.meta)?;
        let id = doc.id.clone();
        collection.insert_one(doc)?;
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
    fn update_doc<D : IntoLayer, DC : DocumentContent<D>>(&mut self, id : &str, content : DC) -> TeangaResult<String> {
        let idx = self.order.iter().position(|x| x == id)
            .ok_or(TeangaError::ModelError(format!("Document {} not found", id)))?;

        let collection = self.db.collection::<Document>("corpus");
        let doc = Document::from_content(&self.order, content, &self.meta)?;
        let new_id = doc.id.clone();
        collection.delete_one(doc! {
            "id": doc.id.clone()
        })?;
        collection.insert_one(doc)?;
        if new_id != id {
            self.order.remove(idx);
            self.order.insert(idx, new_id.clone());
        }
        Ok(new_id)
    }

 
    /// Remove a document from the corpus
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the document
    fn remove_doc(&mut self, id : &str) -> TeangaResult<()> {
        let idx = self.order.iter().position(|x| x == id)
            .ok_or(TeangaError::ModelError(format!("Document {} not found", id)))?;

        let db = Database::open_file(&self.path)?;
        let collection = db.collection::<Document>("corpus");
        collection.delete_one(doc! {
            "id": id
        })?;
        self.order.remove(idx);
        Ok(())
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
    fn get_doc_by_id(&self, id : &str) -> TeangaResult<HashMap<String, Layer>> {
        let db = Database::open_file(&self.path)?;
        let collection = db.collection::<Document>("corpus");
        let doc = collection.find_one(doc! {
            "id": id
        })?.ok_or(TeangaError::ModelError(format!("Document {} not found", id)))?;
        Ok(doc.content)
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
