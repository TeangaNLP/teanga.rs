use std::collections::HashMap;
use crate::*;

#[derive(Debug,Clone)]
/// A corpus object
pub struct DiskCorpus {
    pub meta: HashMap<String, LayerDesc>,
    pub order: Vec<String>,
    pub path: String // TODO not public
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

    /// Create a new corpus with fixed metadata, not metadata read from the 
    /// database
    ///
    /// # Arguments
    /// * `meta` - The metadata for the corpus
    /// * `order` - The order of the documents in the corpus
    /// * `path` - The path to the database
    ///
    /// # Returns
    /// A new corpus object
    pub fn new_unchecked(meta: HashMap<String, LayerDesc>, order: Vec<String>, path: String) -> DiskCorpus {
        DiskCorpus {
            meta,
            order,
            path: path.to_string()
        }
    }
}


impl Corpus for DiskCorpus {
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
        let layer_desc = LayerDesc::new(&name, layer_type, base, data, 
            link_types, target, default, meta)?;
        let db = open_db(&self.path)?;
        let mut id_bytes = Vec::new();
        id_bytes.push(META_PREFIX);
        id_bytes.extend(name.clone().as_bytes());
        db.insert(id_bytes, to_stdvec(&layer_desc)?)
            .map_err(|e| TeangaError::DBError(e))?;

        self.meta.insert(name.clone(), layer_desc);
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

        let db = open_db(&self.path)?;

        db.insert(ORDER_BYTES.to_vec(), to_stdvec(&self.order)?)
            .map_err(|e| TeangaError::DBError(e))?;

        let data = to_stdvec(&doc)?;
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(id.as_bytes());
        db.insert(id_bytes, data).map_err(|e| TeangaError::DBError(e))?;
        Ok(id)
    }

    fn add_docs<D : IntoLayer, DC : DocumentContent<D>>(&mut self, contents : Vec<DC>) -> TeangaResult<Vec<String>> {
        let mut docs = Vec::new();
        let mut ids = Vec::new();
        for content in contents {
            let doc = Document::new(content, &self.meta)?;
            let id = teanga_id(&self.order, &doc);
        
            self.order.push(id.clone());
            docs.push((id.clone(), to_stdvec(&doc)?));
            ids.push(id);
        }

        let order_bytes = to_stdvec(&self.order)?;

        let db = open_db(&self.path)?;
        db.transaction(move |tx| {
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
        let doc = Document::new(content, &self.meta)?;
        let new_id = teanga_id(&self.order, &doc);
        let db = open_db(&self.path)?;
        if id != new_id {
        
            let n = self.order.iter().position(|x| x == id).ok_or_else(|| TeangaError::ModelError(
                format!("Cannot find document in order vector: {}", id)))?;
            self.order.remove(n);
            self.order.insert(n, new_id.clone());

            db.insert(ORDER_BYTES.to_vec(), to_stdvec(&self.order)?).
                map_err(|e| TeangaError::DBError(e))?;

            let mut old_id_bytes = Vec::new();
            old_id_bytes.push(DOCUMENT_PREFIX);
            old_id_bytes.extend(id.as_bytes());
            db.remove(old_id_bytes).map_err(|e| TeangaError::ModelError(
                format!("Cannot remove document {}", e)))?;
        }

        let data = to_stdvec(&doc)?;
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(new_id.as_bytes());
        db.insert(id_bytes, data).map_err(|e| TeangaError::DBError(e))?;
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
        let db = open_db(&self.path)?;
        db.remove(id_bytes).map_err(|e| TeangaError::ModelError(
            format!("Cannot remove document {}", e)))?;
        self.order.retain(|x| x != id);
        db.insert(ORDER_BYTES.to_vec(), to_stdvec(&self.order)?).
            map_err(|e| TeangaError::DBError(e))?;
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
            result.insert(key, layer);

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

