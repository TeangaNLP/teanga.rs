//! Corpora stored on disk.
//!
//! A disk corpus is a corpus that is stored on disk. It is a corpus that is
//! stored in a database. The database is a key-value store that stores the
//! metadata for the corpus and the documents in the corpus.
use std::collections::HashMap;
use crate::*;
use crate::tcf::SupportedStringCompression;
use crate::tcf::read_tcf_header;
use crate::tcf::read_tcf_doc;
use crate::tcf::write_tcf_header_compression;
use crate::tcf::write_tcf_doc;
use crate::tcf::Index;
#[cfg(feature = "fjall")]
use fjall::{Config, PersistMode, Keyspace, PartitionCreateOptions, PartitionHandle};
use ciborium::{from_reader, into_writer};

const DOCUMENT_PREFIX : u8 = 0x00;
const META_BYTES : [u8;1] = [0x01];
const ORDER_BYTES : [u8;1] = [0x02];
const INDEX_BYTES : [u8;1] = [0x03];

/// A corpus stored on disk
pub struct DiskCorpus {
    meta: HashMap<String, LayerDesc>,
    order: Vec<String>,
    compression_model: SupportedStringCompression,
    index: Index,
    db: Box<dyn DBImpl>
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
        let (meta, compression_model) = if let Some(meta_bytes) = db.get(META_BYTES.to_vec())? {
            read_tcf_header::<&[u8]>(&mut meta_bytes.as_ref())
                .map_err(|e| TeangaError::ModelError(e.to_string()))?
        } else {
            (HashMap::new(), SupportedStringCompression::Smaz)
        };
        let order = match db.get(ORDER_BYTES.to_vec())? {
            Some(bytes) => from_bytes::<Vec<String>>(bytes.as_ref())?,
            None => Vec::new()
        };
        let index = match db.get(INDEX_BYTES.to_vec())? {
            Some(bytes) => Index::from_bytes::<&[u8]>(bytes.as_ref())
                .map_err(|e| TeangaError::ModelError(e.to_string()))?,
            None => Index::new()
        };
        Ok(DiskCorpus {
            meta,
            order,
            compression_model,
            index,
            db
        })
    }

    fn insert(&mut self, id : String, doc : Document) -> TeangaResult<()> {
        let mut data = Vec::new();
        write_tcf_doc(&mut data, doc.clone(), &mut self.index, &self.meta, &self.compression_model)
            .map_err(|e| TeangaError::ModelError(e.to_string()))?;
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(id.as_bytes());
        self.db.insert(id_bytes, data)?;
        Ok(())

    }

    fn remove(&mut self, id : &str) -> TeangaResult<()> {
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(id.as_bytes());
        self.db.remove(id_bytes)?;
        Ok(())
    }

    fn get(&self, id : &str) -> TeangaResult<Option<Document>> {
        let mut id_bytes = Vec::new();
        id_bytes.push(DOCUMENT_PREFIX);
        id_bytes.extend(id.as_bytes());
        match self.db.get(id_bytes)? {
            Some(bytes) => {
                let doc = read_tcf_doc(&mut bytes.as_ref(), &self.meta, 
                        &self.index.freeze(), &self.compression_model)
                    .map_err(|e| TeangaError::ModelError(e.to_string()))?;
                Ok(doc)
            },
            None => Ok(None)
        }
    }

    fn commit(&mut self) -> TeangaResult<()> {
        let mut meta_bytes = Vec::new();
        write_tcf_header_compression(&mut meta_bytes, &self.meta, &self.compression_model)
            .map_err(|e| TeangaError::ModelError(e.to_string()))?;
        self.db.insert(META_BYTES.to_vec(), meta_bytes)?;
        self.db.insert(ORDER_BYTES.to_vec(), to_stdvec(&self.order)?)?;
        let index_bytes = self.index.to_bytes();
        self.db.insert(INDEX_BYTES.to_vec(), index_bytes)?;
        Ok(())
    }
}


impl Corpus for DiskCorpus {
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
        self.insert(id.clone(), doc)
            .map_err(|e| TeangaError::ModelError(e.to_string()))?;
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
            self.remove(id)
                .map_err(|e| TeangaError::ModelError(e.to_string()))?;
            self.insert(new_id.clone(), doc)
                .map_err(|e| TeangaError::ModelError(e.to_string()))?;
        } else {
            self.insert(id.to_string(), doc)
                .map_err(|e| TeangaError::ModelError(e.to_string()))?;
        }
        Ok(new_id)
    }

    fn remove_doc(&mut self, id : &str) -> TeangaResult<()> {
        self.remove(id)
            .map_err(|e| TeangaError::ModelError(e.to_string()))?;
        self.order.retain(|x| x != id);
        Ok(())
    }

    fn get_doc_by_id(&self, id : &str) -> TeangaResult<Document> {
        match self.get(id)? {
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

    /// Get the order of the documents in the corpus
    ///
    /// # Returns
    ///
    /// The order of the documents in the corpus
    fn get_order(&self) -> &Vec<String> {
        &self.order
    }
}


impl WriteableCorpus for DiskCorpus {
    fn set_meta(&mut self, meta : HashMap<String, LayerDesc>) -> TeangaResult<()> {
        self.meta = meta;
        Ok(())
        
    }
    fn set_order(&mut self, order : Vec<String>) -> TeangaResult<()> {
        self.order = order;
        Ok(())
    }
}

impl Drop for DiskCorpus {
    fn drop(&mut self) {
        self.commit().unwrap();
    }
}

trait DBImpl {
    fn insert(&self, key : Vec<u8>, value : Vec<u8>) -> TeangaResult<()>;
    fn get(&self, key : Vec<u8>) -> TeangaResult<Option<Vec<u8>>>;
    fn remove(&self, key : Vec<u8>) -> TeangaResult<()>;
    fn flush(&self) -> TeangaResult<()>;
}

#[cfg(feature = "sled")]
struct SledDb(sled::Db);

#[cfg(feature = "sled")]
impl DBImpl for SledDb {
    fn insert(&self, key : Vec<u8>, value : Vec<u8>) -> TeangaResult<()> {
        self.0.insert(key, value).map_err(|e| TeangaError::DBError(e))?;
        Ok(())
    }

    fn get(&self, key : Vec<u8>) -> TeangaResult<Option<Vec<u8>>> {
        let value = self.0.get(key).map_err(|e| TeangaError::DBError(e))?;
        Ok(value.map(|v| v.to_vec()))
    }

    fn remove(&self, key : Vec<u8>) -> TeangaResult<()> {
        self.0.remove(key).map_err(|e| TeangaError::DBError(e))?;
        Ok(())
    }

    fn flush(&self) -> TeangaResult<()> {
        self.0.flush().map_err(|e| TeangaError::DBError(e))?;
        Ok(())
    }
}

#[cfg(feature = "fjall")]
struct FjallDb(PartitionHandle);

#[cfg(feature = "fjall")]
impl DBImpl for FjallDb {
    fn insert(&self, key : Vec<u8>, value : Vec<u8>) -> TeangaResult<()> {
        self.0.insert(key, value)?;
        Ok(())
    }

    fn get(&self, key : Vec<u8>) -> TeangaResult<Option<Vec<u8>>> {
        Ok(self.0.get(key)?.map(|v| v.to_vec()))
    }

    fn remove(&self, key : Vec<u8>) -> TeangaResult<()> {
        self.0.remove(key)?;
        Ok(())
    }

    fn flush(&self) -> TeangaResult<()> {
        Ok(())
    }
}

#[cfg(feature = "sled")]
fn open_db(path : &str) -> TeangaResult<Box<dyn DBImpl>> {
    Ok(Box::new(SledDb(sled::open(path)?)))
}

#[cfg(all(not(feature = "sled"), feature = "fjall"))]
fn open_db(path : &str) -> TeangaResult<Box<dyn DBImpl>> {
    let keyspace = Config::new(path).open()?; 
    let handle = keyspace.open_partition("corpus", PartitionCreateOptions::default())?;
    Ok(Box::new(FjallDb(handle)))
}

fn to_stdvec<T : Serialize>(t : &T) -> TeangaResult<Vec<u8>> {
    let mut v = Vec::new();
    into_writer(t,  &mut v).map_err(|e| TeangaError::DataError(e))?;
    Ok(v)
}

fn from_bytes<T : serde::de::DeserializeOwned>(bytes : &[u8]) -> TeangaResult<T> {
    from_reader(bytes).map_err(|e| TeangaError::DataError2(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_yaml;

    #[test]
    fn test_load_disk_corpus() {
        {
        let mut corpus = DiskCorpus::new("tmp2").unwrap();
        let data = "_meta:
    lemmas:
        type: seq
        base: tokens
        data: string
    oewn:
        type: element
        base: tokens
        data: string
    pos:
        type: seq
        base: tokens
        data: string
    sentence:
        type: div
        base: text
    text:
        type: characters
    tokens:
        type: span
        base: text
    wn30_key:
        type: element
        base: tokens
        data: string
/KOa:
    text: The quick brown fox jumps over the lazy dog.";
        read_yaml(data.as_bytes(), &mut corpus).unwrap();
        assert!(!corpus.get_meta().is_empty());
        }
        {
            let corpus = DiskCorpus::new("tmp2").unwrap();
            assert!(!corpus.get_meta().is_empty());
            assert!(!corpus.get_docs().is_empty());
        }
    }

    #[test]
    fn test_reopen_corpus() {
        let mut corpus = DiskCorpus::new("tmp").unwrap();
        corpus.add_layer_meta("text".to_string(), LayerType::characters, None, Some(DataType::Enum(vec!["a".to_string(),"b".to_string()])), None, None, None, HashMap::new()).unwrap();
        corpus.add_doc(vec![("text".to_string(), "test")]).unwrap();
        drop(corpus);
        let corpus2 = DiskCorpus::new("tmp").unwrap();
        assert!(!corpus2.get_meta().is_empty());
    }


}


