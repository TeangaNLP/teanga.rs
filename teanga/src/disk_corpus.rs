//! Corpora stored on disk.
//!
//! A disk corpus is a corpus that is stored on disk. It is a corpus that is
//! stored in a database. The database is a key-value store that stores the
//! metadata for the corpus and the documents in the corpus.
use std::collections::HashMap;
use crate::*;
use crate::cuac::SupportedStringCompression;
use crate::cuac::read_cuac_header;
use crate::cuac::read_cuac_doc;
use crate::cuac::write_cuac_header_compression;
use crate::cuac::write_cuac_doc;
use crate::cuac::Index;
#[cfg(feature = "fjall")]
use fjall::{Config, PartitionCreateOptions, PartitionHandle};
#[cfg(feature = "redb")]
use redb::{Database, TableDefinition, TableError};
use ciborium::{from_reader, into_writer};
use std::path::Path;

const DOCUMENT_PREFIX : u8 = 0x00;
const META_BYTES : [u8;1] = [0x01];
const ORDER_BYTES : [u8;1] = [0x02];
const INDEX_BYTES : [u8;1] = [0x03];
#[cfg(feature = "redb")]
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("corpus");

/// A corpus stored on disk
pub struct DiskCorpus<D : DBImpl> {
    meta: HashMap<String, LayerDesc>,
    order: Vec<String>,
    compression_model: SupportedStringCompression,
    index: Index,
    db: D
}

#[cfg(feature = "sled")]
impl DiskCorpus<SledDb> {
    /// Create a new corpus
    ///
    /// # Arguments
    /// * `path` - The path to the database
    ///
    /// # Returns
    /// A new corpus object
    ///
    pub fn new<P : AsRef<Path>>(path : P) -> TeangaResult<DiskCorpus<SledDb>> {
        DiskCorpus::with_db(open_sled_db(path)?)
    }
}

#[cfg(all(not(feature = "sled"), feature = "fjall"))]
impl DiskCorpus<FjallDb> {
    /// Create a new corpus
    ///
    /// # Arguments
    /// * `path` - The path to the database
    ///
    /// # Returns
    /// A new corpus object
    ///
    pub fn new<P : AsRef<Path>>(path : P) -> TeangaResult<DiskCorpus<FjallDb>> {
        DiskCorpus::with_db(open_fjall_db(path)?)
    }
}

#[cfg(all(not(feature = "sled"), not(feature = "fjall"), feature = "redb"))]
impl DiskCorpus<RedbDb> {
    /// Create a new corpus
    ///
    /// # Arguments
    /// * `path` - The path to the database
    ///
    /// # Returns
    /// A new corpus object
    ///
    pub fn new<P : AsRef<Path>>(path : P) -> TeangaResult<DiskCorpus<RedbDb>> {
        DiskCorpus::with_db(open_redb_db(path)?)
    }
}

impl DiskCorpus<PathAsDB> {
    /// Create a new corpus, with a specific path in the database. This
    /// path will be loaded in a lazy manner, so that the database is
    /// only opened when it is needed.
    ///
    /// # Arguments
    /// * `path` - The path to the database
    ///
    /// # Returns
    /// A new corpus object
    #[cfg(any(feature = "sled", feature = "fjall", feature = "redb"))]
    pub fn new_path_db<P : AsRef<Path>>(path : P) -> DiskCorpus<PathAsDB> {
        DiskCorpus::with_db(PathAsDB(path.as_ref().to_string_lossy().to_string())).unwrap()
    }
}

impl <D: DBImpl> DiskCorpus<D> {
    /// Create a new corpus, with a specific database. The
    /// DB should be constructed from one of the methods
    /// `open_sled_db`, `open_fjall_db` or `open_redb_db`
    ///
    /// # Arguments
    /// * `db` - The database
    ///
    /// # Returns
    /// A new corpus object
    ///
    pub fn with_db(db : D) -> TeangaResult<DiskCorpus<D>> {
        let (meta, compression_model) = if let Some(meta_bytes) = db.get(META_BYTES.to_vec())? {
            read_cuac_header::<&[u8]>(&mut meta_bytes.as_ref())
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
        write_cuac_doc(&mut data, doc.clone(), &mut self.index, &self.meta, &self.compression_model)
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
                let doc = read_cuac_doc(&mut bytes.as_ref(), &self.meta, 
                        &self.index.freeze(), &self.compression_model)
                    .map_err(|e| TeangaError::ModelError(e.to_string()))?;
                Ok(doc)
            },
            None => Ok(None)
        }
    }

    pub fn commit(&mut self) -> TeangaResult<()> {
        let mut meta_bytes = Vec::new();
        write_cuac_header_compression(&mut meta_bytes, &self.meta, &self.compression_model)
            .map_err(|e| TeangaError::ModelError(e.to_string()))?;
        self.db.insert(META_BYTES.to_vec(), meta_bytes)?;
        self.db.insert(ORDER_BYTES.to_vec(), to_stdvec(&self.order)?)?;
        let index_bytes = self.index.to_bytes();
        self.db.insert(INDEX_BYTES.to_vec(), index_bytes)?;
        Ok(())
    }
}


impl <DB : DBImpl> Corpus for DiskCorpus<DB> {
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

    /// Get the order of the documents in the corpus
    ///
    /// # Returns
    ///
    /// The order of the documents in the corpus
    fn get_order(&self) -> &Vec<String> {
        &self.order
    }
}


impl <DB : DBImpl> WriteableCorpus for DiskCorpus<DB> {
    fn set_meta(&mut self, meta : HashMap<String, LayerDesc>) -> TeangaResult<()> {
        self.meta = meta;
        Ok(())
        
    }
    fn set_order(&mut self, order : Vec<String>) -> TeangaResult<()> {
        self.order = order;
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
}

impl <DB : DBImpl> ReadableCorpus for DiskCorpus<DB> {

    fn get_meta(&self) -> &HashMap<String, LayerDesc> {
        &self.meta
    }
    /// Iterate over all documents in the corpus
    fn iter_docs<'a>(&'a self) -> Box<dyn Iterator<Item=TeangaResult<Document>> + 'a> {
        Box::new(self.get_docs().into_iter().map(move |x| self.get_doc_by_id(&x)))
    }
    /// Iterate over all documents in the corpus with their IDs
    fn iter_doc_ids<'a>(&'a self) -> Box<dyn Iterator<Item=TeangaResult<(String, Document)>> + 'a> {
        Box::new(self.get_docs().into_iter().map(move |x| self.get_doc_by_id(&x).map(|d| (x, d))))
    }


}

impl <DB : DBImpl> Drop for DiskCorpus<DB> {
    fn drop(&mut self) {
        self.commit().unwrap();
    }
}

impl <C : Clone + DBImpl> Clone for DiskCorpus<C> {
    fn clone(&self) -> Self {
        DiskCorpus {
            meta: self.meta.clone(),
            order: self.order.clone(),
            compression_model: self.compression_model.clone(),
            index: self.index.clone(),
            db: self.db.clone()
        }
    }
}

pub trait DBImpl {
    fn insert(&self, key : Vec<u8>, value : Vec<u8>) -> TeangaResult<()>;
    fn get(&self, key : Vec<u8>) -> TeangaResult<Option<Vec<u8>>>;
    fn remove(&self, key : Vec<u8>) -> TeangaResult<()>;
    fn flush(&self) -> TeangaResult<()>;
}

#[cfg(feature = "sled")]
pub struct SledDb(sled::Db);

#[cfg(feature = "sled")]
impl DBImpl for SledDb {
    fn insert(&self, key : Vec<u8>, value : Vec<u8>) -> TeangaResult<()> {
        self.0.insert(key, value)?;
        Ok(())
    }

    fn get(&self, key : Vec<u8>) -> TeangaResult<Option<Vec<u8>>> {
        let value = self.0.get(key)?;
        Ok(value.map(|v| v.to_vec()))
    }

    fn remove(&self, key : Vec<u8>) -> TeangaResult<()> {
        self.0.remove(key)?;
        Ok(())
    }

    fn flush(&self) -> TeangaResult<()> {
        self.0.flush()?;
        Ok(())
    }
}

#[cfg(feature = "fjall")]
pub struct FjallDb(PartitionHandle);

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

#[cfg(feature = "redb")]
pub struct RedbDb(redb::Database);

#[cfg(feature = "redb")]
impl DBImpl for RedbDb {
    fn insert(&self, key : Vec<u8>, value : Vec<u8>) -> TeangaResult<()> {
        let write_txn = self.0.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            table.insert(key.as_slice(), value.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn get(&self, key : Vec<u8>) -> TeangaResult<Option<Vec<u8>>> {
        let read_txn = self.0.begin_read()?;
        match read_txn.open_table(TABLE) {
            Ok(table) => {
                let value = table.get(key.as_slice())?;
                Ok(value.map(|v| v.value().to_vec()))
            },
            Err(TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(TeangaError::DBTableError(e))
        }
    }

    fn remove(&self, key : Vec<u8>) -> TeangaResult<()> {
        let write_txn = self.0.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            table.remove(key.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn flush(&self) -> TeangaResult<()> {
        Ok(())
    }
}


#[cfg(feature = "sled")]
pub fn open_sled_db<P : AsRef<Path>>(path : P) -> TeangaResult<SledDb> {
    Ok(SledDb(sled::open(path)?))
}

#[cfg(feature = "fjall")]
pub fn open_fjall_db<P : AsRef<Path>>(path : P) -> TeangaResult<FjallDb> {
    let keyspace = Config::new(path).open()?; 
    let handle = keyspace.open_partition("corpus", PartitionCreateOptions::default())?;
    Ok(FjallDb(handle))
}

#[cfg(feature = "redb")]
pub fn open_redb_db<P: AsRef<Path>>(path : P) -> TeangaResult<RedbDb> {
    let db = if path.as_ref().exists() {
        Database::open(path)?
    } else {
        Database::create(path)?
    };
    Ok(RedbDb(db))
}

/// A path that opens a new connection to the database each time it is used. 
/// Using this is not recommended for most applications, as it will be slow.
/// This is used in the Python bindings, where the database is opened and closed
/// when passed to the Python environment.
pub struct PathAsDB(String);

impl DBImpl for PathAsDB {
    #[cfg(feature = "sled")]
    fn insert(&self, key : Vec<u8>, value : Vec<u8>) -> TeangaResult<()> {
        let db = open_sled_db(&self.0)?;
        db.insert(key, value)?;
        Ok(())
    }

    #[cfg(all(not(feature = "sled"), feature = "fjall"))]
    fn insert(&self, key : Vec<u8>, value : Vec<u8>) -> TeangaResult<()> {
        let db = open_fjall_db(&self.0)?;
        db.insert(key, value)?;
        Ok(())
    }

    #[cfg(all(not(feature = "sled"), not(feature = "fjall"), feature = "redb"))]
    fn insert(&self, key : Vec<u8>, value : Vec<u8>) -> TeangaResult<()> {
        let db = open_redb_db(&self.0)?;
        db.insert(key, value)?;
        Ok(())
    }

    #[cfg(all(not(feature = "sled"), not(feature = "fjall"), not(feature = "redb")))]
    fn insert(&self, _key : Vec<u8>, _value : Vec<u8>) -> TeangaResult<()> {
        Err(TeangaError::DBError("No Database Feature Selected".to_string()))
    }

    #[cfg(feature = "sled")]
    fn get(&self, key : Vec<u8>) -> TeangaResult<Option<Vec<u8>>> {
        let db = open_sled_db(&self.0)?;
        db.get(key)
    }

    #[cfg(all(not(feature = "sled"), feature = "fjall"))]
    fn get(&self, key : Vec<u8>) -> TeangaResult<Option<Vec<u8>>> {
        let db = open_fjall_db(&self.0)?;
        db.get(key)
    }

    #[cfg(all(not(feature = "sled"), not(feature = "fjall"), feature = "redb"))]
    fn get(&self, key : Vec<u8>) -> TeangaResult<Option<Vec<u8>>> {
        let db = open_redb_db(&self.0)?;
        db.get(key)
    }

    #[cfg(all(not(feature = "sled"), not(feature = "fjall"), not(feature = "redb")))]
    fn get(&self, _key : Vec<u8>) -> TeangaResult<Option<Vec<u8>>> {
        Err(TeangaError::DBError("No Database Feature Selected".to_string()))
    }

    #[cfg(feature = "sled")]
    fn remove(&self, key : Vec<u8>) -> TeangaResult<()> {
        let db = open_sled_db(&self.0)?;
        db.remove(key)
    }

    #[cfg(all(not(feature = "sled"), feature = "fjall"))]
    fn remove(&self, key : Vec<u8>) -> TeangaResult<()> {
        let db = open_fjall_db(&self.0)?;
        db.remove(key)
    }

    #[cfg(all(not(feature = "sled"), not(feature = "fjall"), feature = "redb"))]
    fn remove(&self, key : Vec<u8>) -> TeangaResult<()> {
        let db = open_redb_db(&self.0)?;
        db.remove(key)
    }

    #[cfg(all(not(feature = "sled"), not(feature = "fjall"), not(feature = "redb")))]
    fn remove(&self, _key : Vec<u8>) -> TeangaResult<()> {
        Err(TeangaError::DBError("No Database Feature Selected".to_string()))
    }

    #[cfg(feature = "sled")]
    fn flush(&self) -> TeangaResult<()> {
        let db = open_sled_db(&self.0)?;
        db.flush()
    }

    #[cfg(all(not(feature = "sled"), feature = "fjall"))]
    fn flush(&self) -> TeangaResult<()> {
        let db = open_fjall_db(&self.0)?;
        db.flush()
    }

    #[cfg(all(not(feature = "sled"), not(feature = "fjall"), feature = "redb"))]
    fn flush(&self) -> TeangaResult<()> {
        let db = open_redb_db(&self.0)?;
        db.flush()
    }

    #[cfg(all(not(feature = "sled"), not(feature = "fjall"), not(feature = "redb")))]
    fn flush(&self) -> TeangaResult<()> {
        Err(TeangaError::DBError("No Database Feature Selected".to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_yaml;

    #[test]
    fn test_load_disk_corpus() {
        let dir = tempfile::tempdir().unwrap();
        let tmpfile = dir.path().join("db");
        {
        let mut corpus = DiskCorpus::new(&tmpfile).unwrap();
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
            let corpus = DiskCorpus::new(&tmpfile).unwrap();
            assert!(!corpus.get_meta().is_empty());
            assert!(!corpus.get_docs().is_empty());
        }
    }

    #[test]
    fn test_reopen_corpus() {
        let dir = tempfile::tempdir().unwrap();
        let tmpfile = dir.path().join("db");
        let mut corpus = DiskCorpus::new(&tmpfile).unwrap();
        corpus.add_layer_meta("text".to_string(), LayerType::characters, None, Some(DataType::Enum(vec!["a".to_string(),"b".to_string()])), None, None, None, HashMap::new()).unwrap();
        corpus.add_doc(vec![("text".to_string(), "test")]).unwrap();
        drop(corpus);
        let corpus2 = DiskCorpus::new(&tmpfile).unwrap();
        assert!(!corpus2.get_meta().is_empty());
    }
}
