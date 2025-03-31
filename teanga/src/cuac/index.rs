use std::collections::HashMap;
use lru::LruCache;
use thiserror::Error;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;

/// The result of an index
#[derive(Debug, Clone, PartialEq)]
pub enum IndexResult {
    /// An index value
    Index(u32),
    /// The string (if the string is not in the index)
    String(String)
}


/// An index for strings used to store values in a Cuac file
#[derive(Debug, Clone)]
pub struct Index {
    map : Arc<RwLock<HashMap<String, u32>>>,
    vec : Arc<RwLock<Vec<String>>>,
    cache : Arc<RwLock<LruCache<String, u32>>>,
    frozen : bool
}

impl Index {
    /// Create a new index
    pub fn new() -> Index {
        Index {
            map : Arc::new(RwLock::new(HashMap::new())),
            vec : Arc::new(RwLock::new(Vec::new())),
            cache : Arc::new(RwLock::new(LruCache::new(std::num::NonZeroUsize::new(1_000_000).unwrap()))),
            frozen: false
        }
    }

    /// Create an index from values
    ///
    /// # Arguments
    ///
    /// * `map` - The map of strings to indices
    /// * `vec` - The vector of strings
    /// * `cache` - The cache of strings
    ///
    /// # Returns
    ///
    /// A new index object
    pub fn from_values(map : HashMap<String, u32>,
        vec : Vec<String>, 
        cache : Vec<String>) -> Index {
        let i = Index {
            map: Arc::new(RwLock::new(map)),
            vec: Arc::new(RwLock::new(vec)),
            cache : Arc::new(RwLock::new(LruCache::new(std::num::NonZeroUsize::new(1_000_000).unwrap()))),
            frozen: false
        };
        for v in cache {
            i.cache.write().unwrap().put(v, 0);
        }
        i
    }

    /// Convert the index into its values
    pub fn into_values(self) -> Result<(HashMap<String, u32>, Vec<String>, Vec<String>), &'static str> {
        let mut cache = Vec::new();
        for (k, _) in self.cache.read().unwrap().iter().rev() {
            cache.push(k.clone());
        }
        let map = Arc::<RwLock<HashMap<std::string::String, u32>>>::try_unwrap(self.map)
            .map_err(|_| "Cannot unwrap map")?;
        let vec = Arc::<RwLock<Vec<std::string::String>>>::try_unwrap(self.vec)
            .map_err(|_| "Cannot unwrap vec")?;
        Ok((map.into_inner().unwrap(), vec.into_inner().unwrap(), cache))
    }

    fn cache_has(&self, str : &String) -> bool {
        self.cache.write().unwrap().get(str).is_some()
    }

    /// Get the index of a string
    pub fn idx(&self, str : &String) -> IndexResult {
        if let Some(idx) = self.map.read().unwrap().get(str) {
            return IndexResult::Index(*idx);
        }
        if self.frozen {
            return IndexResult::String(str.clone());
        }
        if self.cache_has(str) {
            let idx = self.vec.read().unwrap().len() as u32;
            self.map.write().unwrap().insert(str.clone(), idx);
            self.vec.write().unwrap().push(str.clone());
            self.cache.write().unwrap().pop(str);
            return IndexResult::String(str.clone());
        } else {
            self.cache.write().unwrap().put(str.clone(), 0);
            return IndexResult::String(str.clone());
        }
    }

    /// Get the string at an index
    pub fn str(&self, idx : u32) -> Option<String> {
        if idx < self.vec.read().unwrap().len() as u32 {
            Some(self.vec.read().unwrap()[idx as usize].clone())
        } else {
            None
        }
    }

    /// Get the vector of strings directly
    pub fn vec(&self) -> RwLockReadGuard<Vec<String>> {
        self.vec.read().unwrap()
    }

    /// Convert this to bytes
    ///
    /// Note this does not include the cache, so should
    /// only be used for serialization
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for string in self.vec.read().unwrap().iter() {
            bytes.extend(string.as_bytes());
            bytes.push(0);
        }
        bytes
    }

    /// Read this from a list of strings
    ///
    /// Note that this returns the index in a frozen state
    pub fn from_bytes<R: std::io::Read>(r : R) -> Result<Index, FromBytesError> {
        let mut map = HashMap::new();
        let mut bytes = Vec::new();
        let mut vec = Vec::new();
        let mut idx = 0;
        for byte in r.bytes() {
            let byte = byte?;
            if byte == 0 {
                let s = String::from_utf8(bytes)?;
                bytes = Vec::new();
                map.insert(s.clone(), idx);
                vec.push(s);
                idx += 1;
            } else {
                bytes.push(byte);
            }
        }
        Ok(Index {
            map: Arc::new(RwLock::new(map)),
            vec: Arc::new(RwLock::new(vec)),
            cache : Arc::new(RwLock::new(LruCache::new(std::num::NonZeroUsize::new(1_000_000).unwrap()))),
            frozen: true
        })
    }

    /// Freeze the index. Future calls will not update the index
    pub fn freeze(&self) -> Index {
        Index {
            map: self.map.clone(),
            vec: self.vec.clone(),
            cache: self.cache.clone(),
            frozen: true
        }
    }
}

#[derive(Error, Debug)]
pub enum FromBytesError {
    #[error("UTF-8 error: {0}")]
    UTF8Error(#[from] std::string::FromUtf8Error),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_values() {
        let index = Index::new();
        index.idx(&"a".to_string());
        index.idx(&"b".to_string());
        index.idx(&"c".to_string());
        index.idx(&"a".to_string());
        index.idx(&"a".to_string());
        let (map, vec, cache) = index.into_values().unwrap();
        let index = Index::from_values(map, vec, cache);
        let (map, vec, cache) = index.into_values().unwrap();
        assert_eq!(map.into_iter().collect::<Vec<_>>(),
            vec![("a".to_string(), 0)]);
        assert_eq!(vec, vec!["a".to_string()]);
        assert_eq!(cache, vec!["b".to_string(), "c".to_string()]);
    }
} 
