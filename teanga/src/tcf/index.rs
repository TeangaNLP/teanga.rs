use std::collections::HashMap;
use lru::LruCache;

/// The result of an index
#[derive(Debug, Clone, PartialEq)]
pub enum IndexResult {
    /// An index value
    Index(u32),
    /// The string (if the string is not in the index)
    String(String)
}


/// An index for strings used to store values in a TCF file
pub struct Index {
    map : HashMap<String, u32>,
    vec : Vec<String>,
    cache : LruCache<String, u32>
}

impl Index {
    /// Create a new index
    pub fn new() -> Index {
        Index {
            map : HashMap::new(),
            vec : Vec::new(),
            cache : LruCache::new(std::num::NonZeroUsize::new(1_000_000).unwrap())
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
        let mut i = Index {
            map, vec,
            cache : LruCache::new(std::num::NonZeroUsize::new(1_000_000).unwrap())
        };
        for v in cache {
            i.cache.put(v, 0);
        }
        i
    }

    /// Convert the index into its values
    pub fn into_values(self) -> (HashMap<String, u32>, Vec<String>, Vec<String>) {
        let mut cache = Vec::new();
        for (k, _) in self.cache.iter().rev() {
            cache.push(k.clone());
        }
        (self.map, self.vec, cache)
    }

    /// Get the index of a string
    pub fn idx(&mut self, str : &String) -> IndexResult {
        if let Some(idx) = self.map.get(str) {
            return IndexResult::Index(*idx);
        } else if let Some(_) = self.cache.get(str) {
            let idx = self.vec.len() as u32;
            self.map.insert(str.clone(), idx);
            self.vec.push(str.clone());
            self.cache.pop(str);
            return IndexResult::String(str.clone());
        } else {
            self.cache.put(str.clone(), 0);
            return IndexResult::String(str.clone());
        }
    }

    /// Get the string at an index
    pub fn str(&self, idx : u32) -> Option<String> {
        if idx < self.vec.len() as u32 {
            Some(self.vec[idx as usize].clone())
        } else {
            None
        }
    }

    /// Get the vector of strings directly
    pub fn vec(&self) -> &Vec<String> {
        &self.vec
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_values() {
        let mut index = Index::new();
        index.idx(&"a".to_string());
        index.idx(&"b".to_string());
        index.idx(&"c".to_string());
        index.idx(&"a".to_string());
        index.idx(&"a".to_string());
        let (map, vec, cache) = index.into_values();
        let index = Index::from_values(map, vec, cache);
        let (map, vec, cache) = index.into_values();
        assert_eq!(map.into_iter().collect::<Vec<_>>(),
            vec![("a".to_string(), 0)]);
        assert_eq!(vec, vec!["a".to_string()]);
        assert_eq!(cache, vec!["b".to_string(), "c".to_string()]);
    }
} 
