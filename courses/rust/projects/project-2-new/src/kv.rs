use failure::Fail;
use std::collections::HashMap;
use std::path::Path;

/// In memory kv store
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

/// KvStoreError
#[derive(Debug, Fail)]
pub enum KvStoreError {
    /// Invalid key
    #[fail(display = "Invalid key: {}", key)]
    InvalidKey {
        /// Key
        key: String
    }
}

/// KvStore implementation
impl KvStore {
    fn new() -> Self {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// Creates a KV store
    pub fn open(path: &Path) -> Result<KvStore, KvStoreError> {
        unimplemented!("open() not implemented")
    }

    /// Get a value using the key
    pub fn get(&self, key: String) -> Result<Option<String>, KvStoreError> {
        match self.map.get(&key).map(|v| v.to_owned()) {
            Some(v) => Ok(Some(v)),
            None => Err(KvStoreError::InvalidKey { key })
        }
    }

    /// Set a value
    pub fn set(&mut self, key: String, value: String) -> Result<(), KvStoreError> {
        self.map.insert(key, value);
        Ok(())
    }

    /// Remove a value with the key
    pub fn remove(&mut self, key: String) -> Result<(), KvStoreError> {
        self.map.remove(&key);
        Ok(())
    }
}
