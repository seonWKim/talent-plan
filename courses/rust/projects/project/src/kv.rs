use std::collections::HashMap;

/// KvStore
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    /// Create new KvStore
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Get a value from key
    pub fn get(&self, key: String) -> Option<String> {
        match self.map.get(&key) {
            Some(s) => Some(s.to_owned()),
            None => None,
        }
    }

    /// Set a key, value
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Remove the key
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
