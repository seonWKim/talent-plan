use std::collections::HashMap;

/// In memory kv store
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

/// KvStore implementation
impl KvStore {
    /// Creates a KV store
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Get a value using the key
    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).map(|v| v.to_owned())
    }

    /// Set a value
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Remove a value with the key
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
