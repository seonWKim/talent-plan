use std::collections::HashMap;

pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::new()
        }
    }

    pub fn get(&self, key: String) -> Option<String> {
        match self.map.get(&key) {
            Some(v) => Some(v.to_owned()),
            None => None
        }
    }

    pub fn set(&self, key: String, value: String) {
        self.set(key, value);
    }

    pub fn remove(&self, key: String) {
        self.remove(key);
    }
}
