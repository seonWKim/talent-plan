use crate::wal::Wal;
use failure::Fail;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// In memory kv store
#[derive(Default)]
pub struct KvStore {
    dir_path: PathBuf,
    key_offset: HashMap<String, u64>,
    wal: Wal,
}

#[derive(Serialize, Deserialize)]
pub struct Record {
    key: String,
    value: String,
    r: bool, // remove flag
}

/// KvStore implementation
impl KvStore {
    fn new(path: PathBuf) -> Self {
        KvStore {
            dir_path: path.clone(),
            key_offset: HashMap::new(),
            wal: Wal::new(path),
        }
    }

    /// Creates a KV store
    pub fn open(dir_path: &Path) -> Result<KvStore, KvStoreError> {
        let mut store = KvStore::new(dir_path.to_path_buf());
        store.build_index()?;
        Ok(store)
    }

    /// Get a value using the key
    pub fn get(&self, key: String) -> Result<Option<String>, KvStoreError> {
        let offset = match self.key_offset.get(&key) {
            Some(&offset) => offset,
            None => return Ok(None),
        };

        let record = self.read_offset(offset)?;
        Ok(Some(record.value))
    }

    /// Set a value
    pub fn set(&mut self, key: String, value: String) -> Result<(), KvStoreError> {
        self.append_to_wal(key, value, false)
    }

    /// Remove a value with the key
    pub fn remove(&mut self, key: String) -> Result<(), KvStoreError> {
        if !self.key_offset.contains_key(&key) {
            println!("Key not found");
            return Err(KvStoreError::InvalidKey { key });
        }

        self.append_to_wal(key.to_owned(), "".to_string(), true)?;
        self.key_offset.remove(&key);
        Ok(())
    }

    fn append_to_wal(&mut self, key: String, value: String, r: bool) -> Result<(), KvStoreError> {
        let record = Record { key: key.clone(), value, r };
        let serialized = serde_json::to_string(&record)?;

        match self.wal.append(&serialized.as_bytes()) {
            Ok(offset) => {
                self.key_offset.insert(key.to_string(), offset);
                Ok(())
            }
            Err(e) => Err(e)
        }
    }

    fn build_index(&mut self) -> Result<(), KvStoreError> {
        if self.wal.get_path().is_none() {
            return Ok(());
        }

        let wal_path = self.wal.get_path().clone().unwrap();
        let mut file = OpenOptions::new().read(true).open(wal_path)?;
        let mut offset = 0;

        loop {
            let mut length_bytes = [0u8; 2];
            if file.read_exact(&mut length_bytes).is_err() {
                break;
            }

            let length = u16::from_be_bytes(length_bytes) as usize;

            let mut buffer = vec![0u8; length];
            file.read_exact(&mut buffer)?;

            let record: Record = serde_json::from_slice(&buffer)?;
            if record.r {
                self.key_offset.remove(&record.key);
            } else {
                self.key_offset.insert(record.key, offset);
            }

            offset = file.seek(SeekFrom::Current(0))?
        }

        Ok(())
    }

    fn read_offset(&self, offset: u64) -> Result<Record, KvStoreError> {
        match self.wal.read_offset(offset) {
            Ok(buffer) => {
                let record: Record = serde_json::from_slice(&buffer)?;
                Ok(record)
            }
            Err(e) => Err(e)
        }
    }
}

/// KvStoreError
#[derive(Debug, Fail)]
pub enum KvStoreError {
    /// Invalid key
    #[fail(display = "Invalid key: {}", key)]
    InvalidKey {
        /// Key
        key: String
    },

    /// Invalid offset
    #[fail(display = "Invalid offset: {}", offset)]
    InvalidOffset {
        /// offset
        offset: u64
    },

    /// Wal file not found
    #[fail(display = "Wal file not found")]
    WalNotFound,

    /// IO error
    #[fail(display = "IO error: {}", _0)]
    Io(#[cause] std::io::Error),

    /// Serde error
    #[fail(display = "Serde error: {}", _0)]
    Serde(#[cause] serde_json::Error),
}

impl From<std::io::Error> for KvStoreError {
    fn from(err: std::io::Error) -> KvStoreError {
        KvStoreError::Io(err)
    }
}

impl From<serde_json::Error> for KvStoreError {
    fn from(err: serde_json::Error) -> KvStoreError {
        KvStoreError::Serde(err)
    }
}
