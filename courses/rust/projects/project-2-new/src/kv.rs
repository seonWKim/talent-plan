use crate::wal::Wal;
use failure::Fail;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// In memory kv store
pub struct KvStore {
    dir_path: PathBuf,
    key_offset: HashMap<String, u64>,
    wal: Wal,
}

#[derive(Serialize, Deserialize)]
pub struct Record {
    k: String, // key
    v: String, // value
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
    pub fn get(&mut self, key: String) -> Result<Option<String>, KvStoreError> {
        if let Some(&offset) = self.key_offset.get(&key) {
            let record = self.read_offset(offset)?;
            Ok(Some(record.v))
        } else {
            Ok(None)
        }
    }

    /// Set a value
    pub fn set(&mut self, key: String, value: String) -> Result<(), KvStoreError> {
        match self.append_to_wal(key.to_owned(), value, false) {
            Ok(offset) => {
                self.key_offset.insert(key, offset);
                Ok(())
            }
            Err(_) => Err(KvStoreError::WalAppendFailed)
        }
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

    fn append_to_wal(&mut self, key: String, value: String, r: bool) -> Result<u64, KvStoreError> {
        let record = Record { k: key.to_owned(), v: value, r };
        let serialized = serde_json::to_string(&record)?;

        self.wal.append(&serialized.as_bytes())
    }

    fn build_index(&mut self) -> Result<(), KvStoreError> {
        let mut offset = 0;
        loop {
            match self.wal.read_offset(offset) {
                Ok(wal_row) => {
                    let value = wal_row.value;
                    let record: Record = serde_json::from_slice(&value)?;

                    if record.r {
                        self.key_offset.remove(&record.k.to_owned());
                    } else {
                        self.key_offset.insert(record.k.to_owned(), offset);
                    }
                    offset += (wal_row.length.len() + value.len()) as u64;
                }
                Err(_) => break,
            }
        }

        Ok(())
    }

    fn read_offset(&mut self, offset: u64) -> Result<Record, KvStoreError> {
        match self.wal.read_offset(offset) {
            Ok(wal_row) => {
                let record: Record = serde_json::from_slice(&wal_row.value)?;
                Ok(record)
            }
            Err(e) => {
                Err(e)
            }
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

    /// Wal append fail
    #[fail(display = "Wal append failed")]
    WalAppendFailed,

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
