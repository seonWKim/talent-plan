use crate::error::KvsError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::fs::{metadata, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
    r: bool,   // remove flag
}

/// KvStore implementation
impl KvStore {
    fn new(path: PathBuf) -> Self {
        KvStore {
            dir_path: path.clone(),
            key_offset: HashMap::new(),
            wal: Wal::initialize(path),
        }
    }

    /// Creates a KV store
    pub fn open(dir_path: &Path) -> Result<KvStore, KvsError> {
        let mut store = KvStore::new(dir_path.to_path_buf());
        store.build_index()?;
        Ok(store)
    }

    /// Get a value using the key
    pub fn get(&mut self, key: String) -> Result<Option<String>, KvsError> {
        if let Some(&offset) = self.key_offset.get(&key) {
            let record = self.read_offset(offset)?;
            Ok(Some(record.v))
        } else {
            Ok(None)
        }
    }

    /// Set a value
    pub fn set(&mut self, key: String, value: String) -> Result<(), KvsError> {
        match self.append_to_wal(key.to_owned(), value, false) {
            Ok(offset) => {
                self.key_offset.insert(key, offset);
                Ok(())
            }
            Err(_) => Err(KvsError::KeyNotFound),
        }
    }

    /// Remove a value with the key
    pub fn remove(&mut self, key: String) -> Result<(), KvsError> {
        if !self.key_offset.contains_key(&key) {
            println!("Key not found");
            return Err(KvsError::KeyNotFound);
        }

        self.append_to_wal(key.to_owned(), "".to_string(), true)?;
        self.key_offset.remove(&key);
        Ok(())
    }

    fn append_to_wal(&mut self, key: String, value: String, r: bool) -> Result<u64, KvsError> {
        let record = Record {
            k: key.to_owned(),
            v: value,
            r,
        };
        let serialized = serde_json::to_string(&record)?;
        let serialized_bytes = serialized.as_bytes();

        if self.wal.size + serialized_bytes.len() as u64 > 1_000_000 {
            self.compact()?;
        }

        self.wal.append(&serialized_bytes)
    }

    fn compact(&mut self) -> Result<(), KvsError> {
        // create a new wal
        let mut new_wal = Wal::new(self.dir_path.clone());

        // append to new wal
        for (_, &offset) in &self.key_offset {
            let row = self.wal.read_offset(offset)?;
            new_wal.append(row.value.as_slice())?;
        }

        let old_wal_path = &self.wal.get_path();
        self.wal = new_wal;
        fs::remove_file(old_wal_path)?;

        Ok(())
    }

    fn build_index(&mut self) -> Result<(), KvsError> {
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

    fn read_offset(&mut self, offset: u64) -> Result<Record, KvsError> {
        match self.wal.read_offset(offset) {
            Ok(wal_row) => {
                let record: Record = serde_json::from_slice(&wal_row.value)?;
                Ok(record)
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Wal {
    pub(crate) size: u64,
    path: PathBuf,
    file: Arc<Mutex<File>>,
}

pub struct WalRow {
    pub length: Vec<u8>,
    pub value: Vec<u8>,
}

impl Wal {
    // supports single wal file
    pub(crate) fn new(dir_path: PathBuf) -> Self {
        let next_index = fs::read_dir(&dir_path)
            .expect("Failed to read directory")
            .filter_map(|entry| {
                entry
                    .ok()?
                    .path()
                    .file_name()?
                    .to_str()?
                    .strip_prefix("wal.")?
                    .parse::<u64>()
                    .ok()
            })
            .max()
            .map_or(0, |index| index + 1);

        let wal_file = dir_path.join(format!("wal.{}", next_index));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&wal_file)
            .expect("Unable to open wal file");
        Wal {
            size: 0,
            path: wal_file,
            file: Arc::new(Mutex::new(file)),
        }
    }

    pub fn initialize(dir_path: PathBuf) -> Wal {
        let mut wal_files: Vec<PathBuf> = fs::read_dir(&dir_path)
            .expect("Failed to read directory")
            .filter_map(|entry| {
                let path = entry.ok()?.path();
                if path.is_file() && path.file_name()?.to_str()?.starts_with("wal.") {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();

        wal_files.sort_by_key(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.split('.').nth(1))
                .and_then(|postfix| postfix.parse::<u64>().ok())
                .unwrap_or(0)
        });

        if let Some(wal_file) = wal_files.pop() {
            let size = metadata(&wal_file)
                .expect("Unable to get file metadata")
                .len();
            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .open(&wal_file)
                .expect("Unable to open wal file");
            Wal {
                size,
                path: wal_file,
                file: Arc::new(Mutex::new(file)),
            }
        } else {
            let wal_file_path = dir_path.join("wal.0");
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&wal_file_path)
                .expect("Unable to create wal file");
            Wal {
                size: 0,
                path: wal_file_path,
                file: Arc::new(Mutex::new(file)),
            }
        }
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn set_path(&mut self, path: PathBuf) {
        self.path = path;
    }

    pub fn append(&mut self, bytes: &[u8]) -> Result<u64, KvsError> {
        let mut file = self.file.lock().unwrap();
        let offset = file.seek(SeekFrom::End(0))?;

        let bytes_len = (bytes.len() as u16).to_be_bytes();
        file.write_all(&bytes_len)?;
        file.write_all(bytes)?;
        file.flush()?;
        self.size += (bytes_len.len() + bytes.len()) as u64;

        Ok(offset)
    }

    pub fn read_offset(&mut self, offset: u64) -> Result<WalRow, KvsError> {
        // let mut file = OpenOptions::new().read(true).open(&self.path)?;
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        let mut length_bytes = [0u8; 2];
        file.read_exact(&mut length_bytes)?;
        let length = u16::from_be_bytes(length_bytes) as usize;

        let mut buffer = vec![0u8; length];
        file.read_exact(&mut buffer)?;

        Ok(WalRow {
            length: length_bytes.to_vec(),
            value: buffer,
        })
    }
}
