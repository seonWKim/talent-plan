use crate::KvStoreError;
use std::fs::{metadata, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct Wal {
    size: u64,
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
        let mut wal_files: Vec<PathBuf> = std::fs::read_dir(&dir_path)
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
            let size = metadata(&wal_file).expect("Unable to get file metadata").len();
            let file = OpenOptions::new().read(true).append(true).open(&wal_file).expect("Unable to open wal file");
            Wal {
                size,
                path: wal_file,
                file: Arc::new(Mutex::new(file)),
            }
        } else {
            let wal_file_path = dir_path.join("wal.0");
            let file = OpenOptions::new().create(true).write(true).open(&wal_file_path).expect("Unable to create wal file");
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

    pub fn append(&mut self, bytes: &[u8]) -> Result<u64, KvStoreError> {
        let mut file = self.file.lock().unwrap();
        let offset = file.seek(SeekFrom::End(0))?;

        let bytes_len = (bytes.len() as u16).to_be_bytes();
        file.write_all(&bytes_len)?;
        file.write_all(bytes)?;
        file.flush()?;
        self.size += (bytes_len.len() + bytes.len()) as u64;

        Ok(offset)
    }

    pub fn read_offset(&mut self, offset: u64) -> Result<WalRow, KvStoreError> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
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
