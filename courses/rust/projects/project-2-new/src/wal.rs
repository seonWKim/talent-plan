use std::fs::{metadata, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use crate::KvStoreError;

#[derive(Default)]
pub struct Wal {
    size: u64,
    path: Option<PathBuf>,
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
            Wal {
                size,
                path: Some(wal_file),
            }
        } else {
            let wal_file_path = dir_path.join("wal.0");
            OpenOptions::new().create(true).write(true).open(&wal_file_path).expect("Unable to create wal file");
            Wal {
                size: 0,
                path: Some(wal_file_path),
            }
        }
    }

    pub fn get_path(&self) -> Option<PathBuf> {
        self.path.clone()
    }

    pub fn set_path(&mut self, path: PathBuf) {
        self.path = Some(path);
    }

    pub fn append(&mut self, bytes: &[u8]) -> Result<u64, KvStoreError> {
        if self.path.is_none() {
            return Err(KvStoreError::WalNotFound);
        }

        let wal_path = self.path.as_ref().unwrap().clone();
        let mut file = OpenOptions::new().append(true).open(wal_path)?;
        let offset = file.seek(SeekFrom::End(0))?;

        let bytes_len = (bytes.len() as u16).to_be_bytes();
        file.write_all(&bytes_len)?;
        file.write_all(bytes)?;

        self.size += bytes.len() as u64;

        Ok(offset)
    }

    pub fn read_offset(&self, offset: u64) -> Result<Vec<u8>, KvStoreError> {
        let wal_path = self.path.as_ref().ok_or(KvStoreError::WalNotFound)?.clone();
        let mut file = OpenOptions::new().read(true).open(wal_path)?;
        file.seek(SeekFrom::Start(offset))?;

        let mut length_bytes = [0u8; 2];
        file.read_exact(&mut length_bytes);
        let length = u16::from_be_bytes(length_bytes) as usize;

        let mut buffer = vec![0u8; length];
        file.read_exact(&mut buffer)?;

        Ok(buffer) 
    }
}
