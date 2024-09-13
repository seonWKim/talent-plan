use std::fs::OpenOptions;
use std::path::PathBuf;

#[derive(Default)]
pub struct Wal {
    pub path: Option<PathBuf>,
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

        if !wal_files.is_empty() {
            Wal {
                path: wal_files.pop(),
            }
        } else {
            let wal_file_path = dir_path.join("wal.0");
            OpenOptions::new().create(true).write(true).open(&wal_file_path).expect("Unable to create wal file");
            Wal {
                path: Some(wal_file_path),
            }
        }
    }
}
