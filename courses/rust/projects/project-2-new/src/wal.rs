use std::path::PathBuf;

#[derive(Default)]
pub struct Wal {
    pub path: Option<PathBuf>,
}

impl Wal {
    // supports single wal file
    pub(crate) fn new(dir_path: PathBuf) -> Self {
        let mut wal_files: Vec<PathBuf> = std::fs::read_dir(dir_path)
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

        Wal {
            path: wal_files.pop(),
        }
    }
}
