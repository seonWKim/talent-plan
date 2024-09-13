#![deny(missing_docs)]
//! A simple key/value store.

pub use kv::{KvStore, KvStoreError};

mod kv;
mod wal;
