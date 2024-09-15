use failure::Fail;
use std::io;
use std::string::FromUtf8Error;

/// Error type for kvs.
#[derive(Fail, Debug)]
pub enum KvsError {
    /// IO error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// Serialization or deserialization error.
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
    /// Removing non-existent key error.
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// Unexpected command type error.
    /// It indicated a corrupted log or a program bug.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,

    /// Sled error
    #[fail(display = "Sled error")]
    Sled
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(_err: sled::Error) -> KvsError { KvsError::Sled }
}

impl From<FromUtf8Error> for KvsError {
    fn from(_err: FromUtf8Error) -> KvsError { KvsError::Sled }
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;
