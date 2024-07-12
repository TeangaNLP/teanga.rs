//! Teanga Compressed Format
use smaz;
use thiserror::Error;

mod data;
mod index;
mod read;
mod layer;
mod tcf_index;
mod type_index;
mod write;

pub use write::{write_tcf, write_tcf_header, write_tcf_doc, doc_content_to_bytes, TCFWriteError};
pub use read::{read_tcf, bytes_to_doc, TCFReadError};
pub use index::{Index, IndexResult};

/// A TCF Result type
pub type TCFResult<T> = Result<T, TCFError>;

/// TCF errors
#[derive(Error, Debug)]
pub enum TCFError {
    /// Smaz error
    #[error("Smaz Error: {0}")]
    SmazError(#[from] smaz::DecompressError),
    /// Ciborium error
    #[error("Ciborium Error: {0}")]
    CiboriumError(#[from] ciborium::de::Error<std::io::Error>),
    /// UTF-8 error
    #[error("UTF-8 Error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    /// Generic I/O error
    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),
    /// A byte was not in the expected range
    #[error("Invalid TCF byte")]
    InvalidByte,
    /// An index was not sorted
    #[error("Index not sorted")]
    IndexNotSorted,
    /// An enum value was invalid
    #[error("Invalid enum value: {0}")]
    InvalidEnumValue(String),
}
