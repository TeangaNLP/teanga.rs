/// Teanga Compressed Format
use smaz;
use thiserror::Error;

mod data;
mod index;
mod read;
mod tcf;
mod tcf_index;
mod type_index;
mod write;

//pub use tcf::{write_tcf, read_tcf, write_tcf_header, write_tcf_doc, doc_content_to_bytes, bytes_to_doc, Index, IndexResult};
pub use write::{write_tcf, write_tcf_header, write_tcf_doc, doc_content_to_bytes};
pub use read::{read_tcf, bytes_to_doc};
pub use index::{Index, IndexResult};

pub type TCFResult<T> = Result<T, TCFError>;

#[derive(Error, Debug)]
pub enum TCFError {
    #[error("Smaz Error: {0}")]
    SmazError(#[from] smaz::DecompressError),
    #[error("Ciborium Error: {0}")]
    CiboriumError(#[from] ciborium::de::Error<std::io::Error>),
    #[error("UTF-8 Error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Invalid TCF byte")]
    InvalidByte,
    #[error("Index not sorted")]
    IndexNotSorted,
    #[error("Invalid enum value: {0}")]
    InvalidEnumValue(String),
}
