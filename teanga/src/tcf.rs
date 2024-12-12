//! Teanga Compressed Format
use thiserror::Error;

mod data;
mod index;
mod read;
mod layer;
mod string;
mod tcf_index;
mod type_index;
mod write;

pub use write::{write_tcf, write_tcf_with_config, write_tcf_header, write_tcf_config, write_tcf_header_compression, write_tcf_doc, doc_content_to_bytes, TCFWriteError};
pub use read::{read_tcf, read_tcf_header, read_tcf_doc, bytes_to_doc, TCFReadError};
pub use index::{Index, IndexResult};
pub use string::{StringCompression, SupportedStringCompression, StringCompressionError, NoCompression, SmazCompression, ShocoCompression};

/// A TCF Result type
pub type TCFResult<T> = Result<T, TCFError>;

/// TCF errors
#[derive(Error, Debug)]
pub enum TCFError {
    /// String compression error
    #[error("String compression error: {0}")]
    StringCompressionError(#[from] crate::tcf::string::StringCompressionError),
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

/// Configuration for TCF 
#[derive(Debug, Clone, PartialEq)]
pub struct TCFConfig {
    /// The compression to use for strings
    pub string_compression : StringCompressionMethod
}

impl Default for TCFConfig {
    fn default() -> Self {
        TCFConfig {
            string_compression : StringCompressionMethod::Smaz
        }
    }
}

impl TCFConfig {
    /// Create a new TCF configuration
    ///
    /// # Arguments
    /// * `string_compression` - The compression method for strings
    ///
    /// # Returns
    /// A new TCF configuration
    pub fn new() -> TCFConfig {
        TCFConfig {
            string_compression : StringCompressionMethod::Smaz
        }
    }

    pub fn with_string_compression(mut self, sc : StringCompressionMethod) -> TCFConfig {
        self.string_compression = sc;
        self
    }
}

/// The compression method for strings
#[derive(Debug, Clone, PartialEq)]
pub enum StringCompressionMethod {
    /// No compression
    None,
    /// Use Smaz
    Smaz,
    /// Use Shoco with default model
    ShocoDefault,
    /// Build a new Shoco model
    GenerateShocoModel(usize)
}

/// The TCF version for binary compatibility
pub static TCF_VERSION : u16 = 1;
