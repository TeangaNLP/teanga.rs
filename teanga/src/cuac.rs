//! Teanga Compressed Format
use thiserror::Error;

mod data;
mod index;
mod read;
mod layer;
mod string;
mod cuac_index;
mod type_index;
mod write;

pub use write::{write_cuac, write_cuac_with_config, write_cuac_header, write_cuac_config, write_cuac_header_compression, write_cuac_doc, doc_content_to_bytes, CuacWriteError};
pub use read::{read_cuac, read_cuac_header, read_cuac_doc, bytes_to_doc, CuacReadError};
pub use index::{Index, IndexResult};
pub use string::{StringCompression, SupportedStringCompression, StringCompressionError, NoCompression, SmazCompression, ShocoCompression};

/// A Cuac Result type
pub type CuacResult<T> = Result<T, CuacError>;

/// Cuac errors
#[derive(Error, Debug)]
pub enum CuacError {
    /// String compression error
    #[error("String compression error: {0}")]
    StringCompressionError(#[from] crate::cuac::string::StringCompressionError),
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
    #[error("Invalid Cuac byte")]
    InvalidByte,
    /// An index was not sorted
    #[error("Index not sorted")]
    IndexNotSorted,
    /// An enum value was invalid
    #[error("Invalid enum value: {0}")]
    InvalidEnumValue(String),
}

/// Configuration for Cuac 
#[derive(Debug, Clone, PartialEq)]
pub struct CuacConfig {
    /// The compression to use for strings
    pub string_compression : StringCompressionMethod
}

impl Default for CuacConfig {
    fn default() -> Self {
        CuacConfig {
            string_compression : StringCompressionMethod::Smaz
        }
    }
}

impl CuacConfig {
    /// Create a new Cuac configuration
    ///
    /// # Arguments
    /// * `string_compression` - The compression method for strings
    ///
    /// # Returns
    /// A new Cuac configuration
    pub fn new() -> CuacConfig {
        CuacConfig {
            string_compression : StringCompressionMethod::Smaz
        }
    }

    pub fn with_string_compression(mut self, sc : StringCompressionMethod) -> CuacConfig {
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

/// The Cuac version for binary compatibility
pub static CUAC_VERSION : u16 = 1;
