use crate::{Layer, LayerDesc, Document};
use std::collections::HashMap;
use ciborium::into_writer;
use std::io::Write;
use thiserror::Error;
use crate::{TeangaResult, TeangaError, DocumentContent, IntoLayer, Corpus};

use crate::tcf::TCF_VERSION;
use crate::tcf::TCFConfig;
use crate::tcf::StringCompressionMethod;
use crate::tcf::TCFResult;
use crate::tcf::index::Index;
use crate::tcf::layer::TCFLayer;
use crate::tcf::layer::TCF_EMPTY_LAYER;
use crate::tcf::string::StringCompression;
use crate::tcf::string::ShocoCompression;
use crate::tcf::string::SupportedStringCompression;
use crate::tcf::string::write_shoco_model;


fn layer_to_bytes<C : StringCompression>(layer : &Layer, idx : &mut Index, 
    ld : &LayerDesc, c : &C) -> TCFResult<Vec<u8>> {
    Ok(TCFLayer::from_layer(layer, idx, ld, c)?.into_bytes(c))
}


/// Convert document content to bytes
///
/// # Arguments
///
/// * `content` - The content of the document
/// * `meta_keys` - The keys of the layers in the document in serialization order
/// * `meta` - The metadata for the document
/// * `index` - The index for the document
pub fn doc_content_to_bytes<DC: DocumentContent<L>, L : IntoLayer, C : StringCompression>
    (content : DC,
     meta_keys : &Vec<String>,
     meta : &HashMap<String, LayerDesc>,
     index : &mut Index,
     c : &C) -> TeangaResult<Vec<u8>> {
    let content = content.as_map(meta)?;
    let mut out = Vec::new();
    for key in meta_keys.iter() {
        if let Some(layer) = content.get(key) {
            let b = layer_to_bytes(&layer,
                index, meta.get(key).unwrap(), c)?;
            out.extend(b.as_slice());
        } else {
            // TCF uses the first byte to identify the layer type, starting
            // from 0, so we use this to indicate a missing layer
            out.push(TCF_EMPTY_LAYER);
        }
    }
    Ok(out)
}


/// An error writing the TCF file
#[derive(Error, Debug)]
pub enum TCFWriteError {
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Teanga error: {0}")]
    TeangaError(#[from] TeangaError)
}

/// Write the corpus to TCF
///
/// # Arguments
///
/// * `out` - The output stream
/// * `corpus` - The corpus to write
pub fn write_tcf<W : Write, C: Corpus>(
    out : &mut W, corpus : &C) -> Result<(), TCFWriteError> {
    write_tcf_with_config(out, corpus, &TCFConfig::default())
}

/// Write the corpus to TCF with a configuration
///
/// # Arguments
///
/// * `out` - The output stream
/// * `corpus` - The corpus to write
/// * `config` - The configuration for the TCF
pub fn write_tcf_with_config<W : Write, C: Corpus>(
    out : &mut W, corpus : &C, config : &TCFConfig) -> Result<(), TCFWriteError> {
    write_tcf_header(out, corpus.get_meta())?;
    let string_compression = write_tcf_config(out, &mut corpus.iter_docs(), config)?;
    let mut index = Index::new();
    for doc in corpus.iter_docs() {
        write_tcf_doc(out, doc?,
                &mut index, corpus.get_meta(), &string_compression)?;
    }
    Ok(())
}

/// Write only the TCF header.
///
/// This is used for progressive conversion on the command line
///
/// # Arguments
/// 
/// * `out` - The output stream
/// * `corpus` - The corpus to write
///
/// # Returns
///
/// The index and the keys of the layers in the corpus. These are then required
/// to call `write_tcf_doc` for each document
pub fn write_tcf_header<W : Write>(
    out : &mut W, meta : &HashMap<String, LayerDesc>) -> Result<(Index, Vec<String>), TCFWriteError> {
    out.write("TEANGA".as_bytes())?;
    out.write(TCF_VERSION.to_be_bytes().as_ref())?;
    let mut meta_bytes : Vec<u8> = Vec::new();
    into_writer(meta, &mut meta_bytes).unwrap();
    out.write((meta_bytes.len() as u32).to_be_bytes().as_ref())?;
    out.write(meta_bytes.as_slice())?;
    let index = Index::new();
    let mut meta_keys : Vec<String> = meta.keys().cloned().collect();
    meta_keys.sort();
    Ok((index, meta_keys))
}

/// Write the TCF configuration
///
/// # Arguments
///
/// * `out` - The output stream
/// * `corpus` - The corpus to write
/// * `config` - The configuration for the TCF
pub fn write_tcf_config<'a, W : Write>(
    out : &mut W, docs : &mut Box<dyn Iterator<Item=TeangaResult<Document>> + 'a>, config : &TCFConfig) -> Result<SupportedStringCompression, TCFWriteError> {
    let c = match config.string_compression {
        StringCompressionMethod::None => {
            out.write(&[0u8])?;
            SupportedStringCompression::None
        },
        StringCompressionMethod::Smaz => {
            out.write(&[1u8])?;
            SupportedStringCompression::Smaz
        },
        StringCompressionMethod::ShocoDefault => {
            out.write(&[2u8])?;
            SupportedStringCompression::Shoco(ShocoCompression::default())
        },
        StringCompressionMethod::GenerateShocoModel(size) => {
            out.write(&[3u8])?;
            let model = ShocoCompression::from_corpus(docs, size)?;
            write_shoco_model(out, &model)?;
            SupportedStringCompression::Shoco(model)
        }
    };
    Ok(c)
}

/// Write TCF header and compression method
///
/// # Arguments
///
/// * `out` - The output stream
/// * `meta` - The metadata for the corpus
/// * `string_compression` - The string compression method
pub fn write_tcf_header_compression<W: Write>(
    out : &mut W, meta : &HashMap<String, LayerDesc>, string_compression : &SupportedStringCompression) -> Result<(), TCFWriteError> {
    out.write("TEANGA".as_bytes())?;
    out.write(TCF_VERSION.to_be_bytes().as_ref())?;
    let mut meta_bytes : Vec<u8> = Vec::new();
    into_writer(meta, &mut meta_bytes).unwrap();
    out.write((meta_bytes.len() as u32).to_be_bytes().as_ref())?;
    out.write(meta_bytes.as_slice())?;
    match string_compression {
        SupportedStringCompression::None => {
            out.write(&[0u8])?;
        },
        SupportedStringCompression::Smaz => {
            out.write(&[1u8])?;
        },
        SupportedStringCompression::Shoco(model) => {
            if *model == ShocoCompression::default() {
                out.write(&[2u8])?;
            } else {
                out.write(&[3u8])?;
                write_shoco_model(out, &model)?;
            }
        }
    }
    Ok(())
}


/// Write a single document as TCF.
///
/// This should be called after `write_tcf_header` to write the document
///
/// # Arguments
///
/// * `out` - The output stream
/// * `doc` - The document to write
/// * `index` - The index for the document
/// * `meta` - The corpus to write
pub fn write_tcf_doc<W : Write, S: StringCompression>(
    out : &mut W, doc : Document, index : &mut Index,
    meta : &HashMap<String, LayerDesc>, s :&S) -> Result<(), TCFWriteError> {
    let mut meta_keys : Vec<String> = meta.keys().cloned().collect();
    meta_keys.sort();
    out.write(doc_content_to_bytes(doc, &meta_keys, meta, index, s)?.as_slice())?;
    Ok(())
}


