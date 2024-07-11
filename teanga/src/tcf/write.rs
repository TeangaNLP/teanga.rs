use crate::{Layer, LayerDesc, Document};
use std::collections::HashMap;
use ciborium::into_writer;
use std::io::Write;
use thiserror::Error;
use crate::{TeangaResult, TeangaError, DocumentContent, IntoLayer, Corpus};

use crate::tcf::TCFResult;
use crate::tcf::index::Index;
use crate::tcf::layer::TCFLayer;
use crate::tcf::layer::TCF_EMPTY_LAYER;

fn layer_to_bytes(layer : &Layer, idx : &mut Index, ld : &LayerDesc) -> TCFResult<Vec<u8>> {
    Ok(TCFLayer::from_layer(layer, idx, ld)?.into_bytes())
}


/// Convert document content to bytes
///
/// # Arguments
///
/// * `content` - The content of the document
/// * `meta_keys` - The keys of the layers in the document in serialization order
/// * `meta` - The metadata for the document
/// * `index` - The index for the document
pub fn doc_content_to_bytes<DC: DocumentContent<L>, L : IntoLayer>(content : DC,
    meta_keys : &Vec<String>,
    meta : &HashMap<String, LayerDesc>,
    index : &mut Index) -> TeangaResult<Vec<u8>> {
    let content = content.as_map(meta)?;
    let mut out = Vec::new();
    for key in meta_keys.iter() {
        if let Some(layer) = content.get(key) {
            let b = layer_to_bytes(&layer,
                index, meta.get(key).unwrap())?;
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
    mut out : W, corpus : &C) -> Result<(), TCFWriteError> {
    let mut meta_bytes : Vec<u8> = Vec::new();
    into_writer(corpus.get_meta(), &mut meta_bytes).unwrap();
    out.write((meta_bytes.len() as u32).to_be_bytes().as_ref())?;
    out.write(meta_bytes.as_slice())?;
    let mut index = Index::new();
    let mut meta_keys : Vec<String> = corpus.get_meta().keys().cloned().collect();
    meta_keys.sort();
    for doc in corpus.iter_docs() {
        out.write(doc_content_to_bytes(doc?,
                &meta_keys, corpus.get_meta(), &mut index)?.as_slice())?;
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
pub fn write_tcf_header<W : Write, C : Corpus>(
    mut out : W, corpus : &C) -> Result<(Index, Vec<String>), TCFWriteError> {
    let mut meta_bytes : Vec<u8> = Vec::new();
    into_writer(corpus.get_meta(), &mut meta_bytes).unwrap();
    out.write((meta_bytes.len() as u32).to_be_bytes().as_ref())?;
    out.write(meta_bytes.as_slice())?;
    let index = Index::new();
    let mut meta_keys : Vec<String> = corpus.get_meta().keys().cloned().collect();
    meta_keys.sort();
    Ok((index, meta_keys))
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
/// * `meta_keys` - The keys of the layers in the document in serialization order
/// * `corpus` - The corpus to write
pub fn write_tcf_doc<W : Write, C : Corpus>(
    mut out : W, doc : Document, index : &mut Index, meta_keys: &Vec<String>, corpus : &C) -> Result<(), TCFWriteError> {
    out.write(doc_content_to_bytes(doc, &meta_keys, corpus.get_meta(), index)?.as_slice())?;
    Ok(())
}


