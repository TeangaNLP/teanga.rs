use crate::{Layer, LayerDesc, Document};
use std::collections::HashMap;
use ciborium::into_writer;
use std::io::Write;
use thiserror::Error;
use crate::{TeangaResult, TeangaError, DocumentContent, IntoLayer, Corpus};

use crate::tcf::TCFResult;
use crate::tcf::index::Index;
use crate::tcf::tcf::TCF;
use crate::tcf::tcf::TCF_EMPTY_LAYER;

fn layer_to_bytes(layer : &Layer, idx : &mut Index, ld : &LayerDesc) -> TCFResult<Vec<u8>> {
    Ok(TCF::from_layer(layer, idx, ld)?.into_bytes())
}


pub fn doc_content_to_bytes<DC: DocumentContent<L>, L : IntoLayer>(content : DC,
    meta_keys : &Vec<String>,
    meta : &HashMap<String, LayerDesc>,
    cache : &mut Index) -> TeangaResult<Vec<u8>> {
    let content = content.as_map(meta)?;
    let mut out = Vec::new();
    for key in meta_keys.iter() {
        if let Some(layer) = content.get(key) {
            let b = layer_to_bytes(&layer,
                cache, meta.get(key).unwrap())?;
            out.extend(b.as_slice());
        } else {
            // TCF uses the first byte to identify the layer type, starting
            // from 0, so we use this to indicate a missing layer
            out.push(TCF_EMPTY_LAYER);
        }
    }
    Ok(out)
}


#[derive(Error, Debug)]
pub enum TCFWriteError {
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Teanga error: {0}")]
    TeangaError(#[from] TeangaError)
}

pub fn write_tcf<W : Write, C: Corpus>(
    mut out : W, corpus : &C) -> Result<(), TCFWriteError> {
    let mut meta_bytes : Vec<u8> = Vec::new();
    into_writer(corpus.get_meta(), &mut meta_bytes).unwrap();
    out.write((meta_bytes.len() as u32).to_be_bytes().as_ref())?;
    out.write(meta_bytes.as_slice())?;
    let mut cache = Index::new();
    let mut meta_keys : Vec<String> = corpus.get_meta().keys().cloned().collect();
    meta_keys.sort();
    for doc in corpus.iter_docs() {
        out.write(doc_content_to_bytes(doc?,
                &meta_keys, corpus.get_meta(), &mut cache)?.as_slice())?;
    }
    Ok(())
}

pub fn write_tcf_header<W : Write, C : Corpus>(
    mut out : W, corpus : &C) -> Result<(Index, Vec<String>), TCFWriteError> {
    let mut meta_bytes : Vec<u8> = Vec::new();
    into_writer(corpus.get_meta(), &mut meta_bytes).unwrap();
    out.write((meta_bytes.len() as u32).to_be_bytes().as_ref())?;
    out.write(meta_bytes.as_slice())?;
    let cache = Index::new();
    let mut meta_keys : Vec<String> = corpus.get_meta().keys().cloned().collect();
    meta_keys.sort();
    Ok((cache, meta_keys))
}

pub fn write_tcf_doc<W : Write, C : Corpus>(
    mut out : W, doc : Document, cache : &mut Index, meta_keys: &Vec<String>, corpus : &C) -> Result<(), TCFWriteError> {
    out.write(doc_content_to_bytes(doc, &meta_keys, corpus.get_meta(), cache)?.as_slice())?;
    Ok(())
}


