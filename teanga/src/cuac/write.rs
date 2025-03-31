use crate::{Layer, LayerDesc, Document};
use std::collections::HashMap;
use ciborium::into_writer;
use std::io::Write;
use thiserror::Error;
use crate::{TeangaResult, TeangaError, DocumentContent, IntoLayer, ReadableCorpus};

use crate::cuac::CUAC_VERSION;
use crate::cuac::CuacConfig;
use crate::cuac::StringCompressionMethod;
use crate::cuac::CuacResult;
use crate::cuac::index::Index;
use crate::cuac::layer::CuacLayer;
use crate::cuac::layer::CUAC_EMPTY_LAYER;
use crate::cuac::string::StringCompression;
use crate::cuac::string::ShocoCompression;
use crate::cuac::string::SupportedStringCompression;
use crate::cuac::string::write_shoco_model;


fn layer_to_bytes<C : StringCompression>(layer : &Layer, idx : &mut Index, 
    ld : &LayerDesc, c : &C) -> CuacResult<Vec<u8>> {
    Ok(CuacLayer::from_layer(layer, idx, ld, c)?.into_bytes(c))
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
            // Cuac uses the first byte to identify the layer type, starting
            // from 0, so we use this to indicate a missing layer
            out.push(CUAC_EMPTY_LAYER);
        }
    }
    Ok(out)
}


/// An error writing the Cuac file
#[derive(Error, Debug)]
pub enum CuacWriteError {
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Teanga error: {0}")]
    TeangaError(#[from] TeangaError)
}

/// Write the corpus to Cuac
///
/// # Arguments
///
/// * `out` - The output stream
/// * `corpus` - The corpus to write
pub fn write_cuac<W : Write, C: ReadableCorpus>(
    out : &mut W, corpus : &C) -> Result<(), CuacWriteError> {
    write_cuac_with_config(out, corpus, &CuacConfig::default())
}

/// Write the corpus to Cuac with a configuration
///
/// # Arguments
///
/// * `out` - The output stream
/// * `corpus` - The corpus to write
/// * `config` - The configuration for the Cuac
pub fn write_cuac_with_config<W : Write, C: ReadableCorpus>(
    out : &mut W, corpus : &C, config : &CuacConfig) -> Result<(), CuacWriteError> {
    write_cuac_header(out, &corpus.get_meta())?;

    // The purpose of this is to allow the compression method to read ahead
    // without consuming the iterator. We cache all the documents in memory
    // and then replay them to write documents.
    let replay = std::cell::RefCell::new(Vec::new());
    let do_replay = std::cell::RefCell::new(true);
    let mut iter : Box<dyn Iterator<Item=TeangaResult<Document>>> = Box::new(
        corpus.iter_docs().map(|doc| {
            match doc {
                Ok(doc) => {
                    if *do_replay.borrow() {
                        replay.borrow_mut().push(doc.clone());
                    }
                    Ok(doc)
                },
                Err(err) => {
                    Err(err)
                }
            }
        }));
    let string_compression = write_cuac_config(out, &mut iter, config)?;
    let mut index = Index::new();

    // Now we replay the iterator
    let replay = replay.take();
    for doc in replay {
        write_cuac_doc(out, doc,
                &mut index, &corpus.get_meta(), &string_compression)?;
    }

    // And save the rest of the documents
    *do_replay.borrow_mut() = false;
    for doc in iter {
        write_cuac_doc(out, doc?,
                &mut index, &corpus.get_meta(), &string_compression)?;
    }
    Ok(())
}

/// Write only the Cuac header.
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
/// to call `write_cuac_doc` for each document
pub fn write_cuac_header<W : Write>(
    out : &mut W, meta : &HashMap<String, LayerDesc>) -> Result<(Index, Vec<String>), CuacWriteError> {
    out.write("TEANGA".as_bytes())?;
    out.write(CUAC_VERSION.to_be_bytes().as_ref())?;
    let mut meta_bytes : Vec<u8> = Vec::new();
    into_writer(meta, &mut meta_bytes).unwrap();
    out.write((meta_bytes.len() as u32).to_be_bytes().as_ref())?;
    out.write(meta_bytes.as_slice())?;
    let index = Index::new();
    let mut meta_keys : Vec<String> = meta.keys().cloned().collect();
    meta_keys.sort();
    Ok((index, meta_keys))
}

/// Write the Cuac configuration
///
/// # Arguments
///
/// * `out` - The output stream
/// * `corpus` - The corpus to write
/// * `config` - The configuration for the Cuac
pub fn write_cuac_config<'a, W : Write>(
    out : &mut W, docs : &mut Box<dyn Iterator<Item=TeangaResult<Document>> + 'a>, config : &CuacConfig) -> Result<SupportedStringCompression, CuacWriteError> {
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

/// Write Cuac header and compression method
///
/// # Arguments
///
/// * `out` - The output stream
/// * `meta` - The metadata for the corpus
/// * `string_compression` - The string compression method
pub fn write_cuac_header_compression<W: Write>(
    out : &mut W, meta : &HashMap<String, LayerDesc>, string_compression : &SupportedStringCompression) -> Result<(), CuacWriteError> {
    out.write("TEANGA".as_bytes())?;
    out.write(CUAC_VERSION.to_be_bytes().as_ref())?;
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


/// Write a single document as Cuac.
///
/// This should be called after `write_cuac_header` to write the document
///
/// # Arguments
///
/// * `out` - The output stream
/// * `doc` - The document to write
/// * `index` - The index for the document
/// * `meta` - The corpus to write
pub fn write_cuac_doc<W : Write, S: StringCompression>(
    out : &mut W, doc : Document, index : &mut Index,
    meta : &HashMap<String, LayerDesc>, s :&S) -> Result<(), CuacWriteError> {
    let mut meta_keys : Vec<String> = meta.keys().cloned().collect();
    meta_keys.sort();
    out.write(doc_content_to_bytes(doc, &meta_keys, meta, index, s)?.as_slice())?;
    Ok(())
}


