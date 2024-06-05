/// Teanga Compressed Format
use crate::{Layer, Value, LayerDesc, Document, DataType};
use std::collections::HashMap;
use smaz;
use ciborium::{into_writer, from_reader};
use std::io::Write;
use lru::LruCache;
use thiserror::Error;
use crate::{TeangaResult, TeangaError, DocumentContent, IntoLayer, Corpus, WriteableCorpus};
use std::io::BufRead;

enum TCF {
    Characters(Vec<u8>),
    L1(TCFIndex),
    L2(TCFIndex, TCFIndex),
    L3(TCFIndex, TCFIndex, TCFIndex),
    LS(TCFData),
    L1S(TCFIndex, TCFData),
    L2S(TCFIndex, TCFIndex, TCFData),
    L3S(TCFIndex, TCFIndex, TCFIndex, TCFData),
    MetaLayer(Vec<HashMap<String, Value>>)
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexResult {
    Index(u32),
    String(String)
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypeIndex(Vec<u8>, usize);

impl TypeIndex {
    pub fn new() -> TypeIndex {
        TypeIndex(Vec::new(), 0)
    }

    fn append(&mut self, v : bool) {
        if self.1 % 8 == 0 {
            if v {
                self.0.push(0b1000_0000);
            } else {
                self.0.push(0b0000_0000);
            }
            self.1 += 1;
        } else {
            if v {
                self.0[self.1 / 8] |= 0b1000_0000 >> (self.1 % 8);
            }
            self.1 += 1;
        }
    }

    fn to_bytes(self) -> Vec<u8> {
        self.0
    }

    fn from_bytes(data : &[u8], len : usize) -> (TypeIndex, usize) {
        let l = len / 8 + (if len % 8 == 0 { 0 } else { 1 });
        (TypeIndex(data[0..l].to_vec(), len), l)
    }

    fn from_reader<R : BufRead>(input : &mut R, len : usize) -> TCFResult<TypeIndex> {
        let mut buf = vec![0u8; len / 8 + (if len % 8 == 0 { 0 } else { 1 })];
        input.read_exact(&mut buf)?;
        Ok(TypeIndex(buf, len)) 
    }

    fn value(&self, idx : usize) -> bool {
        self.0[idx / 8] & (0b1000_0000 >> (idx % 8)) != 0
    }
}

fn index_results_to_bytes(ir : &Vec<IndexResult>) -> Vec<u8> {
    let mut d = Vec::new();
    let mut type_index = TypeIndex::new();
    for i in ir {
        match i {
            IndexResult::Index(idx) => {
                type_index.append(false);
                if *idx >= 2147482648 {
                    panic!("Index too large");
                }
                d.extend(u32_to_varbytes(*idx));
            }
            IndexResult::String(s) => {
                type_index.append(true);
                let b = smaz::compress(&s.as_bytes());
                d.extend(u32_to_varbytes(b.len() as u32));
                d.extend(b);
            }
        }
    }
    let mut d2 = Vec::new();
    d2.extend(u32_to_varbytes(ir.len() as u32));
    d2.extend(type_index.to_bytes());
    d2.extend(d);
    d2
}

fn bytes_to_index_results(data : &[u8]) -> TCFResult<(Vec<IndexResult>, usize)> {
    let mut results = Vec::new();
    let (len, len1) = varbytes_to_u32(&data[0..]);
    let len = len as usize;
    let (type_index, len2) = TypeIndex::from_bytes(&data[len1..], len);
    let mut offset = len1 + len2;
    while results.len() < len {
        if type_index.value(results.len()) {
            let (n, len3) = varbytes_to_u32(&data[offset..]);
            let s = smaz::decompress(&data[offset + len3..offset + len3 + n as usize])?;
            results.push(IndexResult::String(std::str::from_utf8(s.as_slice())?.to_string()));
            offset += len3 + n as usize;
        } else {
            let (n, len) = varbytes_to_u32(&data[offset..]);
            results.push(IndexResult::Index(n));
            offset += len;
        }
    }
    Ok((results, offset))
}

fn reader_to_index_results<R: BufRead>(input : &mut R) -> TCFResult<Vec<IndexResult>> {
    let mut results = Vec::new();
    let len = read_varbytes(input)? as usize;
    let type_index = TypeIndex::from_reader(input, len)?;
    while results.len() < len {
        if type_index.value(results.len()) {
            let n = read_varbytes(input)? as usize;
            let mut buf = vec![0u8; n];
            input.read_exact(&mut buf)?;
            let s = smaz::decompress(&buf)?;
            results.push(IndexResult::String(std::str::from_utf8(s.as_slice())?.to_string()));
        } else {
            let n = read_varbytes(input)?;
            results.push(IndexResult::Index(n));
        }
    }
    Ok(results)
}

fn to_delta(v : Vec<u32>) -> Vec<u32> {
    let mut l = 0;

    v.into_iter().map(|x| {
        let x2 = x - l;
        l = x;
        x2
    }).collect()
}

fn from_delta(v : Vec<u32>) -> Vec<u32> {
    let mut l = 0;
    v.into_iter().map(|x| {
        l += x;
        l
    }).collect()
}

fn to_diff(v1 : &Vec<u32>, v2 : Vec<u32>) -> Vec<u32> {
    v1.into_iter().zip(v2.iter()).map(|(x,y)| y - x ).collect()
}

fn from_diff(v1 : &Vec<u32>, v2 : Vec<u32>) -> Vec<u32> {
    v1.into_iter().zip(v2.iter()).map(|(x,y)| x + y ).collect()
}

pub static TCF_EMPTY_LAYER : u8 = 0b1111_1111;

impl TCF {
    pub fn from_layer(l : &Layer, idx : &mut Index, ld : &LayerDesc) -> TCFResult<TCF> {
        match l {
            Layer::Characters(c) => Ok(TCF::Characters(smaz::compress(&c.as_bytes()))),
            Layer::L1(l) => Ok(TCF::L1(TCFIndex::from_vec(&l))),
            Layer::L2(l) => {
                let v1 : Vec<u32> = l.iter().map(|s| s.0).collect();
                let v2 : Vec<u32> = l.iter().map(|s| s.1).collect();
                if !v1.iter().zip(v2.iter()).all(|(x,y)| x < y) {
                    return Err(TCFError::IndexNotSorted);
                }
                let v2 = to_diff(&v1, v2);
                let v1 = to_delta(v1);
                Ok(TCF::L2(TCFIndex::from_vec(&v1), TCFIndex::from_vec(&v2)))
            }
            Layer::L3(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| s.1).collect();
                let v3 = l.iter().map(|s| s.2).collect();
                Ok(TCF::L3(TCFIndex::from_vec(&v1), TCFIndex::from_vec(&v2), TCFIndex::from_vec(&v3)))
            }
            Layer::LS(l) => {
                Ok(TCF::LS(
                    TCFData::from_iter(l.iter(), ld, idx)?))
            }
            Layer::L1S(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| &s.1);
                Ok(TCF::L1S(TCFIndex::from_vec(&v1), 
                    TCFData::from_iter(v2, ld, idx)?))
            }
            Layer::L2S(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| s.1).collect();
                let v3 = l.iter().map(|s| &s.2);
                Ok(TCF::L2S(TCFIndex::from_vec(&v1), 
                    TCFIndex::from_vec(&v2), 
                    TCFData::from_iter(v3, ld, idx)?))
            }
            Layer::L3S(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| s.1).collect();
                let v3 = l.iter().map(|s| s.2).collect();
                let v4 = l.iter().map(|s| &s.3);
                Ok(TCF::L3S(TCFIndex::from_vec(&v1), 
                    TCFIndex::from_vec(&v2), 
                    TCFIndex::from_vec(&v3),
                    TCFData::from_iter(v4, ld, idx)?))
            }
            Layer::MetaLayer(l) => Ok(TCF::MetaLayer(l.clone()))
        }
    }

    pub fn to_layer(self, index : &mut Index, ld : &LayerDesc) -> Layer {
        match self {
            TCF::Characters(c) => {
                let d = smaz::decompress(&c).unwrap();
                let s : String = std::str::from_utf8(d.as_slice()).unwrap().to_string();
                Layer::Characters(s)
            },
            TCF::L1(l) => Layer::L1(l.to_vec()),
            TCF::L2(l1, l2) => {
                let v1 = l1.to_vec();
                let v2 = l2.to_vec();
                let v1 = from_delta(v1);
                let v2 = from_diff(&v1, v2);
                Layer::L2(v1.into_iter().zip(v2.into_iter()).map(|(x,y)| (x, y)).collect())
            },
            TCF::L3(l1, l2, l3) => {
                let v1 = l1.to_vec();
                let v2 = l2.to_vec();
                let v3 = l3.to_vec();
                Layer::L3(v1.into_iter().zip(v2.into_iter()).zip(v3.into_iter()).map(|((x,y),z)| (x, y, z)).collect())
            },
            TCF::LS(l) => {
                Layer::LS(l.to_vec(index, ld))
            },
            TCF::L1S(l1, l2) => {
                let v1 = l1.to_vec();
                let v2 = l2.to_vec(index, ld);
                Layer::L1S(v1.into_iter().zip(v2.into_iter()).map(|(x,y)| (x, y)).collect())
            },
            TCF::L2S(l1, l2, l3) => {
                let v1 = l1.to_vec();
                let v2 = l2.to_vec();
                let v3 = l3.to_vec(index, ld);
                Layer::L2S(v1.into_iter().zip(v2.into_iter()).zip(v3.into_iter()).map(|((x,y),z)| (x, y, z)).collect())
            },
            TCF::L3S(l1, l2, l3, l4) => {
                let v1 = l1.to_vec();
                let v2 = l2.to_vec();
                let v3 = l3.to_vec();
                let v4 = l4.to_vec(index, ld);
                Layer::L3S(v1.into_iter().zip(v2.into_iter()).zip(v3.into_iter()).zip(v4.into_iter()).map(|(((x,y),z),w)| (x, y, z, w)).collect())
            },
            TCF::MetaLayer(l) => Layer::MetaLayer(l)
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            TCF::Characters(c) => {
                let mut d = Vec::new();
                d.push(0);
                d.extend((c.len() as u16).to_be_bytes().iter());
                d.extend(c);
                d
            }
            TCF::L1(l) => {
                let mut d = Vec::new();
                d.push(1);
                d.extend(l.into_bytes());
                d
            }
            TCF::L2(l1, l2) => {
                let mut d = Vec::new();
                d.push(2);
                d.extend(l1.into_bytes());
                d.extend(l2.into_bytes());
                d
            }
            TCF::L3(l1, l2, l3) => {
                let mut d = Vec::new();
                d.push(3);
                d.extend(l1.into_bytes());
                d.extend(l2.into_bytes());
                d.extend(l3.into_bytes());
                d
            }
            TCF::LS(l) => {
                let mut d = Vec::new();
                d.push(4);
                d.extend(l.into_bytes());
                d
            }
            TCF::L1S(l1, l2) => {
                let mut d = Vec::new();
                d.push(5);
                d.extend(l1.into_bytes());
                d.extend(l2.into_bytes());
                d
            }
            TCF::L2S(l1, l2, l3) => {
                let mut d = Vec::new();
                d.push(6);
                d.extend(l1.into_bytes());
                d.extend(l2.into_bytes());
                d.extend(l3.into_bytes());
                d
            }
            TCF::L3S(l1, l2, l3, l4) => {
                let mut d = Vec::new();
                d.push(7);
                d.extend(l1.into_bytes());
                d.extend(l2.into_bytes());
                d.extend(l3.into_bytes());
                d.extend(l4.into_bytes());
                d
            }
            TCF::MetaLayer(l) => {
                let mut d = Vec::new();
                d.push(8);
                let mut d2 = Vec::new();
                into_writer(&l, &mut d2).unwrap();
                d.extend((d2.len() as u32).to_be_bytes().iter());
                d.extend(d2);
                d
            }
        }
    }

    pub fn from_bytes(bytes : &[u8], offset : usize, layer_desc : &LayerDesc) -> TCFResult<(TCF, usize)> {
        match bytes[offset] {
            0 => {
                let len = u16::from_be_bytes([bytes[offset + 1], bytes[offset + 2]]) as usize;
                Ok((TCF::Characters(bytes[offset + 1..offset + len + 3].to_vec()), offset + len + 3))
            },
            1 => {
                let (l, len) = TCFIndex::from_bytes(&bytes[offset + 1..])?;
                Ok((TCF::L1(l), offset + len + 1))
            },
            2 => {
                let (l1, len1) = TCFIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = TCFIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                Ok((TCF::L2(l1, l2), offset + len1 + len2 + 1))
            },
            3 => {
                let (l1, len1) = TCFIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = TCFIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = TCFIndex::from_bytes(&bytes[offset + 1 + len1 + len2..])?;
                Ok((TCF::L3(l1, l2, l3), offset + len1 + len2 + len3 + 1))
            },
            4 => {
                let (l, len) = TCFData::from_bytes(&bytes[offset + 1..], layer_desc)?;
                Ok((TCF::LS(l), offset + len + 1))

            },
            5 => {
                let (l1, len1) = TCFIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = TCFData::from_bytes(&bytes[offset + 1 + len1..], layer_desc)?;
                Ok((TCF::L1S(l1, l2), offset + len1 + len2 + 1))
            },
            6 => {
                let (l1, len1) = TCFIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = TCFIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = TCFData::from_bytes(&bytes[offset + 1 + len1 + len2..], layer_desc)?;
                Ok((TCF::L2S(l1, l2, l3), offset + len1 + len2 + len3 + 1))
            },
            7 => {
                let (l1, len1) = TCFIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = TCFIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = TCFIndex::from_bytes(&bytes[offset + 1 + len1 + len2..])?;
                let (l4, len4) = TCFData::from_bytes(&bytes[offset + 1 + len1 + len2 + len3..], layer_desc)?;
                Ok((TCF::L3S(l1, l2, l3, l4), offset + len1 + len2 + len3 + len4 + 1))
            },
            8 => {
                let len = u32::from_be_bytes([bytes[offset + 1], bytes[offset + 2], bytes[offset + 3], bytes[offset + 4]]) as usize;
                let l = from_reader(&bytes[offset + 5..offset + 5 + len])?;
                Ok((TCF::MetaLayer(l), offset + len + 5))
            },
            x => {
                if x == TCF_EMPTY_LAYER {
                    eprintln!("Read empty layer byte in to_layer");
                }
                Err(TCFError::InvalidByte)
            }
        }
    }

    pub fn from_reader<R : BufRead>(bytes : &mut R, layer_desc : &LayerDesc) -> TCFResult<ReadLayerResult<TCF>> {
        let mut buf = vec![0u8; 1];
        match bytes.read_exact(&mut buf) {
            Ok(()) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(ReadLayerResult::Eof);
            },
            Err(e) => {
                return Err(TCFError::IOError(e));
            }
        };
        match buf[0] {
            0 => {
                let mut buf = vec![0u8; 2];
                bytes.read_exact(&mut buf)?;
                let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
                let mut buf = vec![0u8; len];
                bytes.read_exact(&mut buf)?;
                Ok(ReadLayerResult::Layer(TCF::Characters(buf)))
            },
            1 => {
                Ok(ReadLayerResult::Layer(TCF::L1(TCFIndex::from_reader(bytes)?)))
            },
            2 => {
                let l1 = TCFIndex::from_reader(bytes)?;
                let l2 = TCFIndex::from_reader(bytes)?;
                Ok(ReadLayerResult::Layer(TCF::L2(l1, l2)))
            },
            3 => {
                let l1 = TCFIndex::from_reader(bytes)?;
                let l2 = TCFIndex::from_reader(bytes)?;
                let l3 = TCFIndex::from_reader(bytes)?;
                Ok(ReadLayerResult::Layer(TCF::L3(l1, l2, l3)))
            },
            4 => {
                let l = TCFData::from_reader(bytes, layer_desc)?;
                Ok(ReadLayerResult::Layer(TCF::LS(l)))
            },
            5 => {
                let l1 = TCFIndex::from_reader(bytes)?;
                let l2 = TCFData::from_reader(bytes, layer_desc)?;
                Ok(ReadLayerResult::Layer(TCF::L1S(l1, l2)))
            },
            6 => {
                let l1 = TCFIndex::from_reader(bytes)?;
                let l2 = TCFIndex::from_reader(bytes)?;
                let l3 = TCFData::from_reader(bytes, layer_desc)?;
                Ok(ReadLayerResult::Layer(TCF::L2S(l1, l2, l3)))
            },
            7 => {
                let l1 = TCFIndex::from_reader(bytes)?;
                let l2 = TCFIndex::from_reader(bytes)?;
                let l3 = TCFIndex::from_reader(bytes)?;
                let l4 = TCFData::from_reader(bytes, layer_desc)?;
                Ok(ReadLayerResult::Layer(TCF::L3S(l1, l2, l3, l4)))
            },
            8 => {
                let mut buf = vec![0u8; 4];
                bytes.read_exact(&mut buf)?;
                let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                let mut buf = vec![0u8; len];
                bytes.read_exact(&mut buf)?;
                let l = from_reader(&buf[..])?;
                Ok(ReadLayerResult::Layer(TCF::MetaLayer(l)))
            },
            x => {
                if x == TCF_EMPTY_LAYER {
                    Ok(ReadLayerResult::Empty)
                } else {
                    Err(TCFError::InvalidByte)
                }
            }
        }
    }

}

fn layer_to_bytes(layer : &Layer, idx : &mut Index, ld : &LayerDesc) -> TCFResult<Vec<u8>> {
    Ok(TCF::from_layer(layer, idx, ld)?.into_bytes())
}

fn bytes_to_layer(bytes : &[u8], idx : &mut Index, layer_desc : &LayerDesc) -> TCFResult<(Layer, usize)> {
    let (tcf, len) = TCF::from_bytes(bytes, 0, layer_desc)?;
    Ok((tcf.to_layer(idx, layer_desc), len))
}

enum ReadLayerResult<Layer> {
    Layer(Layer),
    Empty,
    Eof
}

fn read_layer<R : BufRead>(bytes : &mut R, idx : &mut Index, layer_desc : &LayerDesc) -> TCFResult<ReadLayerResult<Layer>> {
    match TCF::from_reader(bytes, layer_desc)? {
        ReadLayerResult::Layer(tcf) => Ok(ReadLayerResult::Layer(tcf.to_layer(idx, layer_desc))),
        ReadLayerResult::Empty => Ok(ReadLayerResult::Empty),
        ReadLayerResult::Eof => Ok(ReadLayerResult::Eof)
    }
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

pub fn bytes_to_doc(bytes : &[u8], offset : usize,
    meta_keys : &Vec<String>,
    meta : &HashMap<String, LayerDesc>,
    cache : &mut Index) -> TeangaResult<Document> {
    let mut layers = Vec::new();
    let mut i = offset;
    for key in meta_keys.iter() {
        if bytes[i] != TCF_EMPTY_LAYER {
            let (layer, n) = bytes_to_layer(&bytes[i..], 
                cache, meta.get(key).ok_or_else(|| TeangaError::DocumentKeyError(key.clone()))?)?;
            layers.push((key.clone(), layer));
            i += n;
        } else {
            i += 1;
        }
    }
    Document::new(layers, meta)
}

#[derive(Error, Debug)]
pub enum ReadDocError {
    #[error("Model error: {0}")]
    TeangaError(#[from] TeangaError),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Document key error: {0}")]
    DocumentKeyError(String),
    #[error("TCF error: {0}")]
    TCFError(#[from] TCFError)
}

pub fn read_doc<R : BufRead>(input : &mut R, meta_keys : &Vec<String>,
    meta : &HashMap<String, LayerDesc>, cache : &mut Index) -> Result<Option<Document>, ReadDocError> {
    let mut layers = Vec::new();
    for key in meta_keys.iter() {
        let layer_desc = meta.get(key)
            .ok_or_else(|| ReadDocError::DocumentKeyError(key.clone()))?;
        match read_layer(input, cache, layer_desc)? {
            ReadLayerResult::Layer(layer) => {
                layers.push((key.clone(), layer));
            },
            ReadLayerResult::Empty => {
            },
            ReadLayerResult::Eof => {
                return Ok(None);
            }
        }
    }
    Ok(Some(Document::new(layers, meta)?))
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
    for doc_id in corpus.get_order() {
        let doc = corpus.get_doc_by_id(doc_id)?;
        out.write(doc_content_to_bytes(doc,
                &meta_keys, corpus.get_meta(), &mut cache)?.as_slice())?;
    }
    Ok(())
}

#[derive(Error, Debug)]
pub enum TCFReadError {
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Teanga error: {0}")]
    TeangaError(#[from] TeangaError),
    #[error("Ciborium error: {0}")]
    CiboriumError(#[from] ciborium::de::Error<std::io::Error>),
    #[error("TCF read error: {0}")]
    TCFError(#[from] ReadDocError)
}


pub fn read_tcf<R: std::io::BufRead, C: WriteableCorpus>(
    input : &mut R, corpus : &mut C) -> Result<(), TCFReadError> {
    let mut meta_bytes = vec![0u8; 4];
    input.read_exact(meta_bytes.as_mut_slice())?;
    let len = u32::from_be_bytes([meta_bytes[0], meta_bytes[1], meta_bytes[2], meta_bytes[3]]) as usize;
    let mut meta_bytes = vec![0u8; len];
    input.read_exact(meta_bytes.as_mut_slice())?;
    let meta : HashMap<String, LayerDesc> = from_reader(meta_bytes.as_slice())?;
    corpus.set_meta(meta.clone());
    let mut cache = Index::new();
    let mut meta_keys : Vec<String> = meta.keys().cloned().collect();
    meta_keys.sort();
    while let Some(doc) = read_doc(input, &meta_keys, &meta, &mut cache)? {
        corpus.add_doc(doc)?;
    }
    Ok(())
}

        

pub struct Index {
    map : HashMap<String, u32>,
    pub vec : Vec<String>,
    cache : LruCache<String, u32>
}

impl Index {
    fn new() -> Index {
        Index {
            map : HashMap::new(),
            vec : Vec::new(),
            cache : LruCache::new(std::num::NonZeroUsize::new(1_000_000).unwrap())
        }
    }

    pub fn from_values(map : HashMap<String, u32>,
        vec : Vec<String>, 
        cache : Vec<String>) -> Index {
        let mut i = Index {
            map, vec,
            cache : LruCache::new(std::num::NonZeroUsize::new(1_000_000).unwrap())
        };
        for v in cache {
            i.cache.put(v, 0);
        }
        i
    }

    pub fn into_values(self) -> (HashMap<String, u32>, Vec<String>, Vec<String>) {
        let mut cache = Vec::new();
        for (k, _) in self.cache.iter().rev() {
            cache.push(k.clone());
        }
        (self.map, self.vec, cache)
    }

    fn idx(&mut self, str : &String) -> IndexResult {
        if let Some(idx) = self.map.get(str) {
            return IndexResult::Index(*idx);
        } else if let Some(_) = self.cache.get(str) {
            let idx = self.vec.len() as u32;
            self.map.insert(str.clone(), idx);
            self.vec.push(str.clone());
            self.cache.pop(str);
            return IndexResult::String(str.clone());
        } else {
            self.cache.put(str.clone(), 0);
            return IndexResult::String(str.clone());
        }
    }

    fn str(&self, idx : u32) -> Option<String> {
        if idx < self.vec.len() as u32 {
            Some(self.vec[idx as usize].clone())
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum TCFData {
    String(Vec<IndexResult>),
    Enum(Vec<u32>)
}

impl TCFData {
    pub fn from_iter<'a, I>(iter : I, ld : &LayerDesc,
        idx : &mut Index) -> TCFResult<TCFData> where I : Iterator<Item = &'a String> {
        match ld.data {
            Some(DataType::String) => {
                let v = iter.map(|s| idx.idx(&s)).collect();
                Ok(TCFData::String(v))
            }
            Some(DataType::Enum(ref enum_vals)) => {
                let map : HashMap<String, usize> = enum_vals.iter().enumerate().map(|(i, s)| (s.clone(), i)).collect();
                let mut v = Vec::new();
                for s in iter {
                    if !map.contains_key(s) {
                        return Err(TCFError::InvalidEnumValue(s.clone()));
                    }
                    v.push(map[s] as u32);
                }
                Ok(TCFData::Enum(v))
            }
            Some(DataType::Link) => {
                panic!("Link data type not supported");
            }
            None => {
                panic!("No data type specified");
            }
        }
    }

    pub fn to_vec(&self, index : &mut Index, ld : &LayerDesc) -> Vec<String> {
        match self {
            TCFData::String(v) => {
                v.iter().map(|i| match i {
                    IndexResult::String(s) => {
                        index.idx(s);
                        s.clone()
                    }
                    IndexResult::Index(i) => index.str(*i).unwrap()
                }).collect()
            }
            TCFData::Enum(v) => {
                match ld.data {
                    Some(DataType::Enum(ref enum_vals)) => {
                        v.iter().map(|i| enum_vals[*i as usize].clone()).collect()
                    }
                    _ => {
                        panic!("LayerDesc data type does not match TCFData type");
                    }
                }
            }
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            TCFData::String(v) => {
                index_results_to_bytes(&v)
            }
            TCFData::Enum(v) => {
                TCFIndex::from_vec(&v).into_bytes()
            }
        }
    }

    pub fn from_bytes(data : &[u8], ld : &LayerDesc) -> TCFResult<(TCFData, usize)> {
        match ld.data {
            Some(DataType::String) => {
                let (v, len) = bytes_to_index_results(data)?;
                Ok((TCFData::String(v), len))
            }
            Some(DataType::Enum(_)) => {
                let (v, len) = TCFIndex::from_bytes(data)?;
                Ok((TCFData::Enum(v.to_vec()), len))
            }
            Some(DataType::Link) => {
                panic!("Link data type not supported");
            }
            None => {
                panic!("No data type specified");
            }
        }
    }

    pub fn from_reader<R: BufRead>(input : &mut R, ld : &LayerDesc) -> TCFResult<TCFData> {
        match ld.data {
            Some(DataType::String) => {
                let v = reader_to_index_results(input)?;
                Ok(TCFData::String(v))
            }
            Some(DataType::Enum(_)) => {
                let v = TCFIndex::from_reader(input)?;
                Ok(TCFData::Enum(v.to_vec()))
            }
            Some(DataType::Link) => {
                panic!("Link data type not supported");
            }
            None => {
                panic!("No data type specified");
            }
        }
    }

}

struct TCFIndex {
    pub precision: u8,
    pub length: usize,
    pub data: Vec<u8>,
}

impl TCFIndex {
    pub fn from_vec(vec : &Vec<u32>) -> TCFIndex {
        let max = vec.iter().max().unwrap();
        let precision = f32::log2((max + 1) as f32).ceil() as u8;
        let length = vec.len();
        let mut data = Vec::new();
        let mut offset = 0u8;
        let mut last = 0u8;
        for i in vec {
            let bytes = i.to_be_bytes();
            if precision <= 8 {
                offset = push_byte_partial(bytes[3], &mut data, offset, &mut last, precision);
            } else if precision <= 16 {
                offset = push_byte_partial(bytes[2], &mut data, offset, &mut last, precision % 8);
                offset = push_byte_partial(bytes[3], &mut data, offset, &mut last, 8);
            } else if precision <= 24 {
                offset = push_byte_partial(bytes[1], &mut data, offset, &mut last, precision % 8);
                offset = push_byte_partial(bytes[2], &mut data, offset, &mut last, 8);
                offset = push_byte_partial(bytes[3], &mut data, offset, &mut last, 8);
            } else {
                offset = push_byte_partial(bytes[0], &mut data, offset, &mut last, precision % 8);
                offset = push_byte_partial(bytes[1], &mut data, offset, &mut last, 8);
                offset = push_byte_partial(bytes[2], &mut data, offset, &mut last, 8);
                offset = push_byte_partial(bytes[3], &mut data, offset, &mut last, 8);
            }
        }
        if offset != 0 {
            data.push(last);
        }
        TCFIndex {
            precision,
            length,
            data,
        }
    }

    pub fn to_vec(&self) -> Vec<u32> {
        let mut vec = Vec::new();
        let mut offset = 0usize;
        for _ in 0..self.length {
            let mut bytes = [0u8, 0u8, 0u8, 0u8];
            if self.precision <= 8 {
                bytes[3] = read_byte_partial(&self.data, offset, self.precision);
                offset += self.precision as usize;
            } else if self.precision <= 16 {
                bytes[2] = read_byte_partial(&self.data, offset, self.precision % 8);
                offset += (self.precision % 8) as usize;
                bytes[3] = read_byte_partial(&self.data, offset, 8);
                offset += 8;
            } else if self.precision <= 24 {
                bytes[1] = read_byte_partial(&self.data, offset, self.precision % 8);
                offset += (self.precision % 8) as usize;
                bytes[2] = read_byte_partial(&self.data, offset, 8);
                offset += 8;
                bytes[3] = read_byte_partial(&self.data, offset, 8);
                offset += 8;
            } else {
                bytes[0] = read_byte_partial(&self.data, offset, self.precision % 8);
                offset += (self.precision % 8) as usize;
                bytes[1] = read_byte_partial(&self.data, offset, 8);
                offset += 8;
                bytes[2] = read_byte_partial(&self.data, offset, 8);
                offset += 8;
                bytes[3] = read_byte_partial(&self.data, offset, 8);
                offset += 8;
            }
            vec.push(u32::from_be_bytes(bytes));
        }
        vec
    }

    pub fn into_bytes(self) -> Vec<u8> {
        let mut d = Vec::new();
        d.push(self.precision);
        d.extend((self.length as u32).to_be_bytes().iter());
        d.extend(self.data.iter());
        let n_bits = self.length * self.precision as usize;
        let n_bytes = (n_bits + 7) / 8;
        assert_eq!(d.len(), 5 + n_bytes);
        d
    }

    pub fn from_bytes(bytes : &[u8]) -> TCFResult<(TCFIndex, usize)> {
        let precision = bytes[0];
        let length = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        let n_bits = length * precision as usize;
        let n_bytes = (n_bits + 7) / 8;
        let data = bytes[5..5+n_bytes].to_vec();
        Ok((TCFIndex {
            precision,
            length,
            data,
        }, 5 + length))
    }

    pub fn from_reader<R : BufRead>(input : &mut R) -> TCFResult<TCFIndex> {
        let mut buf = vec![0u8; 5];
        input.read_exact(&mut buf)?;
        let precision = buf[0];
        let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        let n_bits = length * precision as usize;
        let n_bytes = (n_bits + 7) / 8;
        let mut buf = vec![0u8; n_bytes];
        input.read_exact(&mut buf)?;
        Ok(TCFIndex {
            precision,
            length,
            data: Vec::from(buf)
        })
    }
}

fn push_byte_partial(b : u8, data : &mut Vec<u8>, offset : u8, last : &mut u8, precision : u8) -> u8 {
    if offset == 0 {
        *last = b << (8 - precision);
        if precision == 8 {
            data.push(*last);
            return 0;
        } else {    
            return precision;
        }
    } else {
        let b2 = b << (8 - precision);
        *last |= b2 >> offset;
        if offset + precision < 8 {
            return offset + precision;
        } else {
            data.push(*last);
            *last = b2 << (8 - offset);
            return (offset + precision) % 8;
        }
    }
}

fn read_byte_partial(data : &Vec<u8>, offset : usize, precision : u8) -> u8 {
    let b = data[offset / 8];
    let o = (offset % 8) as u8;
    let b = if o + precision <= 8 {
        b >> (8 - o - precision)
    } else {
        let b2 = data[offset / 8 + 1];
        (b << (precision + o - 8)) | (b2 >> (16 - precision - o))
    };
    if precision == 0 {
        return 0;
    } else if precision == 1 {
        return b & 0b0000_0001;
    } else if precision == 2 {
        return b & 0b0000_0011;
    } else if precision == 3 {
        return b & 0b0000_0111;
    } else if precision == 4 {
        return b & 0b0000_1111;
    } else if precision == 5 {
        return b & 0b0001_1111;
    } else if precision == 6 {
        return b & 0b0011_1111;
    } else if precision == 7 {
        return b & 0b0111_1111;
    } else {
        return b;
    }
}

fn u32_to_varbytes(n : u32) -> Vec<u8> {
    let bytes = n.to_be_bytes();
    if n < 128 {
        vec![bytes[3]]
    } else if n < 16384 {
        vec![bytes[2] << 1 | bytes[3] >> 7 | 0b1000_0000, 
            bytes[3] & 0b0111_1111]
    } else if n < 2097152 {
        vec![bytes[1] << 2 | bytes[2] >> 6 | 0b1000_0000,
            bytes[2] << 1 | bytes[3] >> 7 | 0b1000_0000,
            bytes[3] & 0b0111_1111]
    } else if n < 268435456 {
        vec![bytes[0] << 3 | bytes[1] >> 5 | 0b1000_0000,
            bytes[1] << 2 | bytes[2] >> 6 | 0b1000_0000,
            bytes[2] << 1 | bytes[3] >> 7 | 0b1000_0000,
            bytes[3] & 0b0111_1111]
    } else {
        vec![bytes[0] | 0b1000_0000 >> 4,
            bytes[0] << 3 | bytes[1] >> 5 | 0b1000_0000,
            bytes[1] << 2 | bytes[2] >> 6 | 0b1000_0000,
            bytes[2] << 1 | bytes[3] >> 7 | 0b1000_0000,
            bytes[3] & 0b0111_1111]
    }
}

fn varbytes_to_u32(bytes : &[u8]) -> (u32,usize) {
    let mut n = 0u32;
    let mut len = 0;
    for b in bytes.iter() {
        n <<= 7;
        n += (b & 0b0111_1111) as u32;
        len += 1;
        if *b & 0b1000_0000 == 0 {
            break;
        }
    }
    (n, len)
}

fn read_varbytes<R : BufRead>(input : &mut R) -> std::io::Result<u32> {
    let mut bytes = Vec::new();
    loop {
        let mut buf = [0u8; 1];
        input.read_exact(&mut buf)?;
        bytes.push(buf[0]);
        if buf[0] & 0b1000_0000 == 0 {
            break;
        }
    }
    Ok(varbytes_to_u32(&bytes).0)
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcf_index() {
        let vec = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let tcf = TCFIndex::from_vec(&vec);
        assert_eq!(tcf.precision, 4);
        assert_eq!(tcf.length, 10);
        assert_eq!(tcf.data, vec![0b0000_0001, 0b0010_0011, 0b0100_0101, 0b0110_0111, 0b1000_1001]);
    }


    #[test]
    fn test_tcf_index2() {
        let vec = vec![0, 1, 2, 3, 4, 5, 6];
        let tcf = TCFIndex::from_vec(&vec);
        assert_eq!(tcf.precision, 3);
        assert_eq!(tcf.length, 7);
        assert_eq!(tcf.data, vec![0b0000_0101, 0b0011_1001, 0b0111_0000]);
    }

    #[test]
    fn test_tcf_index3() {
        let vec = vec![1, 1000];
        let tcf = TCFIndex::from_vec(&vec);
        assert_eq!(tcf.precision, 10);
        assert_eq!(tcf.length, 2);
        assert_eq!(tcf.data, vec![0b0000_0000, 0b0111_1110, 0b1000_0000]);
    }

    #[test]
    fn test_tcf_to_vec() {
        let vec = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let tcf = TCFIndex::from_vec(&vec);
        let vec2 = tcf.to_vec();
        assert_eq!(vec, vec2);
    }

    #[test]
    fn test_tcf_to_vec2() {
        let vec = vec![0, 1, 2, 3, 4, 5, 6];
        let tcf = TCFIndex::from_vec(&vec);
        let vec2 = tcf.to_vec();
        assert_eq!(vec, vec2);
    }

    #[test]
    fn test_tcf_to_vec3() {
        let vec = vec![1, 1000];
        let tcf = TCFIndex::from_vec(&vec);
        let vec2 = tcf.to_vec();
        assert_eq!(vec, vec2);
    }

    #[test]
    fn test_var_bytes() {
        for n in vec![0,1,10,100,1000,10000,100000, 1000000, 10000000, 100000000] {
            let bytes = u32_to_varbytes(n);
            let (n2, _) = varbytes_to_u32(&bytes);
            assert_eq!(n, n2);
        }
    }

    #[test]
    fn test_tcf_data_round_trip() {
        let mut index = Index::new();
        let data = TCFData::from_iter(vec![&"a".to_string(),
                                           &"a".to_string(),
                                           &"b".to_string(),
                                           &"a".to_string()].into_iter(), 
            &LayerDesc {
                data: Some(DataType::String),
                ..LayerDesc::default()
            }, &mut index).unwrap();
        let bytes = data.clone().into_bytes();
        let (data2, _) = TCFData::from_bytes(&bytes, &LayerDesc {
            data: Some(DataType::String),
            ..LayerDesc::default()
        }).unwrap();
        assert_eq!(data, data2);
    }

    #[test]
    fn test_index_values() {
        let mut index = Index::new();
        index.idx(&"a".to_string());
        index.idx(&"b".to_string());
        index.idx(&"c".to_string());
        index.idx(&"a".to_string());
        index.idx(&"a".to_string());
        let (map, vec, cache) = index.into_values();
        let index = Index::from_values(map, vec, cache);
        let (map, vec, cache) = index.into_values();
        assert_eq!(map.into_iter().collect::<Vec<_>>(),
            vec![("a".to_string(), 0)]);
        assert_eq!(vec, vec!["a".to_string()]);
        assert_eq!(cache, vec!["b".to_string(), "c".to_string()]);
    }
    
    use crate::{SimpleCorpus, build_layer, LayerType, DataType};

    #[test]
    fn test_read_doc() {
        let mut corpus = SimpleCorpus::new();
        build_layer(&mut corpus, "text").add().unwrap();
        build_layer(&mut corpus, "document")
            .layer_type(LayerType::div)
            .base("characters")
            .default(Layer::L1(vec![0]))
            .add().unwrap();
        build_layer(&mut corpus, "url")
            .layer_type(LayerType::seq)
            .base("document")
            .data(DataType::String)
            .add().unwrap();
        println!("str: {:?}", smaz::compress(b"Beginners BBQ Class Taking Place in Missoula!\nDo you want to get better at making delicious BBQ? You will have the opportunity, put this on your calendar now. Thursday, September 22nd join World Class BBQ Champion, Tony Balay from Lonestar Smoke Rangers. He will be teaching a beginner level class for everyone who wants to get better with their culinary skills.\nHe will teach you everything you need to know to compete in a KCBS BBQ competition, including techniques, recipes, timelines, meat selection and trimming, plus smoker and fire information.\nThe cost to be in the class is $35 per person, and for spectators it is free. Included in the cost will be either a t-shirt or apron and you will be tasting samples of each meat that is prepared."));
        let doc_id = corpus.add_doc(
            vec![("text".to_string(), 
                "Beginners BBQ Class Taking Place in Missoula!\nDo you want to get better at making delicious BBQ? You will have the opportunity, put this on your calendar now. Thursday, September 22nd join World Class BBQ Champion, Tony Balay from Lonestar Smoke Rangers. He will be teaching a beginner level class for everyone who wants to get better with their culinary skills.\nHe will teach you everything you need to know to compete in a KCBS BBQ competition, including techniques, recipes, timelines, meat selection and trimming, plus smoker and fire information.\nThe cost to be in the class is $35 per person, and for spectators it is free. Included in the cost will be either a t-shirt or apron and you will be tasting samples of each meat that is prepared.".to_string()),
                ("url".to_string(),
                "https://klyq.com/beginners-bbq-class-taking-place-in-missoula/".to_string())
            ]).unwrap();
        let mut doc = corpus.get_doc_by_id(&doc_id).unwrap();
        doc.set("url", Layer::LS(vec!["https://klyq.com/beginners-bbq-class-taking-place-in-missoula/".to_string()]));
        let mut data : Vec<u8> = Vec::new();
        write_tcf(&mut data, &corpus).unwrap();
        let mut corpus2 = SimpleCorpus::new();
        read_tcf(&mut data.as_slice(), &mut corpus2).unwrap();
        assert_eq!(corpus, corpus2);
    }

    #[test]
    fn test_read_doc_2() {
        let mut corpus = SimpleCorpus::new();
        build_layer(&mut corpus, "text").add().unwrap();
        let _doc_id = corpus.add_doc(vec![(
            "text".to_string(),
            "Test string".to_string())]).unwrap();
        let mut data : Vec<u8> = Vec::new();
        write_tcf(&mut data, &corpus).unwrap();
        assert_eq!(data, vec![0, 0, 0, 23, 161, 100, 116, 101, 120, 116, 161, 100, 116, 121, 112, 101, 106, 99, 104, 97, 114, 97, 99, 116, 101, 114, 115, 0, 0, 7, 254, 84, 54, 35, 77, 114, 84]);
        let mut corpus2 = SimpleCorpus::new();
        read_tcf(&mut data.as_slice(), &mut corpus2).unwrap();
    }

    #[test]
    fn test_serialize_3() {
        let mut corpus = SimpleCorpus::new();
        build_layer(&mut corpus, "text").add().unwrap();
        build_layer(&mut corpus, "document")
            .layer_type(LayerType::div)
            .base("characters")
            .default(Layer::L1(vec![0]))
            .add().unwrap();
        build_layer(&mut corpus, "url")
            .layer_type(LayerType::seq)
            .base("document")
            .data(DataType::String)
            .add().unwrap();
        build_layer(&mut corpus, "timestamp")
            .layer_type(LayerType::seq)
            .base("document")
            .data(DataType::String)
            .add().unwrap();
        build_layer(&mut corpus, "words")
            .layer_type(LayerType::span)
            .base("characters")
            .add().unwrap();
        build_layer(&mut corpus, "pos")
            .layer_type(LayerType::seq)
            .base("words")
            .data(DataType::Enum(vec!["ADJ", "ADP", "PUNCT", "ADV", "AUX", 
                    "SYM", "INTJ", "CCONJ", "X", "NOUN", "DET", "PROPN", 
                    "NUM", "VERB", "PART", "PRON", "SPACE", "SCONJ"].into_iter().map(|s| s.to_owned()).collect()))
            .add().unwrap();
        build_layer(&mut corpus, "lemma")
            .layer_type(LayerType::seq)
            .base("words")
            .data(DataType::String)
            .add().unwrap();
        corpus.add_doc(vec![(
            "text".to_string(),
            "Beginners BBQ Class Taking Place in Missoula!\nDo you want to get better at making delicious BBQ? You will have the opportunity, put this on your calendar now. Thursday, September 22nd join World Class BBQ Champion, Tony Balay from Lonestar Smoke Rangers. He will be teaching a beginner level class for everyone who wants to get better with their culinary skills.\nHe will teach you everything you need to know to compete in a KCBS BBQ competition, including techniques, recipes, timelines, meat selection and trimming, plus smoker and fire information.\nThe cost to be in the class is $35 per person, and for spectators it is free. Included in the cost will be either a t-shirt or apron and you will be tasting samples of each meat that is prepared.".into_layer(&corpus.get_meta()["text"]).unwrap()),
            ("timestamp".to_string(),
            vec!["2019-04-25T12:57:54Z"].into_layer(&corpus.get_meta()["timestamp"]).unwrap()),
            ("url".to_string(),
            vec!["https://klyq.com/beginners-bbq-class-taking-place-in-missoula/"].into_layer(&corpus.get_meta()["url"]).unwrap()),
            ("words".to_string(),
             vec![(0, 9), (10, 13), (14, 19), (20, 26), (27, 32), (33, 35), (36, 44), (44, 45), (45, 46), (46, 48), (49, 52), (53, 57), (58, 60), (61, 64), (65, 71), (72, 74), (75, 81), (82, 91), (92, 95), (95, 96), (97, 100), (101, 105), (106, 110), (111, 114), (115, 126), (126, 127), (128, 131), (132, 136), (137, 139), (140, 144), (145, 153), (154, 157), (157, 158), (159, 167), (167, 168), (169, 178), (179, 183), (184, 188), (189, 194), (195, 200), (201, 204), (205, 213), (213, 214), (215, 219), (220, 225), (226, 230), (231, 239), (240, 245), (246, 253), (253, 254), (255, 257), (258, 262), (263, 265), (266, 274), (275, 276), (277, 285), (286, 291), (292, 297), (298, 301), (302, 310), (311, 314), (315, 320), (321, 323), (324, 327), (328, 334), (335, 339), (340, 345), (346, 354), (355, 361), (361, 362), (362, 363), (363, 365), (366, 370), (371, 376), (377, 380), (381, 391), (392, 395), (396, 400), (401, 403), (404, 408), (409, 411), (412, 419), (420, 422), (423, 424), (425, 429), (430, 433), (434, 445), (445, 446), (447, 456), (457, 467), (467, 468), (469, 476), (476, 477), (478, 487), (487, 488), (489, 493), (494, 503), (504, 507), (508, 516), (516, 517), (518, 522), (523, 529), (530, 533), (534, 538), (539, 550), (550, 551), (551, 552), (552, 555), (556, 560), (561, 563), (564, 566), (567, 569), (570, 573), (574, 579), (580, 582), (583, 584), (584, 586), (587, 590), (591, 597), (597, 598), (599, 602), (603, 606), (607, 617), (618, 620), (621, 623), (624, 628), (628, 629), (630, 638), (639, 641), (642, 645), (646, 650), (651, 655), (656, 658), (659, 665), (666, 667), (668, 669), (669, 670), (670, 675), (676, 678), (679, 684), (685, 688), (689, 692), (693, 697), (698, 700), (701, 708), (709, 716), (717, 719), (720, 724), (725, 729), (730, 734), (735, 737), (738, 746), (746, 747)].into_layer(&corpus.get_meta()["words"]).unwrap()),
             ("pos".to_string(),
              vec!["NOUN", "PROPN", "PROPN", "PROPN", "PROPN", "ADP", "PROPN", "PUNCT", "SPACE", "AUX", "PRON", "VERB", "PART", "VERB", "ADJ", "ADP", "VERB", "ADJ", "PROPN", "PUNCT", "PRON", "AUX", "VERB", "DET", "NOUN", "PUNCT", "VERB", "PRON", "ADP", "PRON", "NOUN", "ADV", "PUNCT", "PROPN", "PUNCT", "PROPN", "NOUN", "VERB", "PROPN", "PROPN", "PROPN", "PROPN", "PUNCT", "PROPN", "PROPN", "ADP", "PROPN", "PROPN", "PROPN", "PUNCT", "PRON", "AUX", "AUX", "VERB", "DET", "NOUN", "NOUN", "NOUN", "ADP", "PRON", "PRON", "VERB", "PART", "VERB", "ADJ", "ADP", "PRON", "ADJ", "NOUN", "PUNCT", "SPACE", "PRON", "AUX", "VERB", "PRON", "PRON", "PRON", "VERB", "PART", "VERB", "PART", "VERB", "ADP", "DET", "PROPN", "PROPN", "NOUN", "PUNCT", "VERB", "NOUN", "PUNCT", "NOUN", "PUNCT", "NOUN", "PUNCT", "NOUN", "NOUN", "CCONJ", "NOUN", "PUNCT", "CCONJ", "NOUN", "CCONJ", "NOUN", "NOUN", "PUNCT", "SPACE", "DET", "NOUN", "PART", "AUX", "ADP", "DET", "NOUN", "AUX", "SYM", "NUM", "ADP", "NOUN", "PUNCT", "CCONJ", "ADP", "NOUN", "PRON", "AUX", "ADJ", "PUNCT", "VERB", "ADP", "DET", "NOUN", "AUX", "AUX", "CCONJ", "DET", "NOUN", "PUNCT", "NOUN", "CCONJ", "NOUN", "CCONJ", "PRON", "AUX", "AUX", "VERB", "NOUN", "ADP", "DET", "NOUN", "PRON", "AUX", "VERB", "PUNCT"].into_layer(&corpus.get_meta()["pos"]).unwrap()),
             ("lemma".to_string(),
              vec!["beginner", "BBQ", "Class", "Taking", "Place", "in", "Missoula", "!", "\n", "do", "you", "want", "to", "get", "well", "at", "make", "delicious", "BBQ", "?", "you", "will", "have", "the", "opportunity", ",", "put", "this", "on", "your", "calendar", "now", ".", "Thursday", ",", "September", "22nd", "join", "World", "Class", "BBQ", "Champion", ",", "Tony", "Balay", "from", "Lonestar", "Smoke", "Rangers", ".", "he", "will", "be", "teach", "a", "beginner", "level", "class", "for", "everyone", "who", "want", "to", "get", "well", "with", "their", "culinary", "skill", ".", "\n", "he", "will", "teach", "you", "everything", "you", "need", "to", "know", "to", "compete", "in", "a", "KCBS", "BBQ", "competition", ",", "include", "technique", ",", "recipe", ",", "timeline", ",", "meat", "selection", "and", "trimming", ",", "plus", "smoker", "and", "fire", "information", ".", "\n", "the", "cost", "to", "be", "in", "the", "class", "be", "$", "35", "per", "person", ",", "and", "for", "spectator", "it", "be", "free", ".", "include", "in", "the", "cost", "will", "be", "either", "a", "t", "-", "shirt", "or", "apron", "and", "you", "will", "be", "taste", "sample", "of", "each", "meat", "that", "be", "prepare", "."].into_layer(&corpus.get_meta()["lemma"]).unwrap())]).unwrap();
        let mut data : Vec<u8> = Vec::new();
        write_tcf(&mut data, &corpus).unwrap();
        let mut corpus2 = SimpleCorpus::new();
        read_tcf(&mut data.as_slice(), &mut corpus2).unwrap();
        for (docid1, docid2) in corpus.get_docs().iter().zip(corpus2.get_docs().iter()) {
            let doc1 = corpus.get_doc_by_id(docid1).unwrap();
            let doc2 = corpus.get_doc_by_id(docid2).unwrap();
            assert_eq!(doc1.keys(), doc2.keys());
            for key in doc1.keys() {
                println!("key: {}", key);
                assert_eq!(doc1[&key], doc2[&key]);
            }
        }
        assert_eq!(corpus.meta, corpus2.meta);
        assert_eq!(corpus.order, corpus2.order);
        assert_eq!(corpus.content.keys().collect::<Vec<&String>>(), corpus2.content.keys().collect::<Vec<&String>>());
        for (k, doc) in corpus.content.iter() {
            let doc2 = corpus2.content.get(k);
            let mut keys1 = doc.content.keys().collect::<Vec<&String>>();
            keys1.sort();
            let mut keys2 = doc2.unwrap().content.keys().collect::<Vec<&String>>();
            keys2.sort();
            assert_eq!(keys1, keys2);
            for (k2, l2) in doc.content.iter() {
                let l = doc2.unwrap().content.get(k2).unwrap();
                assert_eq!(l, l2);
            }
        }
        assert_eq!(corpus.content, corpus2.content);
        //assert_eq!(corpus, corpus2);
     }

    #[test]
    fn test_type_index() {
        let mut type_index = TypeIndex::new();
        let values = vec![false, true, true, false, false, false, true,
            false, true, true, true, false, false];
        for v in values.iter() {
            type_index.append(*v);
        }
        for i in 0..values.len() {
            assert_eq!(type_index.value(i), values[i]);
        }
    }

    #[test]
    fn test_var_bytes2() {
        let i = 16384;
        let bytes = u32_to_varbytes(i);
        println!("{:?}", bytes);
        let i2 = read_varbytes(&mut bytes.as_slice()).unwrap();
        assert_eq!(i, i2);
    }
}
