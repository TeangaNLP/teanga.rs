/// Teanga Compressed Format
use crate::{Layer, Value, LayerDesc};
use ciborium::{into_writer, from_reader};
use std::io::BufRead;

use crate::cuac::{CuacResult, CuacError};
use crate::cuac::cuac_index::CuacIndex;
use crate::cuac::data::CuacData;
use crate::cuac::index::Index;
use crate::cuac::read::ReadLayerResult;
use crate::cuac::string::StringCompression;


pub static CUAC_EMPTY_LAYER : u8 = 0b1111_1111;

pub enum CuacLayer {
    Characters(Vec<u8>),
    L1(CuacIndex, bool),
    L2(CuacIndex, CuacIndex, bool, bool),
    L3(CuacIndex, CuacIndex, CuacIndex, bool, bool),
    LS(CuacData),
    L1S(CuacIndex, CuacData, bool),
    L2S(CuacIndex, CuacIndex, CuacData, bool, bool),
    L3S(CuacIndex, CuacIndex, CuacIndex, CuacData, bool, bool),
    MetaLayer(Option<Value>)
}

impl CuacLayer {
    pub fn from_layer<S : StringCompression>(l : &Layer, idx : &mut Index, ld : &LayerDesc, s : &S) -> CuacResult<CuacLayer> {
        match l {
            Layer::Characters(c) => Ok(CuacLayer::Characters(s.compress(c))),
            Layer::L1(l) => {
                if all_ascending(l) {
                    Ok(CuacLayer::L1(CuacIndex::from_vec(&to_delta(l.clone())), true))
                } else {
                    Ok(CuacLayer::L1(CuacIndex::from_vec(l), false))
                }
            }
            Layer::L2(l) => {
                let v1 : Vec<u32> = l.iter().map(|s| s.0).collect();
                let v2 : Vec<u32> = l.iter().map(|s| s.1).collect();
                if all_ascending(&v1) {
                    if follows(&v1, &v2) {
                        let v2 = to_diff(&v1, v2);
                        let v1 = to_delta(v1);
                        Ok(CuacLayer::L2(CuacIndex::from_vec(&v1), CuacIndex::from_vec(&v2), true, true))
                    } else {
                        let v1 = to_delta(v1);
                        Ok(CuacLayer::L2(CuacIndex::from_vec(&v1), CuacIndex::from_vec(&v2), true, false))
                    }
                } else {
                    if follows(&v1, &v2) {
                        let v2 = to_diff(&v1, v2);
                        Ok(CuacLayer::L2(CuacIndex::from_vec(&v1), CuacIndex::from_vec(&v2), false, true))
                    } else {
                        Ok(CuacLayer::L2(CuacIndex::from_vec(&v1), CuacIndex::from_vec(&v2), false, false))
                    }
                }
            }
            Layer::L3(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| s.1).collect();
                let v3 = l.iter().map(|s| s.2).collect();
                if all_ascending(&v1) {
                    if follows(&v1, &v2) {
                        let v2 = to_diff(&v1, v2);
                        let v1 = to_delta(v1);
                        Ok(CuacLayer::L3(CuacIndex::from_vec(&v1), CuacIndex::from_vec(&v2), CuacIndex::from_vec(&v3), true, true))
                    } else {
                        let v1 = to_delta(v1);
                        Ok(CuacLayer::L3(CuacIndex::from_vec(&v1), CuacIndex::from_vec(&v2), CuacIndex::from_vec(&v3), true, false))
                    }
                } else {
                    if follows(&v1, &v2) {
                        let v2 = to_diff(&v1, v2);
                        Ok(CuacLayer::L3(CuacIndex::from_vec(&v1), CuacIndex::from_vec(&v2), CuacIndex::from_vec(&v3), false, true))
                    } else {
                        Ok(CuacLayer::L3(CuacIndex::from_vec(&v1), CuacIndex::from_vec(&v2), CuacIndex::from_vec(&v3), false, false))
                    }
                }
            }
            Layer::LS(l) => {
                Ok(CuacLayer::LS(
                    CuacData::from_iter(l.iter(), ld, idx)?))
            }
            Layer::L1S(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| &s.1);
                if all_ascending(&v1) {
                    Ok(CuacLayer::L1S(CuacIndex::from_vec(&to_delta(v1)), 
                        CuacData::from_iter(v2, ld, idx)?, true))
                } else {
                    Ok(CuacLayer::L1S(CuacIndex::from_vec(&v1), 
                        CuacData::from_iter(v2, ld, idx)?, false))
                }
            }
            Layer::L2S(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| s.1).collect();
                let v3 = l.iter().map(|s| &s.2);
                if all_ascending(&v1) {
                    if follows(&v1, &v2) {
                        let v2 = to_diff(&v1, v2);
                        let v1 = to_delta(v1);
                        Ok(CuacLayer::L2S(CuacIndex::from_vec(&v1), 
                            CuacIndex::from_vec(&v2), 
                            CuacData::from_iter(v3, ld, idx)?, true, true))
                    } else {
                        let v1 = to_delta(v1);
                        Ok(CuacLayer::L2S(CuacIndex::from_vec(&v1), 
                            CuacIndex::from_vec(&v2), 
                            CuacData::from_iter(v3, ld, idx)?, true, false))
                    }
                } else {
                    if follows(&v1, &v2) {
                        let v2 = to_diff(&v1, v2);
                        Ok(CuacLayer::L2S(CuacIndex::from_vec(&v1), 
                            CuacIndex::from_vec(&v2), 
                            CuacData::from_iter(v3, ld, idx)?, false, true))
                    } else {
                        Ok(CuacLayer::L2S(CuacIndex::from_vec(&v1), 
                            CuacIndex::from_vec(&v2), 
                            CuacData::from_iter(v3, ld, idx)?, false, false))
                    }
                }
            }
            Layer::L3S(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| s.1).collect();
                let v3 = l.iter().map(|s| s.2).collect();
                let v4 = l.iter().map(|s| &s.3);
                if all_ascending(&v1) {
                    if follows(&v1, &v2) {
                        let v2 = to_diff(&v1, v2);
                        let v1 = to_delta(v1);
                        Ok(CuacLayer::L3S(CuacIndex::from_vec(&v1), 
                            CuacIndex::from_vec(&v2), 
                            CuacIndex::from_vec(&v3), 
                            CuacData::from_iter(v4, ld, idx)?, true, true))
                    } else {
                        let v1 = to_delta(v1);
                        Ok(CuacLayer::L3S(CuacIndex::from_vec(&v1), 
                            CuacIndex::from_vec(&v2), 
                            CuacIndex::from_vec(&v3), 
                            CuacData::from_iter(v4, ld, idx)?, true, false))
                    }
                } else {
                    if follows(&v1, &v2) {
                        let v2 = to_diff(&v1, v2);
                        Ok(CuacLayer::L3S(CuacIndex::from_vec(&v1), 
                            CuacIndex::from_vec(&v2), 
                            CuacIndex::from_vec(&v3), 
                            CuacData::from_iter(v4, ld, idx)?, false, true))
                    } else {
                        Ok(CuacLayer::L3S(CuacIndex::from_vec(&v1), 
                            CuacIndex::from_vec(&v2), 
                            CuacIndex::from_vec(&v3), 
                            CuacData::from_iter(v4, ld, idx)?, false, false))
                    }
                }
            }
            Layer::MetaLayer(l) => Ok(CuacLayer::MetaLayer(l.clone()))
        }
    }

    pub fn to_layer<S : StringCompression>(self, index : &Index, ld : &LayerDesc, s : &S) -> Layer {
        match self {
            CuacLayer::Characters(c) => {
                let s = s.decompress(&c).unwrap();
                Layer::Characters(s)
            },
            CuacLayer::L1(l, delta) => {
                if delta {
                    Layer::L1(from_delta(l.to_vec()))
                } else {
                    Layer::L1(l.to_vec())
                }
            },
            CuacLayer::L2(l1, l2, delta, diff) => {
                let v1 = l1.to_vec();
                let v2 = l2.to_vec();
                let v1 = if delta { from_delta(v1) } else { v1 };
                let v2 = if diff { from_diff(&v1, v2) } else { v2 };
                Layer::L2(v1.into_iter().zip(v2.into_iter()).map(|(x,y)| (x, y)).collect())
            },
            CuacLayer::L3(l1, l2, l3, delta, diff) => {
                let v1 = l1.to_vec();
                let v2 = l2.to_vec();
                let v3 = l3.to_vec();
                let v1 = if delta { from_delta(v1) } else { v1 };
                let v2 = if diff { from_diff(&v1, v2) } else { v2 };
                Layer::L3(v1.into_iter().zip(v2.into_iter()).zip(v3.into_iter()).map(|((x,y),z)| (x, y, z)).collect())
            },
            CuacLayer::LS(l) => {
                Layer::LS(l.to_vec(index, ld))
            },
            CuacLayer::L1S(l1, l2, delta) => {
                let v1 = l1.to_vec();
                let v2 = l2.to_vec(index, ld);
                let v1 = if delta { from_delta(v1) } else { v1 };
                Layer::L1S(v1.into_iter().zip(v2.into_iter()).map(|(x,y)| (x, y)).collect())
            },
            CuacLayer::L2S(l1, l2, l3, delta, diff) => {
                let v1 = l1.to_vec();
                let v2 = l2.to_vec();
                let v3 = l3.to_vec(index, ld);
                let v1 = if delta { from_delta(v1) } else { v1 };
                let v2 = if diff { from_diff(&v1, v2) } else { v2 };
                Layer::L2S(v1.into_iter().zip(v2.into_iter()).zip(v3.into_iter()).map(|((x,y),z)| (x, y, z)).collect())
            },
            CuacLayer::L3S(l1, l2, l3, l4, delta, diff) => {
                let v1 = l1.to_vec();
                let v2 = l2.to_vec();
                let v3 = l3.to_vec();
                let v4 = l4.to_vec(index, ld);
                let v1 = if delta { from_delta(v1) } else { v1 };
                let v2 = if diff { from_diff(&v1, v2) } else { v2 };
                Layer::L3S(v1.into_iter().zip(v2.into_iter()).zip(v3.into_iter()).zip(v4.into_iter()).map(|(((x,y),z),w)| (x, y, z, w)).collect())
            },
            CuacLayer::MetaLayer(l) => Layer::MetaLayer(l)
        }
    }

    pub fn into_bytes<C : StringCompression>(self, c : &C) -> Vec<u8> {
        match self {
            CuacLayer::Characters(c) => {
                let mut d = Vec::new();
                d.push(0);
                d.extend((c.len() as u16).to_be_bytes().iter());
                d.extend(c);
                d
            }
            CuacLayer::L1(l, delta) => {
                let mut d = Vec::new();
                if delta {
                    d.push(1);
                } else {
                    d.push(2);
                }
                d.extend(l.into_bytes());
                d
            }
            CuacLayer::L2(l1, l2, delta, diff) => {
                let mut d = Vec::new();
                if delta && diff {
                    d.push(3);
                } else if delta {
                    d.push(4);
                } else if diff {
                    d.push(5);
                } else {
                    d.push(6);
                }
                d.extend(l1.into_bytes());
                d.extend(l2.into_bytes());
                d
            }
            CuacLayer::L3(l1, l2, l3, delta, diff) => {
                let mut d = Vec::new();
                if delta && diff {
                    d.push(7);
                } else if delta {
                    d.push(8);
                } else if diff {
                    d.push(9);
                } else {
                    d.push(10);
                }
                d.extend(l1.into_bytes());
                d.extend(l2.into_bytes());
                d.extend(l3.into_bytes());
                d
            }
            CuacLayer::LS(l) => {
                let mut d = Vec::new();
                d.push(11);
                d.extend(l.into_bytes(c));
                d
            }
            CuacLayer::L1S(l1, l2, delta) => {
                let mut d = Vec::new();
                if delta {
                    d.push(12);
                } else {
                    d.push(13);
                }
                d.extend(l1.into_bytes());
                d.extend(l2.into_bytes(c));
                d
            }
            CuacLayer::L2S(l1, l2, l3, delta, diff) => {
                let mut d = Vec::new();
                if delta && diff {
                    d.push(14);
                } else if delta {
                    d.push(15);
                } else if diff {
                    d.push(16);
                } else {
                    d.push(17);
                }
                d.extend(l1.into_bytes());
                d.extend(l2.into_bytes());
                d.extend(l3.into_bytes(c));
                d
            }
            CuacLayer::L3S(l1, l2, l3, l4, delta, diff) => {
                let mut d = Vec::new();
                if delta && diff {
                    d.push(18);
                } else if delta {
                    d.push(19);
                } else if diff {
                    d.push(20);
                } else {
                    d.push(21);
                }
                d.extend(l1.into_bytes());
                d.extend(l2.into_bytes());
                d.extend(l3.into_bytes());
                d.extend(l4.into_bytes(c));
                d
            }
            CuacLayer::MetaLayer(l) => {
                let mut d = Vec::new();
                d.push(22);
                let mut d2 = Vec::new();
                into_writer(&l, &mut d2).unwrap();
                d.extend((d2.len() as u32).to_be_bytes().iter());
                d.extend(d2);
                d
            }
        }
    }

    pub fn from_bytes<S : StringCompression>(bytes : &[u8], offset : usize, 
        layer_desc : &LayerDesc, s : &S) -> CuacResult<(CuacLayer, usize)> {
        match bytes[offset] {
            0 => {
                let len = u16::from_be_bytes([bytes[offset + 1], bytes[offset + 2]]) as usize;
                Ok((CuacLayer::Characters(bytes[offset + 1..offset + len + 3].to_vec()), offset + len + 3))
            },
            1 => {
                let (l, len) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                Ok((CuacLayer::L1(l, true), offset + len + 1))
            },
            2 => {
                let (l, len) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                Ok((CuacLayer::L1(l, false), offset + len + 1))
            },
            3 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                Ok((CuacLayer::L2(l1, l2, true, true), offset + len1 + len2 + 1))
            },
            4 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                Ok((CuacLayer::L2(l1, l2, true, false), offset + len1 + len2 + 1))
            },
            5 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                Ok((CuacLayer::L2(l1, l2, false, true), offset + len1 + len2 + 1))
            },
            6 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                Ok((CuacLayer::L2(l1, l2, false, false), offset + len1 + len2 + 1))
            },
            7 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacIndex::from_bytes(&bytes[offset + 1 + len1 + len2..])?;
                Ok((CuacLayer::L3(l1, l2, l3, true, true), offset + len1 + len2 + len3 + 1))
            },
            8 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacIndex::from_bytes(&bytes[offset + 1 + len1 + len2..])?;
                Ok((CuacLayer::L3(l1, l2, l3, true, false), offset + len1 + len2 + len3 + 1))
            },
            9 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacIndex::from_bytes(&bytes[offset + 1 + len1 + len2..])?;
                Ok((CuacLayer::L3(l1, l2, l3, false, true), offset + len1 + len2 + len3 + 1))
            },
            10 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacIndex::from_bytes(&bytes[offset + 1 + len1 + len2..])?;
                Ok((CuacLayer::L3(l1, l2, l3, false, false), offset + len1 + len2 + len3 + 1))
            },
            11 => {
                let (l, len) = CuacData::from_bytes(&bytes[offset + 1..], layer_desc, s)?;
                Ok((CuacLayer::LS(l), offset + len + 1))

            },
            12 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacData::from_bytes(&bytes[offset + 1 + len1..], layer_desc, s)?;
                Ok((CuacLayer::L1S(l1, l2, true), offset + len1 + len2 + 1))
            },
            13 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacData::from_bytes(&bytes[offset + 1 + len1..], layer_desc, s)?;
                Ok((CuacLayer::L1S(l1, l2, false), offset + len1 + len2 + 1))
            },
            14 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacData::from_bytes(&bytes[offset + 1 + len1 + len2..], layer_desc, s)?;
                Ok((CuacLayer::L2S(l1, l2, l3, true, true), offset + len1 + len2 + len3 + 1))
            },
            15 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacData::from_bytes(&bytes[offset + 1 + len1 + len2..], layer_desc, s)?;
                Ok((CuacLayer::L2S(l1, l2, l3, true, false), offset + len1 + len2 + len3 + 1))
            },
            16 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacData::from_bytes(&bytes[offset + 1 + len1 + len2..], layer_desc, s)?;
                Ok((CuacLayer::L2S(l1, l2, l3, false, true), offset + len1 + len2 + len3 + 1))
            },
            17 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacData::from_bytes(&bytes[offset + 1 + len1 + len2..], layer_desc, s)?;
                Ok((CuacLayer::L2S(l1, l2, l3, false, false), offset + len1 + len2 + len3 + 1))
            },
            18 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacIndex::from_bytes(&bytes[offset + 1 + len1 + len2..])?;
                let (l4, len4) = CuacData::from_bytes(&bytes[offset + 1 + len1 + len2 + len3..], layer_desc, s)?;
                Ok((CuacLayer::L3S(l1, l2, l3, l4, true, true), offset + len1 + len2 + len3 + len4 + 1))
            },
            19 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacIndex::from_bytes(&bytes[offset + 1 + len1 + len2..])?;
                let (l4, len4) = CuacData::from_bytes(&bytes[offset + 1 + len1 + len2 + len3..], layer_desc, s)?;
                Ok((CuacLayer::L3S(l1, l2, l3, l4, true, false), offset + len1 + len2 + len3 + len4 + 1))
            },
            20 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacIndex::from_bytes(&bytes[offset + 1 + len1 + len2..])?;
                let (l4, len4) = CuacData::from_bytes(&bytes[offset + 1 + len1 + len2 + len3..], layer_desc, s)?;
                Ok((CuacLayer::L3S(l1, l2, l3, l4, false, true), offset + len1 + len2 + len3 + len4 + 1))
            },
            21 => {
                let (l1, len1) = CuacIndex::from_bytes(&bytes[offset + 1..])?;
                let (l2, len2) = CuacIndex::from_bytes(&bytes[offset + 1 + len1..])?;
                let (l3, len3) = CuacIndex::from_bytes(&bytes[offset + 1 + len1 + len2..])?;
                let (l4, len4) = CuacData::from_bytes(&bytes[offset + 1 + len1 + len2 + len3..], layer_desc, s)?;
                Ok((CuacLayer::L3S(l1, l2, l3, l4, false, false), offset + len1 + len2 + len3 + len4 + 1))
            },
            22 => {
                let len = u32::from_be_bytes([bytes[offset + 1], bytes[offset + 2], bytes[offset + 3], bytes[offset + 4]]) as usize;
                let l = from_reader(&bytes[offset + 5..offset + 5 + len])?;
                Ok((CuacLayer::MetaLayer(l), offset + len + 5))
            },
            x => {
                if x == CUAC_EMPTY_LAYER {
                    eprintln!("Read empty layer byte in to_layer");
                }
                Err(CuacError::InvalidByte)
            }
        }
    }

    pub fn from_reader<R : BufRead, S : StringCompression>(bytes : &mut R, 
        layer_desc : &LayerDesc, s : &S) -> CuacResult<ReadLayerResult<CuacLayer>> {
        let mut buf = vec![0u8; 1];
        match bytes.read_exact(&mut buf) {
            Ok(()) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(ReadLayerResult::Eof);
            },
            Err(e) => {
                return Err(CuacError::IOError(e));
            }
        };
        match buf[0] {
            0 => {
                let mut buf = vec![0u8; 2];
                bytes.read_exact(&mut buf)?;
                let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
                let mut buf = vec![0u8; len];
                bytes.read_exact(&mut buf)?;
                Ok(ReadLayerResult::Layer(CuacLayer::Characters(buf)))
            },
            1 => {
                Ok(ReadLayerResult::Layer(CuacLayer::L1(CuacIndex::from_reader(bytes)?, true)))
            },
            2 => {
                Ok(ReadLayerResult::Layer(CuacLayer::L1(CuacIndex::from_reader(bytes)?, false)))
            },
            3 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L2(l1, l2, true, true)))
            },
            4 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L2(l1, l2, true, false)))
            },
            5 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L2(l1, l2, false, true)))
            },
            6 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L2(l1, l2, false, false)))
            },
            7 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacIndex::from_reader(bytes)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L3(l1, l2, l3, true, true)))
            },
            8 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacIndex::from_reader(bytes)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L3(l1, l2, l3, true, false)))
            },
            9 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacIndex::from_reader(bytes)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L3(l1, l2, l3, false, true)))
            },
            10 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacIndex::from_reader(bytes)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L3(l1, l2, l3, false, false)))
            },
            11 => {
                let l = CuacData::from_reader(bytes, layer_desc, s)?;
                Ok(ReadLayerResult::Layer(CuacLayer::LS(l)))
            },
            12 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacData::from_reader(bytes, layer_desc, s)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L1S(l1, l2, true)))
            },
            13 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacData::from_reader(bytes, layer_desc, s)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L1S(l1, l2, false)))
            },
            14 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacData::from_reader(bytes, layer_desc, s)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L2S(l1, l2, l3, true, true)))
            },
            15 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacData::from_reader(bytes, layer_desc, s)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L2S(l1, l2, l3, true, false)))
            },
            16 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacData::from_reader(bytes, layer_desc, s)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L2S(l1, l2, l3, false, true)))
            },
            17 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacData::from_reader(bytes, layer_desc, s)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L2S(l1, l2, l3, false, false)))
            },
            18 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacIndex::from_reader(bytes)?;
                let l4 = CuacData::from_reader(bytes, layer_desc, s)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L3S(l1, l2, l3, l4, true, true)))
            },
            19 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacIndex::from_reader(bytes)?;
                let l4 = CuacData::from_reader(bytes, layer_desc, s)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L3S(l1, l2, l3, l4, true, false)))
            },
            20 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacIndex::from_reader(bytes)?;
                let l4 = CuacData::from_reader(bytes, layer_desc, s)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L3S(l1, l2, l3, l4, false, true)))
            },
            21 => {
                let l1 = CuacIndex::from_reader(bytes)?;
                let l2 = CuacIndex::from_reader(bytes)?;
                let l3 = CuacIndex::from_reader(bytes)?;
                let l4 = CuacData::from_reader(bytes, layer_desc, s)?;
                Ok(ReadLayerResult::Layer(CuacLayer::L3S(l1, l2, l3, l4, false, false)))
            },
            22 => {
                let mut buf = vec![0u8; 4];
                bytes.read_exact(&mut buf)?;
                let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                let mut buf = vec![0u8; len];
                bytes.read_exact(&mut buf)?;
                let l = from_reader(&buf[..])?;
                Ok(ReadLayerResult::Layer(CuacLayer::MetaLayer(l)))
            },
            x => {
                if x == CUAC_EMPTY_LAYER {
                    Ok(ReadLayerResult::Empty)
                } else {
                    Err(CuacError::InvalidByte)
                }
            }
        }
    }

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

fn all_ascending(v : &Vec<u32>) -> bool {
    v.windows(2).all(|w| w[0] < w[1])
}

fn follows(v1 : &Vec<u32>, v2 : &Vec<u32>) -> bool {
    v1.iter().zip(v2.iter()).all(|(x,y)| x <= y)
}

