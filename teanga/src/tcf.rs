/// Teanga Compressed Format
use crate::{Layer, Value, LayerDesc, Document, DataType};
use std::collections::HashMap;
use smaz;
use ciborium::into_writer;
use std::io::Write;
use lru::LruCache;

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

pub enum IndexResult {
    Index(u32),
    String(String)
}

pub trait Index {
    fn idx(&mut self, str : &String) -> IndexResult;
    fn str(&self, idx : u32) -> Option<String>;
}

fn index_results_to_bytes(ir : &Vec<IndexResult>) -> Vec<u8> {
    let mut d = Vec::new();
    for i in ir {
        match i {
            IndexResult::Index(idx) => {
                if *idx >= 2147482648 {
                    panic!("Index too large");
                }
                d.extend(u32_to_varbytes(*idx));
            }
            IndexResult::String(s) => {
                let b = smaz::compress(&s.as_bytes());
                d.push(0b1000_0000 & (b.len() as u8));
                d.extend(b);
            }
        }
    }
    d
}

fn to_delta(v : Vec<u32>) -> Vec<u32> {
    let mut l = 0;

    v.into_iter().map(|x| {
        let x2 = x - l;
        l = x;
        x2
    }).collect()
}

fn to_diff(v1 : &Vec<u32>, v2 : Vec<u32>) -> Vec<u32> {
    v2.into_iter().zip(v1.iter()).map(|(x,y)| y - x ).collect()
}

impl TCF {
    pub fn from_layer<I : Index>(l : &Layer, idx : &mut I, ld : &LayerDesc) -> TCF {
        match l {
            Layer::Characters(c) => TCF::Characters(smaz::compress(&c.as_bytes())),
            Layer::L1(l) => TCF::L1(TCFIndex::from_vec(&l)),
            Layer::L2(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| s.1).collect();
                let v2 = to_diff(&v1, v2);
                let v1 = to_delta(v1);
                TCF::L2(TCFIndex::from_vec(&v1), TCFIndex::from_vec(&v2))
            }
            Layer::L3(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| s.1).collect();
                let v3 = l.iter().map(|s| s.2).collect();
                TCF::L3(TCFIndex::from_vec(&v1), TCFIndex::from_vec(&v2), TCFIndex::from_vec(&v3))
            }
            Layer::LS(l) => {
                TCF::LS(
                    TCFData::from_iter(l.iter(), ld, idx))
            }
            Layer::L1S(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| &s.1);
                TCF::L1S(TCFIndex::from_vec(&v1), 
                    TCFData::from_iter(v2, ld, idx))
            }
            Layer::L2S(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| s.1).collect();
                let v3 = l.iter().map(|s| &s.2);
                TCF::L2S(TCFIndex::from_vec(&v1), 
                    TCFIndex::from_vec(&v2), 
                    TCFData::from_iter(v3, ld, idx))
            }
            Layer::L3S(l) => {
                let v1 = l.iter().map(|s| s.0).collect();
                let v2 = l.iter().map(|s| s.1).collect();
                let v3 = l.iter().map(|s| s.2).collect();
                let v4 = l.iter().map(|s| &s.3);
                TCF::L3S(TCFIndex::from_vec(&v1), 
                    TCFIndex::from_vec(&v2), 
                    TCFIndex::from_vec(&v3),
                    TCFData::from_iter(v4, ld, idx))
            }
            Layer::MetaLayer(l) => TCF::MetaLayer(l.clone())
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            TCF::Characters(c) => {
                let mut d = Vec::new();
                d.push(0);
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
                into_writer(&l, &mut d).unwrap();
                d
            }
        }
    }
}

pub fn layer_to_bytes<I: Index>(layer : &Layer, idx : &mut I, ld : &LayerDesc) -> Vec<u8> {
    TCF::from_layer(layer, idx, ld).into_bytes()
}

pub fn write_tcf_corpus<W : Write, I>(
    mut out : W,
    meta : &HashMap<String, LayerDesc>,
    data : I,
    byte_counts : &mut HashMap<String, u32>) -> std::io::Result<()> 
        where I : Iterator<Item = (String, Document)> {
    into_writer(meta, &mut out).unwrap();
    let mut cache = LRUIndex::new(1000);
    for (doc_id, doc) in data {
        into_writer(&doc_id, &mut out).unwrap();
        into_writer(&(doc.content.len() as u32), &mut out).unwrap();
        for (key, layer) in doc.content {
            into_writer(&key, &mut out).unwrap();
            let b = TCF::from_layer(&layer, &mut cache,
                meta.get(&key).unwrap()).into_bytes();
            byte_counts.entry(key.clone())
                .and_modify(|e| *e += b.len() as u32)
                .or_insert(b.len() as u32);
            out.write(b.as_slice()).unwrap();
        }
    }
    //into_writer(&cache.vec, &mut out).unwrap();

    Ok(())
}

struct LRUIndex {
    map : HashMap<String, u32>,
    pub vec : Vec<String>,
    cache : LruCache<String, u32>
}

impl LRUIndex {
    fn new(size : usize) -> LRUIndex {
        LRUIndex {
            map : HashMap::new(),
            vec : Vec::new(),
            cache : LruCache::new(std::num::NonZeroUsize::new(size).unwrap())
        }
    }
}

impl Index for LRUIndex {
    fn idx(&mut self, str : &String) -> IndexResult {
        if let Some(idx) = self.map.get(str) {
            return IndexResult::Index(*idx);
        } else if let Some(_) = self.cache.get(str) {
            let idx = self.vec.len() as u32;
            self.map.insert(str.clone(), idx);
            self.vec.push(str.clone());
            self.cache.pop(str);
            return IndexResult::Index(idx);
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

enum TCFData {
    String(Vec<IndexResult>),
    Enum(Vec<u32>)
}

impl TCFData {
    pub fn from_iter<'a, I, I2 : Index>(iter : I, ld : &LayerDesc,
        idx : &mut I2) -> TCFData where I : Iterator<Item = &'a String> {
        match ld.data {
            Some(DataType::String) => {
                let v = iter.map(|s| idx.idx(&s)).collect();
                TCFData::String(v)
            }
            Some(DataType::Enum(ref enum_vals)) => {
                let map : HashMap<String, usize> = enum_vals.iter().enumerate().map(|(i, s)| (s.clone(), i)).collect();
                let v = iter.map(|s| {
                    //if !map.contains_key(s) {
                    //    panic!("Invalid enum value: {}", s);
                    //}
                    map.get(s).map(|s| *s).unwrap_or(0) as u32
                }).collect();
                TCFData::Enum(v)
            }
            Some(DataType::Link) => {
                panic!("Link data type not supported");
            }
            None => {
                panic!("No data type specified");
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
        d
    }

    pub fn from_bytes(bytes : &Vec<u8>) -> TCFIndex {
        let precision = bytes[0];
        let length = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        let data = bytes[5..].to_vec();
        TCFIndex {
            precision,
            length,
            data,
        }
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
            bytes[3] | 0b1000_0000]
    } else if n < 268435456 {
        vec![bytes[0] << 3 | bytes[1] >> 5 | 0b1000_0000,
            bytes[1] << 2 | bytes[2] >> 6 | 0b1000_0000,
            bytes[2] << 1 | bytes[3] >> 7 | 0b1000_0000,
            bytes[3] | 0b1000_0000]
    } else {
        vec![bytes[0] | 0b1000_0000 >> 4,
            bytes[0] << 3 | bytes[1] >> 5 | 0b1000_0000,
            bytes[1] << 2 | bytes[2] >> 6 | 0b1000_0000,
            bytes[2] << 1 | bytes[3] >> 7 | 0b1000_0000,
            bytes[3] | 0b1000_0000]
    }
}

fn varbytes_to_u32(bytes : &Vec<u8>) -> u32 {
    let mut n = 0u32;
    for (i, b) in bytes.iter().enumerate() {
        n += ((b & 0b0111_1111) as u32) << ((bytes.len() - i - 1) * 7);
        if *b & 0b1000_0000 == 0 {
            break;
        }
    }
    n
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
            let n2 = varbytes_to_u32(&bytes);
            assert_eq!(n, n2);
        }
    }
}
