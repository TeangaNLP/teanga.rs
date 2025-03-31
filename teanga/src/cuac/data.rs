/// Teanga Compressed Format
use crate::{LayerDesc, DataType};
use crate::cuac::string::StringCompression;
use crate::cuac::index::{Index, IndexResult};
use crate::cuac::cuac_index::CuacIndex;
use crate::cuac::type_index::TypeIndex;
use crate::cuac::{CuacResult, CuacError};
use std::collections::HashMap;
use std::io::BufRead;


#[derive(Debug, Clone, PartialEq)]
pub enum CuacData {
    String(Vec<IndexResult>),
    Enum(Vec<u32>)
}

impl CuacData {
    pub fn from_iter<'a, I>(iter : I, ld : &LayerDesc,
        idx : &mut Index) -> CuacResult<CuacData> where I : Iterator<Item = &'a String> {
        match ld.data {
            Some(DataType::String) => {
                let v = iter.map(|s| idx.idx(&s)).collect();
                Ok(CuacData::String(v))
            }
            Some(DataType::Enum(ref enum_vals)) => {
                let map : HashMap<String, usize> = enum_vals.iter().enumerate().map(|(i, s)| (s.clone(), i)).collect();
                let mut v = Vec::new();
                for s in iter {
                    if !map.contains_key(s) {
                        return Err(CuacError::InvalidEnumValue(s.clone()));
                    }
                    v.push(map[s] as u32);
                }
                Ok(CuacData::Enum(v))
            }
            Some(DataType::Link) => {
                let v = iter.map(|s| idx.idx(&s)).collect();
                Ok(CuacData::String(v))
            }
            None => {
                panic!("No data type specified");
            }
        }
    }

    pub fn to_vec(&self, index : &Index, ld : &LayerDesc) -> Vec<String> {
        match self {
            CuacData::String(v) => {
                v.iter().map(|i| match i {
                    IndexResult::String(s) => {
                        index.idx(s);
                        s.clone()
                    }
                    IndexResult::Index(i) => index.str(*i).unwrap()
                }).collect()
            }
            CuacData::Enum(v) => {
                match ld.data {
                    Some(DataType::Enum(ref enum_vals)) => {
                        v.iter().map(|i| enum_vals[*i as usize].clone()).collect()
                    }
                    _ => {
                        panic!("LayerDesc data type does not match CuacData type");
                    }
                }
            }
        }
    }

    pub fn into_bytes<C : StringCompression>(self, compress : &C) -> Vec<u8> {
        match self {
            CuacData::String(v) => {
                index_results_to_bytes(&v, compress)
            }
            CuacData::Enum(v) => {
                CuacIndex::from_vec(&v).into_bytes()
            }
        }
    }

    pub fn from_bytes<S : StringCompression>(data : &[u8], ld : &LayerDesc, s: &S) -> CuacResult<(CuacData, usize)> {
        match ld.data {
            Some(DataType::String) => {
                let (v, len) = bytes_to_index_results(data, s)?;
                Ok((CuacData::String(v), len))
            }
            Some(DataType::Enum(_)) => {
                let (v, len) = CuacIndex::from_bytes(data)?;
                Ok((CuacData::Enum(v.to_vec()), len))
            }
            Some(DataType::Link) => {
                let (v, len) = bytes_to_index_results(data, s)?;
                Ok((CuacData::String(v), len))
            }
            None => {
                panic!("No data type specified");
            }
        }
    }

    pub fn from_reader<R: BufRead, S : StringCompression>(input : &mut R, ld : &LayerDesc, s : &S) -> CuacResult<CuacData> {
        match ld.data {
            Some(DataType::String) => {
                let v = reader_to_index_results(input, s)?;
                Ok(CuacData::String(v))
            }
            Some(DataType::Enum(_)) => {
                let v = CuacIndex::from_reader(input)?;
                Ok(CuacData::Enum(v.to_vec()))
            }
            Some(DataType::Link) => {
                let v = reader_to_index_results(input, s)?;
                Ok(CuacData::String(v))
            }
            None => {
                panic!("No data type specified");
            }
        }
    }

}


fn index_results_to_bytes<C : StringCompression>(ir : &Vec<IndexResult>, compress : &C) -> Vec<u8> {
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
                let b = compress.compress(s);
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

fn bytes_to_index_results<S : StringCompression>(data : &[u8], s : &S) -> CuacResult<(Vec<IndexResult>, usize)> {
    let mut results = Vec::new();
    let (len, len1) = varbytes_to_u32(&data[0..]);
    let len = len as usize;
    let (type_index, len2) = TypeIndex::from_bytes(&data[len1..], len);
    let mut offset = len1 + len2;
    while results.len() < len {
        if type_index.value(results.len()) {
            let (n, len3) = varbytes_to_u32(&data[offset..]);
            let s = s.decompress(&data[offset + len3..offset + len3 + n as usize])?;
            results.push(IndexResult::String(s));
            offset += len3 + n as usize;
        } else {
            let (n, len) = varbytes_to_u32(&data[offset..]);
            results.push(IndexResult::Index(n));
            offset += len;
        }
    }
    Ok((results, offset))
}

fn reader_to_index_results<R: BufRead, S : StringCompression>(input : &mut R, s: &S) -> CuacResult<Vec<IndexResult>> {
    let mut results = Vec::new();
    let len = read_varbytes(input)? as usize;
    let type_index = TypeIndex::from_reader(input, len)?;
    while results.len() < len {
        if type_index.value(results.len()) {
            let n = read_varbytes(input)? as usize;
            let mut buf = vec![0u8; n];
            input.read_exact(&mut buf)?;
            let s = s.decompress(&buf)?;
            results.push(IndexResult::String(s));
        } else {
            let n = read_varbytes(input)?;
            results.push(IndexResult::Index(n));
        }
    }
    Ok(results)
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

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_var_bytes() {
        for n in vec![0,1,10,100,1000,10000,100000, 1000000, 10000000, 100000000] {
            let bytes = u32_to_varbytes(n);
            let (n2, _) = varbytes_to_u32(&bytes);
            assert_eq!(n, n2);
        }
    }

    #[test]
    fn test_cuac_data_round_trip() {
        let mut index = Index::new();
        let data = CuacData::from_iter(vec![&"a".to_string(),
                                           &"a".to_string(),
                                           &"b".to_string(),
                                           &"a".to_string()].into_iter(), 
            &LayerDesc {
                data: Some(DataType::String),
                ..LayerDesc::default()
            }, &mut index).unwrap();
        let c = crate::cuac::string::SmazCompression;
        let bytes = data.clone().into_bytes(&c);
        let (data2, _) = CuacData::from_bytes(&bytes, &LayerDesc {
            data: Some(DataType::String),
            ..LayerDesc::default()
        }, &c).unwrap();
        assert_eq!(data, data2);
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
