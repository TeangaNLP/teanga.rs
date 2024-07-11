/// Teanga Compressed Format
use crate::{LayerDesc, DataType};
use crate::tcf::index::{Index, IndexResult};
use crate::tcf::tcf_index::TCFIndex;
use crate::tcf::type_index::TypeIndex;
use crate::tcf::{TCFResult, TCFError};
use std::collections::HashMap;
use std::io::BufRead;


#[derive(Debug, Clone, PartialEq)]
pub enum TCFData {
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
    fn test_var_bytes2() {
        let i = 16384;
        let bytes = u32_to_varbytes(i);
        println!("{:?}", bytes);
        let i2 = read_varbytes(&mut bytes.as_slice()).unwrap();
        assert_eq!(i, i2);
    }
}
