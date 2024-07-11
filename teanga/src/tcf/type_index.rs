use std::io::BufRead;

use crate::tcf::TCFResult;

#[derive(Debug, Clone, PartialEq)]
pub struct TypeIndex(Vec<u8>, usize);

impl TypeIndex {
    pub fn new() -> TypeIndex {
        TypeIndex(Vec::new(), 0)
    }

    pub fn append(&mut self, v : bool) {
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

    pub fn to_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn from_bytes(data : &[u8], len : usize) -> (TypeIndex, usize) {
        let l = len / 8 + (if len % 8 == 0 { 0 } else { 1 });
        (TypeIndex(data[0..l].to_vec(), len), l)
    }

    pub fn from_reader<R : BufRead>(input : &mut R, len : usize) -> TCFResult<TypeIndex> {
        let mut buf = vec![0u8; len / 8 + (if len % 8 == 0 { 0 } else { 1 })];
        input.read_exact(&mut buf)?;
        Ok(TypeIndex(buf, len)) 
    }

    pub fn value(&self, idx : usize) -> bool {
        self.0[idx / 8] & (0b1000_0000 >> (idx % 8)) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

