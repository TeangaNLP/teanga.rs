use std::io::BufRead;

use crate::tcf::TCFResult;

pub struct TCFIndex {
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
}
