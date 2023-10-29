use std::ops::{Index, Range, RangeFrom, RangeTo};

#[derive(Clone, PartialEq, Debug)]
pub struct Nibbles {
    data: Vec<u8>,
}

impl Nibbles {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut data = Vec::with_capacity(bytes.len() * 2);
        for byte in bytes {
            data.push(byte >> 4);
            data.push(byte & 0x0F);
        }

        Self {
            data,
        }
    }

    pub fn intersection(&self, other: &Self) -> Self {
        let mut result = Vec::new();
        let mut other_iter = other.data.iter();
        for nibble in &self.data {
            if let Some(other_nibble) = other_iter.next() {
                result.push(nibble & other_nibble);
            } else {
                break;
            }
        }

        Self {
            data: result,
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn prefixed_bytes(&self, leaf: bool) -> Vec<u8> {
        let mut prefixed: Vec<u8> = Vec::new();

        let prefix = match self.data.len() % 2 {
            0 => vec![0x00, 0x00],
            _ => vec![0x01],
        };

        prefixed.extend(prefix);
        prefixed.extend(&self.data);
        if leaf {
            prefixed[0] += 0x02
        }

        let mut result = Vec::with_capacity(prefixed.len() / 2);
        for i in (0..prefixed.len()).step_by(2) {
            result.push((prefixed[i] << 4) + prefixed[i + 1]);
        }
        result
    }

    pub fn slice_to(&self, end: usize) -> Self {
        Self {
            data: self.data[..end].to_vec(),
        }
    }

    pub fn slice_from(&self, start: usize) -> Self {
        Self {
            data: self.data[start..].to_vec(),
        }
    }

    pub fn at(&self, index: usize) -> usize {
        self.data[index] as usize
    }
}

macro_rules! nibbles {
    ( $( $x:expr ),* ) => {
        {
            Nibbles {
                data: vec![ $( $x ),*]
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_bytes() {
        let nibbles = Nibbles::from_bytes(&[0x12, 0x34, 0x56, 0x78]);
        assert_eq!(nibbles.at(0), 0x01);
        assert_eq!(nibbles.at(1), 0x02);
        assert_eq!(nibbles.at(2), 0x03);
        assert_eq!(nibbles.at(3), 0x04);
        assert_eq!(nibbles.at(4), 0x05);
        assert_eq!(nibbles.at(5), 0x06);
        assert_eq!(nibbles.at(6), 0x07);
        assert_eq!(nibbles.at(7), 0x08);
    }

    #[test]
    fn test_intersection() {
        let nibbles1 = Nibbles::from_bytes(&[0x12, 0x34, 0x56, 0x78]);
        let nibbles2 = Nibbles::from_bytes(&[0x12, 0x34, 0x56, 0x78]);
        let result = nibbles1.intersection(&nibbles2);
        assert_eq!(result.at(0), 0x01);
        assert_eq!(result.at(1), 0x02);
        assert_eq!(result.at(2), 0x03);
        assert_eq!(result.at(3), 0x04);
        assert_eq!(result.at(4), 0x05);
        assert_eq!(result.at(5), 0x06);
        assert_eq!(result.at(6), 0x07);
        assert_eq!(result.at(7), 0x08);
    }

    #[test]
    fn test_slicing() {
        let nibbles = Nibbles::from_bytes(&[0x12, 0x34, 0x56, 0x78]);
        assert_eq!(nibbles.slice_to(4), Nibbles { data: vec![0x01, 0x02, 0x03, 0x04] });
        assert_eq!(nibbles.slice_from(4), Nibbles { data: vec![0x05, 0x06, 0x07, 0x08] });
        assert_eq!(nibbles.at(3), 0x04);
    }

    #[test]
    fn test_prefixed_bytes() {
        prefixed_bytes_test(&[0x01], &[0x11], false);
        prefixed_bytes_test(&[0x01], &[0x31], true);
        prefixed_bytes_test(&[0x01, 0x02], &[0x00, 0x12], false);
        prefixed_bytes_test(&[0x01, 0x02], &[0x20, 0x12], true);
        prefixed_bytes_test(&[0x01, 0x02, 0x03], &[0x11, 0x23], false);
        prefixed_bytes_test(&[0x01, 0x02, 0x03], &[0x31, 0x23], true);
    }

    #[test]
    fn test_len() {
        let nibbles = Nibbles::from_bytes(&[0x12, 0x34, 0x56, 0x78]);
        assert_eq!(nibbles.len(), 8);
    }

    fn prefixed_bytes_test(data: &[u8], exp: &[u8], leaf: bool) {
        let nibbles = Nibbles { data: data.to_vec() };
        let prefixed = nibbles.prefixed_bytes(leaf);
        assert_eq!(prefixed, exp);
    }
}