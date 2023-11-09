use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Debug, Default)]
pub struct Nibbles {
    data: Vec<u8>,
}

impl Nibbles {
    pub fn from_raw_bytes(bytes: &[u8]) -> Self {
        Self {
            data: bytes.to_vec(),
        }
    }

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
            match other_iter.next() {
                None => break,
                Some(other_nibble) => {
                    if nibble == other_nibble {
                        result.push(*nibble);
                    } else {
                        break;
                    }
                }
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
        let mut prefixed: Vec<u8> = Vec::with_capacity(2 + self.data.len());

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

    pub fn raw_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl Serialize for Nibbles {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
        let mut bytes = Vec::with_capacity(self.data.len() / 2 + 1);
        if self.data.len() % 2 == 0 {
            bytes.push(0x00);
        } else {
            bytes.push(0x01);
        }
        for i in (0..self.data.len()).step_by(2) {
            let mut byte = self.data[i] << 4;
            if i + 1 < self.data.len() {
                byte += self.data[i + 1];
            }
            bytes.push(byte);
        }
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for Nibbles {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: serde::Deserializer<'de> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let mut data = Vec::with_capacity(bytes.len() * 2);
        for byte in bytes[1..].iter() {
            data.push(byte >> 4);
            data.push(byte & 0x0F);
        }

        let data = if bytes[0] == 0x00 {
            data
        } else {
            data[..data.len() - 1].to_vec()
        };

        Ok(Self {
            data,
        })
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
        let nibbles2 = Nibbles::from_bytes(&[0x12, 0x34, 0x66, 0x78]);
        let result = nibbles1.intersection(&nibbles2);
        assert_eq!(result.at(0), 0x01);
        assert_eq!(result.at(1), 0x02);
        assert_eq!(result.at(2), 0x03);
        assert_eq!(result.at(3), 0x04);
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_slicing() {
        let nibbles = Nibbles::from_bytes(&[0x12, 0x34, 0x56, 0x78]);
        assert_eq!(nibbles.slice_to(4), nibbles![0x01, 0x02, 0x03, 0x04]);
        assert_eq!(nibbles.slice_from(4), nibbles![0x05, 0x06, 0x07, 0x08]);
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

    #[test]
    fn test_serde() -> Result<(), serde_json::Error> {
        let nibbles = nibbles![0x01, 0x02, 0x03];
        let serialized = serde_json::to_string(&nibbles)?;
        assert_eq!(serialized, "[1,18,48]");
        let deserialized: Nibbles = serde_json::from_str(&serialized)?;
        assert_eq!(deserialized, nibbles);
        Ok(())
    }

    fn prefixed_bytes_test(data: &[u8], exp: &[u8], leaf: bool) {
        let nibbles = Nibbles { data: data.to_vec() };
        let prefixed = nibbles.prefixed_bytes(leaf);
        assert_eq!(prefixed, exp);
    }
}