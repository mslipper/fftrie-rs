use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::nibbles::Nibbles;

#[derive(Serialize, Deserialize, Clone)]
pub struct Meta {
    pub hash: Option<Vec<u8>>,

    #[serde(skip_serializing, default = "default_as_false")]
    pub(super) dirty: bool,

    #[serde(skip_serializing, default = "default_as_true")]
    pub(super) committed: bool,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            hash: None,
            dirty: true,
            committed: false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "node")]
pub enum Node {
    Branch(Branch),
    Leaf(Leaf),
    Extension(Extension),
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Node::Branch(branch) => write!(f, "Branch"),
            Node::Leaf(leaf) => write!(f, "Leaf"),
            Node::Extension(extension) => write!(f, "Extension"),
        }
    }
}

impl Node {
    pub fn from_slice(slice: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut n: usize = 0;

        let mut node = match slice[n] {
            0 => {
                n += 1;

                let mut children = [0i64; 16];

                for i in 0..16 {
                    children[i] = i64::from_be_bytes(slice[n..n + 8].try_into().unwrap());
                    n += 8;
                }

                let value_len = u16::from_be_bytes(slice[n..n + 2].try_into().unwrap()) as usize;
                n += 2;

                let value = if value_len > 0 {
                    Some(slice[n..n + value_len].to_vec())
                } else {
                    None
                };

                n += value_len;

                Node::Branch(Branch {
                    children,
                    value,
                    meta: Meta::default(),
                })
            }
            1 => {
                n += 1;

                let path_len = slice[n] as usize;
                n += 1;

                let path = Nibbles::from_raw_bytes(&slice[n..n + path_len]);
                n += path_len;

                let value_len = u16::from_be_bytes(slice[n..n + 2].try_into().unwrap()) as usize;
                n += 2;

                let value = slice[n..n + value_len].to_vec();
                n += value_len;

                Node::Leaf(Leaf {
                    path,
                    value,
                    meta: Meta::default(),
                })
            }
            2 => {
                n += 1;

                let path_len = slice[n] as usize;
                n += 1;

                let path = Nibbles::from_raw_bytes(&slice[n..n + path_len]);
                n += path_len;

                let child = i64::from_be_bytes(slice[n..n + 8].try_into().unwrap());
                n += 8;

                Node::Extension(Extension {
                    path,
                    child,
                    meta: Meta::default(),
                })
            }
            _ => panic!("Invalid node type"),
        };

        let hash = slice[n..n + 32].to_vec();
        n += 32;
        node.set_hash(hash);
        node.set_committed(true);
        Ok(node)
    }

    pub fn to_writer(&self, writer: &mut dyn std::io::Write) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Node::Branch(branch) => {
                writer.write_all(&[0])?;

                for i in 0..16 {
                    writer.write_all(&branch.children[i].to_be_bytes())?;
                }

                match &branch.value {
                    Some(value) => {
                        writer.write_all(&(value.len() as u16).to_be_bytes())?;
                        writer.write_all(value)?;
                    }
                    None => {
                        writer.write_all(&0u64.to_be_bytes())?;
                    }
                }
            }
            Node::Leaf(leaf) => {
                writer.write_all(&[1])?;

                writer.write_all(&(leaf.path.len() as u8).to_be_bytes())?;
                writer.write_all(&leaf.path.raw_bytes())?;

                writer.write_all(&(leaf.value.len() as u16).to_be_bytes())?;
                writer.write_all(&leaf.value)?;
            }
            Node::Extension(extension) => {
                writer.write_all(&[2])?;

                writer.write_all(&(extension.path.len() as u8).to_be_bytes())?;
                writer.write_all(&extension.path.raw_bytes())?;

                writer.write_all(&extension.child.to_be_bytes())?;
            }
        }

        match self.hash() {
            Some(hash) => writer.write_all(&hash)?,
            None => {
                return Err("node hash not set".into());
            }
        }

        Ok(())
    }

    pub fn hash(&self) -> Option<Vec<u8>> {
        match self {
            Node::Branch(branch) => branch.meta.hash.clone(),
            Node::Leaf(leaf) => leaf.meta.hash.clone(),
            Node::Extension(extension) => extension.meta.hash.clone(),
        }
    }

    pub fn set_hash(&mut self, hash: Vec<u8>) {
        match self {
            Node::Branch(branch) => branch.meta.hash = Some(hash),
            Node::Leaf(leaf) => leaf.meta.hash = Some(hash),
            Node::Extension(extension) => extension.meta.hash = Some(hash),
        }
    }

    pub fn is_dirty(&self) -> bool {
        match self {
            Node::Branch(branch) => branch.meta.dirty,
            Node::Leaf(leaf) => leaf.meta.dirty,
            Node::Extension(extension) => extension.meta.dirty,
        }
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        match self {
            Node::Branch(branch) => branch.meta.dirty = dirty,
            Node::Leaf(leaf) => leaf.meta.dirty = dirty,
            Node::Extension(extension) => extension.meta.dirty = dirty,
        }
    }

    pub fn is_committed(&self) -> bool {
        match self {
            Node::Branch(branch) => branch.meta.committed,
            Node::Leaf(leaf) => leaf.meta.committed,
            Node::Extension(extension) => extension.meta.committed,
        }
    }

    pub fn set_committed(&mut self, committed: bool) {
        match self {
            Node::Branch(branch) => branch.meta.committed = committed,
            Node::Leaf(leaf) => leaf.meta.committed = committed,
            Node::Extension(extension) => extension.meta.committed = committed,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Branch {
    pub children: [i64; 16],
    pub value: Option<Vec<u8>>,
    pub meta: Meta,
}

impl Branch {
    pub fn new() -> Self {
        Branch::default()
    }
}

impl Default for Branch {
    fn default() -> Self {
        Self {
            children: [0; 16],
            value: None,
            meta: Meta::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Leaf {
    pub path: Nibbles,
    pub value: Vec<u8>,
    pub meta: Meta,
}

impl Leaf {
    pub fn new(path: Nibbles, value: Vec<u8>) -> Self {
        Self {
            path,
            value,
            meta: Meta::default(),
        }
    }
}

impl Default for Leaf {
    fn default() -> Self {
        Self {
            path: Nibbles::default(),
            value: Vec::new(),
            meta: Meta::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Extension {
    pub path: Nibbles,
    pub child: i64,
    pub meta: Meta,
}

impl Extension {
    pub fn new(path: Nibbles, child: i64) -> Self {
        Self {
            path,
            child,
            meta: Meta::default(),
        }
    }
}

impl Default for Extension {
    fn default() -> Self {
        Self {
            path: Nibbles::default(),
            child: 0,
            meta: Meta::default(),
        }
    }
}

// https://github.com/serde-rs/serde/issues/368
fn default_as_true() -> bool {
    true
}

// https://github.com/serde-rs/serde/issues/368
fn default_as_false() -> bool {
    false
}