use std::collections::{HashMap, HashSet};
use crate::nibbles::Nibbles;
use crate::node::Node;
use crate::store::Store;

use rlp::{RlpStream};
use tiny_keccak::Hasher;

mod nibbles;
mod node;
mod store;

const EMPTY_ROOT_HASH: [u8; 32] = [
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
    0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
    0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
];

pub struct Trie {
    root: Option<i64>,
    nodes: HashMap<i64, node::Node>,
    dirties: HashSet<i64>,
    store: Box<dyn Store>,
    last_id: i64,
}

impl Trie {
    pub fn new(store: Box<dyn Store>) -> Self {
        Self {
            root: None,
            nodes: HashMap::new(),
            dirties: HashSet::new(),
            store,
            last_id: -100,
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let mut path = Nibbles::from_bytes(key);
        if self.root.is_none() {
            let leaf = node::Node::Leaf(node::Leaf {
                path,
                value: value.to_vec(),
            });
            self.root = Some(self.intern(leaf));
            self.dirties.insert(self.root.unwrap());
            return Ok(());
        }

        let mut current_node_id = self.root.unwrap();
        loop {
            let current_node = self.get_node(current_node_id)?;
            self.dirties.insert(current_node_id);

            match current_node {
                node::Node::Leaf(mut leaf) => {
                    let shared_prefix = leaf.path.intersection(&path);

                    if shared_prefix.len() == leaf.path.len() && shared_prefix.len() == path.len() {
                        leaf.value = value.to_vec();
                        self.nodes.insert(current_node_id, node::Node::Leaf(leaf));
                        break;
                    }

                    let mut branch = node::Branch {
                        children: [0; 16],
                        value: None,
                    };

                    if shared_prefix.len() == path.len() {
                        branch.value = Some(value.to_vec());
                    } else if shared_prefix.len() == leaf.path.len() {
                        branch.value = Some(leaf.value.clone());
                    }

                    if shared_prefix.len() < leaf.path.len() {
                        let child_nibble = leaf.path.at(shared_prefix.len()) as usize;
                        branch.children[child_nibble] = self.intern(node::Node::Leaf(node::Leaf {
                            path: leaf.path.slice_from(shared_prefix.len() + 1),
                            value: leaf.value,
                        }));
                    }

                    if shared_prefix.len() < path.len() {
                        let child_path = path.at(shared_prefix.len()) as usize;
                        branch.children[child_path] = self.intern(node::Node::Leaf(node::Leaf {
                            path: path.slice_from(shared_prefix.len() + 1),
                            value: value.to_vec(),
                        }));
                    }

                    if shared_prefix.len() > 0 {
                        let branch_id = self.intern(node::Node::Branch(branch));
                        let ext = node::Extension {
                            path: leaf.path.slice_to(shared_prefix.len()),
                            child: branch_id,
                            value: None,
                        };

                        self.nodes.insert(current_node_id, node::Node::Extension(ext));
                    } else {
                        self.nodes.insert(current_node_id, node::Node::Branch(branch));
                    }
                }
                node::Node::Extension(mut ext) => {
                    let shared_prefix = ext.path.intersection(&path);

                    let child = self.get_node(ext.child)?;

                    // Shared prefix is the same, replace the child.
                    if shared_prefix.len() == ext.path.len() {
                        path = path.slice_from(shared_prefix.len());
                        let new_child = child.clone();
                        ext.child = self.intern(new_child);
                        current_node_id = ext.child;
                        self.nodes.insert(current_node_id, node::Node::Extension(ext));
                        continue;
                    }

                    // Shared prefix is a subset of the extension path, split the extension.
                    let matched_path = ext.path.slice_to(shared_prefix.len());
                    let branch_nibble = ext.path.at(shared_prefix.len());
                    let unmatched_path = ext.path.slice_from(shared_prefix.len() + 1);

                    let mut branch = node::Branch {
                        children: [0; 16],
                        value: None,
                    };

                    if unmatched_path.len() == 0 {
                        branch.children[branch_nibble as usize] = ext.child;
                    } else {
                        branch.children[branch_nibble as usize] = self.intern(node::Node::Extension(node::Extension {
                            path: unmatched_path.clone(),
                            child: ext.child,
                            value: None,
                        }));
                    }

                    if shared_prefix.len() < path.len() {
                        let child_path = path.at(shared_prefix.len());
                        branch.children[child_path] = self.intern(node::Node::Leaf(node::Leaf {
                            path: path.slice_from(shared_prefix.len() + 1),
                            value: value.to_vec(),
                        }));
                    } else if shared_prefix.len() == path.len() {
                        branch.value = Some(value.to_vec());
                    } else {
                        unreachable!("shared_prefix.len() > path.len() -> should never happen");
                    }

                    if matched_path.len() == 0 {
                        self.nodes.insert(current_node_id, node::Node::Branch(branch));
                    } else {
                        let branch_id = self.intern(node::Node::Branch(branch));
                        let ext = node::Extension {
                            path: matched_path,
                            child: branch_id,
                            value: None,
                        };
                        self.nodes.insert(current_node_id, node::Node::Extension(ext));
                    }

                    break;
                }
                node::Node::Branch(mut branch) => {
                    if path.len() == 0 {
                        branch.value = Some(value.to_vec());
                        self.nodes.insert(current_node_id, node::Node::Branch(branch));
                        break;
                    }

                    let branch_nibble = path.at(0);
                    path = path.slice_from(1);

                    // This branch has no child at the branch nibble, so we create a leaf node.
                    if branch.children[branch_nibble] == 0 {
                        branch.children[branch_nibble] = self.intern(node::Node::Leaf(node::Leaf {
                            path,
                            value: value.to_vec(),
                        }));
                        self.nodes.insert(current_node_id, node::Node::Branch(branch));
                        break;
                    }

                    let child_offset = branch.children[branch_nibble];

                    // This node is already dirty, so we can just traverse into it.
                    if child_offset < 0 {
                        current_node_id = child_offset;
                        continue;
                    }

                    // This node is not dirty, so we need to create a cloned dirty node
                    // at this offset.
                    let child = self.get_node(child_offset)?;
                    let new_child = child.clone();
                    branch.children[branch_nibble] = self.intern(new_child);
                    current_node_id = branch.children[branch_nibble];
                    self.nodes.insert(current_node_id, node::Node::Branch(branch));
                }
            }
        }

        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        if self.root.is_none() {
            return Err("root not found".into());
        }

        let mut path = Nibbles::from_bytes(key);
        let mut current_node_id = self.root.unwrap();

        loop {
            let current_node = self.get_node(current_node_id)?;

            match current_node {
                node::Node::Leaf(leaf) => {
                    let shared_prefix = leaf.path.intersection(&path);

                    if shared_prefix.len() == leaf.path.len() && shared_prefix.len() == path.len() {
                        return Ok(leaf.value.clone());
                    }

                    return Err("key not found".into());
                }
                node::Node::Extension(ext) => {
                    let shared_prefix = ext.path.intersection(&path);

                    if shared_prefix.len() != ext.path.len() {
                        return Err("key not found".into());
                    }

                    current_node_id = ext.child;
                    path = path.slice_from(shared_prefix.len());
                }
                node::Node::Branch(branch) => {
                    if path.len() == 0 {
                        return match &branch.value {
                            Some(value) => Ok(value.clone()),
                            None => Err("key not found".into()),
                        };
                    }

                    let branch_nibble = path.at(0);
                    let child_offset = branch.children[branch_nibble];
                    if child_offset == 0 {
                        return Err("key not found".into());
                    }

                    current_node_id = child_offset;
                    path = path.slice_from(1);
                }
            }
        }
    }

    fn intern(&mut self, node: node::Node) -> i64 {
        let id = self.last_id;
        self.nodes.insert(id, node);
        self.dirties.insert(id);
        self.last_id -= 1;
        id
    }

    fn get_node(&self, offset: i64) -> Result<node::Node, Box<dyn std::error::Error>> {
        if offset < 0 {
            println!("store offset: {}", offset);
            return self.nodes.get(&offset)
                .cloned()
                .ok_or("node not found".into());
        }

        match self.nodes.get(&offset) {
            Some(node) => Ok(node.clone()),
            None => {
                let node = self.store.get(offset)?;
                Ok(node)
            }
        }
    }

    fn root(&mut self) -> Result<[u8; 32], Box<dyn std::error::Error>> {
        if self.root.is_none() {
            return Ok(EMPTY_ROOT_HASH);
        }

        let root = self.get_node(self.root.unwrap())?;
        let hash = self.hash_node(root)?;

        if hash.len() < 32 {
            let mut hasher = tiny_keccak::Keccak::v256();
            hasher.update(hash.as_slice());
            let mut root_hash = [0u8; 32];
            hasher.finalize(&mut root_hash);
            return Ok(root_hash);
        }

        let mut root_hash = [0u8; 32];
        root_hash.copy_from_slice(&hash[..32]);
        Ok(root_hash)
    }

    fn hash_node(&mut self, node: Node) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let data = match node {
            Node::Extension(ext) => {
                let child = self.hash_node(self.get_node(ext.child)?)?;
                let mut stream = RlpStream::new_list(2);
                stream.append_list(ext.path.prefixed_bytes(false).as_slice())
                    .append_list(child.as_slice());
                stream.out().to_vec()
            }
            Node::Leaf(leaf) => {
                let mut stream = RlpStream::new_list(2);
                stream.append_list(leaf.path.prefixed_bytes(true).as_slice())
                    .append_list(leaf.value.as_slice());
                stream.out().to_vec()
            }
            Node::Branch(branch) => {
                let mut stream = RlpStream::new_list(17);
                for child in &branch.children {
                    if *child == 0 {
                        stream.append_empty_data();
                    } else {
                        let child = self.hash_node(self.get_node(*child)?)?;
                        stream.append_list(child.as_slice());
                    }
                }

                match branch.value {
                    Some(value) => {
                        stream.append_list(value.as_slice())
                    }
                    None => {
                        stream.append_empty_data()
                    }
                };

                stream.out().to_vec()
            }
        };

        if data.len() < 32 {
            return Ok(data);
        }

        let mut hash = [0u8; 32];
        if data.len() >= 32 {
            let mut hasher = tiny_keccak::Keccak::v256();
            hasher.update(data.as_slice());
            hasher.finalize(&mut hash);
        }
        Ok(hash.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_basic() -> Result<(), Box<dyn std::error::Error>> {
        let store = Box::new(store::MemoryStore::new());
        let mut trie = Trie::new(store);
        trie.insert(b"do", b"verb")?;
        trie.insert(b"horse", b"stallion")?;
        trie.insert(b"doge", b"coin")?;
        trie.insert(b"dog", b"puppy")?;
        let root = trie.root()?;
        assert_eq!(hex::encode(&root), "5991bb8c6514148a29db676a14ac506cd2cd5775ace63c30a4fe457715e9ac84");
        Ok(())
    }
}
