extern crate core;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rlp::RlpStream;
use tiny_keccak::Hasher;

use crate::nibbles::Nibbles;
use crate::node::{Branch, Extension, Leaf, Node};
use crate::store::Store;

mod nibbles;
mod node;
mod store;

const EMPTY_ROOT_HASH: [u8; 32] = [
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
    0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
    0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
];

pub struct CommitResult {
    root_hash: [u8; 32],
    root_offset: i64,
}

pub struct Trie {
    root_offset: Option<i64>,
    store: Rc<RefCell<dyn Store>>,
    nodes: HashMap<i64, Node>,
    last_id: i64,
}

impl Trie {
    pub fn new(store: Rc<RefCell<dyn Store>>, root_offset: Option<i64>) -> Self {
        Self {
            root_offset,
            store,
            nodes: HashMap::new(),
            last_id: -100,
        }
    }

    pub fn new_empty(store: Rc<RefCell<dyn Store>>) -> Self {
        Trie::new(store, None)
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let mut path = Nibbles::from_bytes(key);
        if self.root_offset.is_none() {
            let leaf = Node::Leaf(Leaf::new(path, value.to_vec()));
            self.root_offset = Some(self.intern(leaf));
            return Ok(());
        }

        // If the root offset is > 0, immediately intern it since it's dirty.
        if self.root_offset.unwrap() > 0 {
            let root = self.get_node(self.root_offset.unwrap())?;
            let new_root = root.clone();
            self.root_offset = Some(self.intern(new_root));
        }

        let mut current_node_id = self.root_offset.unwrap();
        loop {
            let mut current_node = self.get_node(current_node_id)?;
            current_node.set_dirty(true);
            current_node.set_committed(false);

            match current_node {
                Node::Leaf(mut leaf) => {
                    let shared_prefix = leaf.path.intersection(&path);

                    if shared_prefix.len() == leaf.path.len() && shared_prefix.len() == path.len() {
                        leaf.value = value.to_vec();
                        self.nodes.insert(current_node_id, Node::Leaf(leaf));
                        break;
                    }

                    let mut branch = Branch::new();

                    if shared_prefix.len() == path.len() {
                        branch.value = Some(value.to_vec());
                    } else if shared_prefix.len() == leaf.path.len() {
                        branch.value = Some(leaf.value.clone());
                    }

                    if shared_prefix.len() < leaf.path.len() {
                        let child_nibble = leaf.path.at(shared_prefix.len());
                        let branch_path = leaf.path.slice_from(shared_prefix.len() + 1);
                        branch.children[child_nibble] = self.intern(Node::Leaf(Leaf::new(branch_path, leaf.value.clone())));
                    }

                    if shared_prefix.len() < path.len() {
                        let child_path = path.at(shared_prefix.len());
                        let branch_path = path.slice_from(shared_prefix.len() + 1);
                        branch.children[child_path] = self.intern(Node::Leaf(Leaf::new(branch_path, value.to_vec())));
                    }

                    if shared_prefix.len() > 0 {
                        let branch_id = self.intern(Node::Branch(branch));
                        let ext_path = leaf.path.slice_to(shared_prefix.len());

                        self.nodes.insert(current_node_id, Node::Extension(Extension::new(ext_path, branch_id)));
                    } else {
                        self.nodes.insert(current_node_id, Node::Branch(branch));
                    }

                    break;
                }
                Node::Extension(mut ext) => {
                    let shared_prefix = ext.path.intersection(&path);

                    let child = self.get_node(ext.child)?;

                    // Shared prefix is the same, replace the child.
                    if shared_prefix.len() == ext.path.len() {
                        path = path.slice_from(shared_prefix.len());
                        let new_child = child.clone();
                        ext.child = self.intern(new_child);
                        self.nodes.insert(current_node_id, Node::Extension(ext.clone()));
                        current_node_id = ext.child;
                        continue;
                    }

                    // Shared prefix is a subset of the extension path, split the extension.
                    let matched_path = ext.path.slice_to(shared_prefix.len());
                    let branch_nibble = ext.path.at(shared_prefix.len());
                    let unmatched_path = ext.path.slice_from(shared_prefix.len() + 1);

                    let mut branch = Branch::new();

                    if unmatched_path.len() == 0 {
                        branch.children[branch_nibble] = ext.child;
                    } else {
                        branch.children[branch_nibble] = self.intern(Node::Extension(Extension::new(unmatched_path, ext.child)));
                    }

                    if shared_prefix.len() < path.len() {
                        let child_path = path.at(shared_prefix.len());
                        let branch_path = path.slice_from(shared_prefix.len() + 1);
                        branch.children[child_path] = self.intern(Node::Leaf(Leaf::new(branch_path, value.to_vec())));
                    } else if shared_prefix.len() == path.len() {
                        branch.value = Some(value.to_vec());
                    } else {
                        unreachable!("shared_prefix.len() > path.len() -> should never happen");
                    }

                    if matched_path.len() == 0 {
                        self.nodes.insert(current_node_id, Node::Branch(branch));
                    } else {
                        let branch_id = self.intern(Node::Branch(branch));
                        self.nodes.insert(current_node_id, Node::Extension(Extension::new(matched_path, branch_id)));
                    }

                    break;
                }
                Node::Branch(mut branch) => {
                    if path.len() == 0 {
                        branch.value = Some(value.to_vec());
                        self.nodes.insert(current_node_id, Node::Branch(branch));
                        break;
                    }

                    let branch_nibble = path.at(0);
                    path = path.slice_from(1);

                    // This branch has no child at the branch nibble, so we create a leaf node.
                    if branch.children[branch_nibble] == 0 {
                        branch.children[branch_nibble] = self.intern(Node::Leaf(Leaf::new(path, value.to_vec())));
                        self.nodes.insert(current_node_id, Node::Branch(branch));
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
                    self.nodes.insert(current_node_id, Node::Branch(branch.clone()));
                    current_node_id = branch.children[branch_nibble];
                }
            }
        }

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        if self.root_offset.is_none() {
            return Err("root not found".into());
        }

        let mut path = Nibbles::from_bytes(key);
        let mut current_node_id = self.root_offset.unwrap();

        loop {
            let current_node = self.get_node(current_node_id)?;

            match current_node {
                Node::Leaf(leaf) => {
                    let shared_prefix = leaf.path.intersection(&path);

                    if shared_prefix.len() == leaf.path.len() && shared_prefix.len() == path.len() {
                        return Ok(leaf.value.clone());
                    }

                    return Err("key not found".into());
                }
                Node::Extension(ext) => {
                    let shared_prefix = ext.path.intersection(&path);

                    if shared_prefix.len() != ext.path.len() {
                        return Err("key not found".into());
                    }

                    current_node_id = ext.child;
                    path = path.slice_from(shared_prefix.len());
                }
                Node::Branch(branch) => {
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

    pub fn commit(&mut self) -> Result<CommitResult, Box<dyn std::error::Error>> {
        if self.root_offset.is_none() {
            return Err("root not found".into());
        }

        let root_hash = self.calculate_root()?;
        let root_offset = self.write_node(&mut self.get_node(self.root_offset.unwrap())?)?;
        self.root_offset = Some(root_offset);
        self.nodes.clear();
        self.store.borrow_mut().flush()?;

        Ok(CommitResult {
            root_hash,
            root_offset,
        })
    }

    fn intern(&mut self, node: Node) -> i64 {
        let id = self.last_id;
        self.nodes.insert(id, node);
        self.last_id -= 1;
        id
    }

    fn get_node(&self, offset: i64) -> Result<Node, Box<dyn std::error::Error>> {
        self.get_node_with_local_map(offset, &self.nodes)
    }

    fn get_node_with_local_map(&self, offset: i64, nodes: &HashMap<i64, Node>) -> Result<Node, Box<dyn std::error::Error>> {
        if offset < 0 {
            return nodes.get(&offset)
                .cloned()
                .ok_or("node not found here".into());
        }

        match nodes.get(&offset) {
            Some(node) => Ok((*node).clone()),
            None => {
                let node = self.store.borrow_mut().get(offset)?;
                Ok(node)
            }
        }
    }

    fn calculate_root(&mut self) -> Result<[u8; 32], Box<dyn std::error::Error>> {
        if self.root_offset.is_none() {
            return Ok(EMPTY_ROOT_HASH);
        }

        let mut nodes = std::mem::take(&mut self.nodes);
        let hash = self.hash_node(self.root_offset.unwrap(), &mut nodes)?;
        self.nodes = nodes;

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

    fn hash_node(&self, offset: i64, nodes: &mut HashMap<i64, Node>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut node = self.get_node_with_local_map(offset, nodes)?;

        if !node.is_dirty() {
            return Ok(node.hash().expect("node is clean but has no hash"));
        }

        let data = match node {
            Node::Extension(ref ext) => {
                let child_hash = self.hash_node(ext.child, nodes)?;
                let mut stream = RlpStream::new_list(2);
                stream.append(&ext.path.prefixed_bytes(false));
                if child_hash.len() < 32 {
                    stream.append_raw(&child_hash, 1);
                } else {
                    stream.append_raw(rlp_hash(child_hash).as_slice(), 1);
                }

                stream.out().to_vec()
            }
            Node::Leaf(ref leaf) => {
                let mut stream = RlpStream::new_list(2);
                stream.append(&leaf.path.prefixed_bytes(true))
                    .append(&leaf.value);
                stream.out().to_vec()
            }
            Node::Branch(ref branch) => {
                let mut stream = RlpStream::new_list(17);
                for child in &branch.children {
                    if *child == 0 {
                        stream.append_empty_data();
                    } else {
                        let child_hash = self.hash_node(*child, nodes)?;

                        if child_hash.len() < 32 {
                            stream.append_raw(&child_hash, 1);
                        } else {
                            stream.append_raw(rlp_hash(child_hash).as_slice(), 1);
                        }
                    }
                }

                match &branch.value {
                    Some(value) => {
                        stream.append(value)
                    }
                    None => {
                        stream.append_empty_data()
                    }
                };

                stream.out().to_vec()
            }
        };

        let out = if data.len() < 32 {
            data
        } else {
            let mut hash = [0u8; 32];
            if data.len() >= 32 {
                let mut hasher = tiny_keccak::Keccak::v256();
                hasher.update(data.as_slice());
                hasher.finalize(&mut hash);
            }
            hash.to_vec()
        };

        node.set_hash(out.clone());
        node.set_dirty(false);
        nodes.insert(offset, node.clone());
        Ok(out)
    }

    fn write_node(&mut self, node: &mut Node) -> Result<i64, Box<dyn std::error::Error>> {
        if node.is_dirty() {
            return Err("node is dirty".into());
        }

        if node.is_committed() {
            return Err("node is already committed".into());
        }

        match node {
            Node::Extension(ext) => {
                if ext.child < 0 {
                    let child = self.write_node(&mut self.get_node(ext.child)?)?;
                    ext.child = child;
                }
            }
            Node::Branch(branch) => {
                let children = branch.children.clone();
                for (i, child) in children.iter().enumerate() {
                    if *child >= 0 {
                        continue;
                    }

                    let child_offset = self.write_node(&mut self.get_node(*child)?)?;
                    branch.children[i] = child_offset;
                }
            }
            // Do nothing for leaves, since they are written directly.
            _ => {}
        }

        node.set_committed(true);
        let offset = self.store.borrow_mut().put(node.clone())?;
        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::store::MemoryStore;

    use super::*;

    #[test]
    fn test_root() -> Result<(), Box<dyn std::error::Error>> {
        let store = Rc::new(RefCell::new(MemoryStore::new()));
        let mut trie = Trie::new_empty(store);
        trie.insert(b"do", b"verb")?;
        trie.insert(b"horse", b"stallion")?;
        trie.insert(b"doge", b"coin")?;
        trie.insert(b"dog", b"puppy")?;
        assert_eq!(
            hex::encode(trie.calculate_root()?),
            "5991bb8c6514148a29db676a14ac506cd2cd5775ace63c30a4fe457715e9ac84",
        );
        Ok(())
    }

    #[test]
    fn test_get() -> Result<(), Box<dyn std::error::Error>> {
        let store = Rc::new(RefCell::new(MemoryStore::new()));
        let mut trie = Trie::new_empty(store);
        trie.insert(b"do", b"verb")?;
        trie.insert(b"horse", b"stallion")?;
        trie.insert(b"doge", b"coin")?;
        trie.insert(b"dog", b"puppy")?;
        assert_eq!(trie.get(b"do")?, b"verb");
        assert_eq!(trie.get(b"horse")?, b"stallion");
        assert_eq!(trie.get(b"doge")?, b"coin");
        assert_eq!(trie.get(b"dog")?, b"puppy");
        Ok(())
    }

    #[test]
    fn test_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let ms = MemoryStore::new();
        let store: Rc<RefCell<dyn Store>> = Rc::new(RefCell::new(ms));

        let mut trie1 = Trie::new_empty(Rc::clone(&store));
        trie1.insert(b"do", b"verb")?;
        trie1.insert(b"horse", b"stallion")?;
        trie1.insert(b"doge", b"coin")?;
        trie1.insert(b"dog", b"puppy")?;
        let result = trie1.commit()?;

        let trie2 = Trie::new(Rc::clone(&store), Some(result.root_offset));
        assert_eq!(trie2.get(b"do")?, b"verb");
        assert_eq!(trie2.get(b"horse")?, b"stallion");
        assert_eq!(trie2.get(b"doge")?, b"coin");
        assert_eq!(trie2.get(b"dog")?, b"puppy");
        Ok(())
    }

    #[cfg(feature = "bench")]
    mod bench {
        use super::*;

        use crate::store::{FileStore, CachingStore};

        #[test]
        fn bench_10000_sets() -> Result<(), Box<dyn std::error::Error>> {
            let file_store = FileStore::new("/tmp/test.db")?;
            let cache_store = CachingStore::new(file_store);
            let store: Rc<RefCell<dyn Store>> = Rc::new(RefCell::new(cache_store));
            let mut trie = Trie::new_empty(Rc::clone(&store));

            let binding = hex::decode("f8448080a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")?;
            let empty_acc = binding.as_slice();

            let mut seed = hmac_sha256::Hash::hash(b"all your base are belong to us");;
            let mut last_result: CommitResult;
            for _ in 0..25 {
                let mut inputs = get_kvs(&seed);
                seed = inputs.1;

                println!("starting 10000 sets");
                let now = std::time::Instant::now();
                for key in inputs.0 {
                    trie.insert(&key, empty_acc).unwrap();
                }
                last_result = trie.commit().unwrap();
                let since = now.elapsed();
                println!("10000 sets took {:?}", since);
                trie = Trie::new(Rc::clone(&store), Some(last_result.root_offset));
            }

            Ok(())
        }
    }
}

fn get_kvs(data: &[u8; 32]) -> ([[u8; 32]; 10000], [u8; 32]) {
    let mut last_data = data;
    let mut out = [[0; 32]; 10000];

    for i in 0..10000 {
        out[i] = hmac_sha256::Hash::hash(last_data);
        last_data = &out[i];
    }

    (out, *last_data)
}

fn rlp_hash(hash: Vec<u8>) -> Vec<u8> {
    if hash.len() != 32 {
        panic!("hash must be 32 bytes");
    }

    [[0x80 + 32u8].as_slice(), hash.as_slice()].concat()
}