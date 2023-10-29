use std::collections::HashMap;
use std::error::Error;
use crate::node::Node;


pub trait Store {
    fn get(&self, offset: i64) -> Result<Node, Box<dyn Error>>;
    fn put(&mut self, node: Node) -> Result<i64, Box<dyn Error>>;
}

pub struct MemoryStore {
    nodes: HashMap<i64, Node>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    fn clear(&mut self) {
        self.nodes.clear();
    }
}

impl Store for MemoryStore {
    fn get(&self, offset: i64) -> Result<Node, Box<dyn Error>> {
        match self.nodes.get(&offset) {
            Some(node) => Ok(node.clone()),
            None => Err("node not found".into()),
        }
    }

    fn put(&mut self, node: Node) -> Result<i64, Box<dyn Error>> {
        let offset = self.nodes.len() as i64;
        self.nodes.insert(offset, node);
        Ok(offset)
    }
}