use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::io::{BufWriter, Read, Seek, Write};

use memmap2::{Mmap, MmapOptions};

use crate::node::Node;

pub trait Store {
    fn get(&mut self, offset: i64) -> Result<Node, Box<dyn Error>>;
    fn put(&mut self, node: Node) -> Result<i64, Box<dyn Error>>;

    fn flush(&mut self) -> io::Result<()>;
}

pub struct MemoryStore {
    nodes: Vec<Node>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
        }
    }
}

impl Store for MemoryStore {
    fn get(&mut self, offset: i64) -> Result<Node, Box<dyn Error>> {
        self.nodes.get(offset as usize - 1)
            .ok_or("node not found".into())
            .map(|n| n.clone())
    }

    fn put(&mut self, node: Node) -> Result<i64, Box<dyn Error>> {
        self.nodes.push(node);
        Ok(self.nodes.len() as i64)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct FileStore {
    file: std::fs::File,
    buf: Vec<u8>,
    disk_size: i64,
    mem_size: i64,
    mmap: Mmap,
}

impl FileStore {
    pub fn new(path: &str) -> Result<Self, Box<dyn Error>> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let size = file.seek(io::SeekFrom::End(0))?;
        Ok(Self {
            file: file.try_clone()?,
            buf: Vec::with_capacity(10 * 1024 * 1024),
            disk_size: size as i64,
            mem_size: size as i64,
            mmap: unsafe { MmapOptions::new().len(size as usize).map(&file)? },
        })
    }
}

impl Store for FileStore {
    fn get(&mut self, offset: i64) -> Result<Node, Box<dyn Error>> {
        if offset > self.mem_size {
            return Err("offset out of bounds".into());
        }

        let sizer: &[u8; 2] = &self.mmap[offset as usize..offset as usize + 2].try_into()?;
        let size = u16::from_be_bytes(*sizer) as usize;
        let data = &self.mmap[offset as usize + 2..offset as usize + 2 + size];
        let node = Node::from_slice(data)?;
        Ok(node)
    }

    fn put(&mut self, node: Node) -> Result<i64, Box<dyn Error>> {
        let mut buf = Vec::new();
        node.to_writer(&mut buf)?;
        self.buf.write_all(&(u16::to_be_bytes(buf.len() as u16) as [u8; 2]))?;
        self.buf.write_all(&buf)?;
        let offset = self.mem_size;
        self.mem_size += buf.len() as i64 + 2;
        Ok(offset)
    }

    fn flush(&mut self) -> io::Result<()> {
        let bw = &mut BufWriter::new(&self.file);
        bw.write_all(&self.buf)?;
        bw.flush()?;
        self.disk_size += self.buf.len() as i64;
        self.buf.clear();

        let mmap = unsafe {
            MmapOptions::new().len(self.disk_size as usize).map(&self.file)?
        };
        _ = std::mem::replace(&mut self.mmap, mmap);

        Ok(())
    }
}

pub struct CachingStore<S: Store> {
    store: S,
    cache: HashMap<i64, Node>,
}

impl<S: Store> CachingStore<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            cache: HashMap::new(),
        }
    }
}

impl<S: Store> Store for CachingStore<S> {
    fn get(&mut self, offset: i64) -> Result<Node, Box<dyn Error>> {
        match self.cache.get(&offset) {
            Some(node) => Ok(node.clone()),
            None => {
                let node = self.store.get(offset)?;
                self.cache.insert(offset, node.clone());
                Ok(node)
            }
        }
    }

    fn put(&mut self, node: Node) -> Result<i64, Box<dyn Error>> {
        let offset = self.store.put(node.clone())?;
        self.cache.insert(offset, node);
        Ok(offset)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.store.flush()
    }
}