use revm::primitives::{B256, Bytecode, Bytes, U256};
use tiny_keccak::Hasher;
use sled;

pub trait KVStore {
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn get(&mut self, key: &[u8]) -> Option<Vec<u8>>;
    fn delete(&mut self, key: &[u8]);
}

pub struct SledKVStore {
    db: sled::Db,
}

impl KVStore for SledKVStore {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.db.insert(key, value).unwrap();
    }

    fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get(key).unwrap().map(|v| v.to_vec())
    }

    fn delete(&mut self, key: &[u8]) {
        self.db.remove(key).unwrap();
    }
}

pub struct RawDB {
    store: Box<dyn KVStore>,
}

impl RawDB {
    pub fn put_code(&mut self, code: Bytecode) {
        let mut hasher = tiny_keccak::Keccak::v256();
        hasher.update(&code.bytes());
        let mut out: [u8; 32] = [0; 32];
        hasher.finalize(&mut out);
        self.store.put(&out, &code.bytes());
    }

    pub fn get_code(&mut self, code_hash: B256) -> Option<Bytecode> {
        self.store.get(code_hash.as_slice())
            .map(|bytes| Bytecode::new_raw(Bytes::from(bytes)))
    }

    pub fn put_root(&mut self, root: B256, offset: i64) {
        self.store.put(root.as_slice(), &offset.to_be_bytes());
    }

    pub fn get_root(&mut self, root: B256) -> Option<i64> {
        self.store.get(root.as_slice())
            .map(|bytes| {
                let mut buf = [0; 8];
                buf.copy_from_slice(&bytes);
                i64::from_be_bytes(buf)
            })
    }

    pub fn put_block_hash(&mut self, number: U256, hash: B256) {
        self.store.put(&number.to_be_bytes::<32>(), hash.as_slice());
    }

    pub fn get_block_hash(&mut self, number: U256) -> Option<B256> {
        self.store.get(&number.to_be_bytes::<32>())
            .map(|bytes| {
                let mut buf = [0; 32];
                buf.copy_from_slice(&bytes);
                B256::from(buf)
            })
    }
}