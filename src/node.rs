use crate::nibbles::Nibbles;

#[derive(Clone)]
pub struct Extension {
    pub path: Nibbles,
    pub child: i64,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct Leaf {
    pub path: Nibbles,
    pub value: Vec<u8>,
}

#[derive(Clone)]
pub struct Branch {
    pub children: [i64; 16],
    pub value: Option<Vec<u8>>,
}

#[derive(Clone)]
pub enum Node {
    Extension(Extension),
    Leaf(Leaf),
    Branch(Branch),
}