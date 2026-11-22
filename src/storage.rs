use std::collections::{HashMap, VecDeque};

#[derive(Default)]
pub struct Storage {
    inner: HashMap<Vec<u8>, Entry>,
}

pub enum Entry {
    String(Vec<u8>),
    #[allow(dead_code)] // reserved for list commands
    List(VecDeque<Vec<u8>>),
}

impl Storage {
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.inner.insert(key, Entry::String(value));
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.inner.get(key) {
            Some(Entry::String(value)) => Some(value.clone()),
            _ => None,
        }
    }
}
