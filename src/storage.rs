use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

#[derive(Default)]
pub struct Storage(HashMap<Vec<u8>, Entry>);

pub struct Entry {
    value: Value,
    expire_at: Option<Instant>,
}
pub enum Value {
    String(Vec<u8>),
    #[allow(dead_code)] // reserved for list commands
    List(VecDeque<Vec<u8>>),
}

impl Storage {
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expire_in: Option<Duration>) {
        self.0.insert(
            key,
            Entry {
                value: Value::String(value),
                expire_at: expire_in.and_then(|ttl| Instant::now().checked_add(ttl)),
            },
        );
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let now = Instant::now();
        let entry = self.0.get(key)?;

        if entry.expire_at.is_some_and(|deadline| deadline <= now) {
            self.0.remove(key);
            return None;
        }

        match &entry.value {
            Value::String(value) => Some(value.clone()),
            _ => None,
        }
    }
}
