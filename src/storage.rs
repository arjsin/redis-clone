use std::{
    collections::{HashMap, VecDeque},
    fmt,
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
    List(VecDeque<Vec<u8>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    WrongType,
    Closed,
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongType => {
                f.write_str("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            Self::Closed => f.write_str("ERR storage actor is not running"),
        }
    }
}

impl std::error::Error for StorageError {}

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

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let now = Instant::now();
        self.remove_if_expired(key, now);

        match self.0.get(key).map(|entry| &entry.value) {
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(StorageError::WrongType),
            None => Ok(None),
        }
    }

    pub fn rpush(&mut self, key: Vec<u8>, values: Vec<Vec<u8>>) -> Result<usize, StorageError> {
        let now = Instant::now();
        self.remove_if_expired(&key, now);

        match self.0.get_mut(&key) {
            Some(Entry {
                value: Value::List(list),
                ..
            }) => {
                list.extend(values);
                Ok(list.len())
            }
            Some(_) => Err(StorageError::WrongType),
            None => {
                let list: VecDeque<Vec<u8>> = values.into_iter().collect();
                let len = list.len();
                self.0.insert(
                    key,
                    Entry {
                        value: Value::List(list),
                        expire_at: None,
                    },
                );
                Ok(len)
            }
        }
    }

    fn remove_if_expired(&mut self, key: &[u8], now: Instant) {
        let expired = self
            .0
            .get(key)
            .is_some_and(|entry| entry.expire_at.is_some_and(|deadline| deadline <= now));

        if expired {
            self.0.remove(key);
        }
    }
}
