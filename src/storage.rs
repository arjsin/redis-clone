use std::{
    collections::{HashMap, VecDeque},
    fmt,
    time::{Duration, Instant},
};

use bytes::Bytes;

#[derive(Default)]
pub struct Storage(HashMap<Bytes, Entry>);

pub struct Entry {
    value: Value,
    expire_at: Option<Instant>,
}

pub enum Value {
    String(Bytes),
    List(VecDeque<Bytes>),
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
    pub fn set(&mut self, key: Bytes, value: Bytes, expire_in: Option<Duration>) {
        self.0.insert(
            key,
            Entry {
                value: Value::String(value),
                expire_at: expire_in.and_then(|ttl| Instant::now().checked_add(ttl)),
            },
        );
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        match self.live(key).map(|entry| &entry.value) {
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(StorageError::WrongType),
            None => Ok(None),
        }
    }

    pub fn lrange(
        &mut self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Bytes>, StorageError> {
        match self.live(key).map(|entry| &entry.value) {
            Some(Value::List(list)) => {
                let Some((start, stop)) = normalize_range(list.len(), start, stop) else {
                    return Ok(Vec::new());
                };
                Ok(list
                    .iter()
                    .skip(start)
                    .take(stop - start + 1)
                    .cloned()
                    .collect())
            }
            Some(_) => Err(StorageError::WrongType),
            None => Ok(Vec::new()),
        }
    }

    pub fn rpush(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, StorageError> {
        match self.live_mut(&key) {
            Some(Entry {
                value: Value::List(list),
                ..
            }) => {
                list.extend(values);
                Ok(list.len())
            }
            Some(_) => Err(StorageError::WrongType),
            None => {
                let list: VecDeque<Bytes> = values.into_iter().collect();
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

    /// Look up a key, lazily removing it first if it has expired.
    fn live(&mut self, key: &[u8]) -> Option<&Entry> {
        self.remove_if_expired(key, Instant::now());
        self.0.get(key)
    }

    /// Mutable variant of [`Storage::live`].
    fn live_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        self.remove_if_expired(key, Instant::now());
        self.0.get_mut(key)
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

/// Convert Redis-style inclusive indices (which may be negative) into a
/// half-open-friendly `(start, stop)` pair, or `None` when the range is empty.
fn normalize_range(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    let len = i64::try_from(len).ok()?;
    let start = if start < 0 {
        start.saturating_add(len)
    } else {
        start
    }
    .max(0);
    let stop = if stop < 0 {
        stop.saturating_add(len)
    } else {
        stop
    }
    .min(len - 1);

    if start > stop {
        return None;
    }
    Some((usize::try_from(start).ok()?, usize::try_from(stop).ok()?))
}
