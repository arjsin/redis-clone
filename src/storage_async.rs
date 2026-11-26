use std::{fmt, time::Duration};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::storage::Storage;

/// How many requests the actor can queue before senders start waiting.
const MAILBOX_CAPACITY: usize = 1_024;

/// Returned when the storage actor is no longer running.
#[derive(Debug, Clone, Copy)]
pub struct StorageClosed;

impl fmt::Display for StorageClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("storage actor is not running")
    }
}

impl std::error::Error for StorageClosed {}

/// A unit of work applied to the owned [`Storage`] on the actor task.
type Job = Box<dyn FnOnce(&mut Storage) + Send + 'static>;

/// Cloneable handle to the storage actor.
#[derive(Clone)]
pub struct StorageHandle {
    tx: mpsc::Sender<Job>,
}

impl StorageHandle {
    /// Start the actor and return a handle plus its task.
    pub fn spawn() -> (Self, JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<Job>(MAILBOX_CAPACITY);
        let task = tokio::spawn(async move {
            let mut storage = Storage::default();
            while let Some(job) = rx.recv().await {
                job(&mut storage);
            }
        });

        (Self { tx }, task)
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, StorageClosed> {
        self.call(move |storage| storage.get(&key)).await
    }

    pub async fn set(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expire_in: Option<Duration>,
    ) -> Result<(), StorageClosed> {
        self.call(move |storage| storage.set(key, value, expire_in))
            .await
    }

    /// Queue `op` on the actor and await its result.
    async fn call<T: Send + 'static>(
        &self,
        op: impl FnOnce(&mut Storage) -> T + Send + 'static,
    ) -> Result<T, StorageClosed> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let job: Job = Box::new(move |storage| {
            let _ = reply_tx.send(op(storage));
        });

        self.tx.send(job).await.map_err(|_| StorageClosed)?;
        reply_rx.await.map_err(|_| StorageClosed)
    }
}
