use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::command;
use crate::storage_async::StorageHandle;

/// How long to wait for active connections to finish before aborting them.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// A bound, not-yet-running Redis server.
///
/// Call [`Server::bind`] to reserve the address, then [`Server::serve`] to
/// accept connections until the shutdown token is cancelled.
pub struct Server {
    listener: TcpListener,
    local_addr: SocketAddr,
    shutdown: CancellationToken,
    storage: StorageHandle,
    storage_task: JoinHandle<()>,
}

impl Server {
    /// Bind `addr` without accepting connections yet.
    pub async fn bind(addr: impl ToSocketAddrs) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (storage, storage_task) = StorageHandle::spawn();

        Ok(Self {
            listener,
            local_addr,
            shutdown: CancellationToken::new(),
            storage,
            storage_task,
        })
    }

    /// The address the server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// A handle that stops the server when cancelled.
    pub fn shutdown(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Accept connections until the shutdown token is cancelled, then wait for
    /// active connections to finish (up to [`SHUTDOWN_TIMEOUT`]).
    pub async fn serve(self) -> std::io::Result<()> {
        let mut connections = JoinSet::new();

        loop {
            // Reap finished connections so their results do not accumulate.
            while let Some(result) = connections.try_join_next() {
                if let Err(err) = result {
                    eprintln!("connection task failed: {err}");
                }
            }

            tokio::select! {
                biased;

                _ = self.shutdown.cancelled() => break,

                accepted = self.listener.accept() => match accepted {
                    Ok((socket, _peer)) => {
                        let connection_shutdown = self.shutdown.clone();
                        let storage = self.storage.clone();
                        connections.spawn(async move {
                            command::process(socket, storage, connection_shutdown).await;
                        });
                    }
                    Err(err) => {
                        eprintln!("accept failed: {err}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                },
            }
        }

        drain_connections(&mut connections).await;

        // Closing the last handle lets the actor drain its mailbox and exit.
        let Server {
            storage,
            storage_task,
            ..
        } = self;
        drop(storage);
        if let Err(err) = storage_task.await {
            eprintln!("storage task failed: {err}");
        }

        Ok(())
    }
}

/// Wait for connections to finish, aborting when timeout.
async fn drain_connections(connections: &mut JoinSet<()>) {
    let finished = tokio::time::timeout(SHUTDOWN_TIMEOUT, async {
        while connections.join_next().await.is_some() {}
    })
    .await;

    if finished.is_err() {
        eprintln!(
            "shutdown timed out; aborting {} connection(s)",
            connections.len()
        );
        connections.shutdown().await;
    }
}
