mod command;
mod protocol;
mod server;
mod storage;
mod storage_async;

use server::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = Server::bind("127.0.0.1:6379").await?;
    println!("listening on {}", server.local_addr());

    let shutdown = server.shutdown();
    let serve = server.serve();
    tokio::pin!(serve);

    tokio::select! {
        _ = shutdown_signal() => {
            println!("shutting down");
            shutdown.cancel();
            serve.await?;
        }
        result = &mut serve => result?,
    }

    Ok(())
}

/// Resolves when the process receives Ctrl+C or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        signal.recv().await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => println!("received Ctrl+C"),
        _ = terminate => println!("received terminate signal"),
    }
}
