use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        process(socket).await;
    }
}

async fn process(mut socket: TcpStream) {
    let (mut rx, mut tx) = socket.split();
    let mut buf = [0u8; 512];
    loop {
        let size = rx.read(&mut buf).await.unwrap();
        if size == 0 {
            break;
        }
        tx.write_all(b"+PONG\r\n").await.unwrap();
    }
}
