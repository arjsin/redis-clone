mod resp;

use futures::{SinkExt, StreamExt};
use resp::{Resp, RespCodec};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let _ = tokio::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(socket: TcpStream) {
    let mut framed = Framed::new(socket, RespCodec);
    loop {
        let Some(frame) = framed.next().await else {
            return;
        };
        match frame {
            Err(e) => {
                println!("unexpected error {}", e);
            }
            Ok(Resp::Array(v)) => {
                if v.len() == 0 {
                    continue;
                }
                let command = match &v[0] {
                    Resp::BulkString(s) => s.to_ascii_lowercase(),
                    v => {
                        println!("unexpected commend type {:?}", v);
                        continue;
                    }
                };
                match command.as_str() {
                    "ping" => {
                        framed
                            .send(Resp::SimpleString("PONG".to_string()))
                            .await
                            .unwrap();
                    }
                    "echo" => {
                        let msg = if v.len() > 1
                            && let Resp::BulkString(s) = &v[1]
                        {
                            s.clone()
                        } else {
                            String::new()
                        };
                        framed.send(Resp::BulkString(msg)).await.unwrap();
                    }
                    v => {
                        println!("unexpected BulkString {:?}", v);
                    }
                };
            }
            Ok(v) => {
                println!("unexpected {:?}", v);
            }
        }
    }
}
