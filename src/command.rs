use crate::protocol::{Resp, RespCodec};
use anyhow::Error;
use futures::{Sink, SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

pub async fn process(socket: TcpStream) {
    let mut framed = Framed::new(socket, RespCodec);
    while let Some(frame) = framed.next().await {
        let v = match frame {
            Ok(f) => match Command::try_from(f) {
                Ok(c) => c.handle(&mut framed).await,
                Err(e) => framed.send(Resp::SimpleError(e.to_string())).await,
            },
            Err(e) => framed.send(Resp::SimpleError(e.to_string())).await,
        };
        if let Err(e) = v {
            println!("error while sending to client: {}", e);
            break;
        }
    }
}

pub enum Command {
    Ping,
    Echo(Vec<u8>),
}

impl TryFrom<Resp> for Command {
    type Error = Error;

    fn try_from(value: Resp) -> Result<Self, Self::Error> {
        if let Resp::Array(mut v) = value {
            if v.is_empty() {
                anyhow::bail!("unexpected empty array frame");
            }
            let command = match &v[0] {
                Resp::BulkString(s) => s.to_ascii_lowercase(),
                v => {
                    anyhow::bail!("unexpected command type {:?}", v);
                }
            };
            match command.as_slice() {
                b"ping" => Ok(Command::Ping),
                b"echo" => {
                    if v.len() != 2 {
                        anyhow::bail!("wrong number of arguments for 'echo' command");
                    }
                    let Some(Resp::BulkString(msg)) = v.pop() else {
                        anyhow::bail!("invalid arguments for 'echo' command");
                    };
                    Ok(Command::Echo(msg))
                }
                v => {
                    anyhow::bail!("unexpected BulkString {:?}", v);
                }
            }
        } else {
            anyhow::bail!("invalid frame type");
        }
    }
}

impl Command {
    async fn handle<S: Unpin + Sink<Resp>>(self, framed: &mut S) -> Result<(), Error>
    where
        <S as futures::Sink<Resp>>::Error: Into<Error>,
    {
        match self {
            Command::Ping => framed
                .send(Resp::SimpleString("PONG".to_string()))
                .await
                .map_err(Into::into),
            Command::Echo(msg) => framed.send(Resp::BulkString(msg)).await.map_err(Into::into),
        }
    }
}
