use crate::protocol::{Resp, RespCodec};
use anyhow::Error;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

pub async fn process(socket: TcpStream, shutdown: CancellationToken) {
    let mut framed = Framed::new(socket, RespCodec);
    loop {
        let frame = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            frame = framed.next() => frame,
        };
        let Some(frame) = frame else {
            break;
        };

        let sent = match frame {
            Ok(f) => match Command::try_from(f) {
                Ok(c) => framed.send(c.execute()).await,
                Err(e) => framed.send(Resp::SimpleError(e.to_string())).await,
            },
            Err(e) => {
                // A decode error leaves the stream desynced and can't be recovered,
                // so report and close it.
                let _ = framed.send(Resp::SimpleError(e.to_string())).await;
                break;
            }
        };
        if let Err(e) = sent {
            eprintln!("error while sending to client: {e}");
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
    fn execute(self) -> Resp {
        match self {
            Command::Ping => Resp::SimpleString("PONG".to_string()),
            Command::Echo(msg) => Resp::BulkString(msg),
        }
    }
}
