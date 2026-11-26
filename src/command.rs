use std::time::Duration;

use crate::protocol::{Resp, RespCodec};
use crate::storage_async::StorageHandle;
use anyhow::Error;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

pub async fn process(socket: TcpStream, storage: StorageHandle, shutdown: CancellationToken) {
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
                Ok(c) => {
                    let reply = c.execute(&storage).await;
                    framed.send(reply).await
                }
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
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        expire_in: Option<Duration>,
    },
    Get(Vec<u8>),
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
                v => anyhow::bail!("unexpected command type {:?}", v),
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
                b"set" => {
                    let expire_in = match v.len() {
                        3 => None,
                        5 => {
                            let Some(Resp::BulkString(ms)) = v.pop() else {
                                anyhow::bail!("invalid arguments for 'set' command");
                            };
                            let Some(Resp::BulkString(unit)) = v.pop() else {
                                anyhow::bail!("invalid arguments for 'set' command");
                            };
                            if !unit.eq_ignore_ascii_case(b"px") {
                                anyhow::bail!("syntax error");
                            }
                            let ms: u64 = std::str::from_utf8(&ms)?.parse()?;
                            if ms == 0 {
                                anyhow::bail!("invalid expire time in 'set' command");
                            }
                            Some(Duration::from_millis(ms))
                        }
                        _ => anyhow::bail!("wrong number of arguments for 'set' command"),
                    };
                    let Some(Resp::BulkString(value)) = v.pop() else {
                        anyhow::bail!("invalid arguments for 'set' command");
                    };
                    let Some(Resp::BulkString(key)) = v.pop() else {
                        anyhow::bail!("invalid arguments for 'set' command");
                    };
                    Ok(Command::Set {
                        key,
                        value,
                        expire_in,
                    })
                }
                b"get" => {
                    if v.len() != 2 {
                        anyhow::bail!("wrong number of arguments for 'get' command");
                    }
                    let Some(Resp::BulkString(key)) = v.pop() else {
                        anyhow::bail!("invalid arguments for 'get' command");
                    };
                    Ok(Command::Get(key))
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
    async fn execute(self, storage: &StorageHandle) -> Resp {
        match self {
            Command::Ping => Resp::SimpleString("PONG".to_string()),
            Command::Echo(msg) => Resp::BulkString(msg),
            Command::Set {
                key,
                value,
                expire_in,
            } => match storage.set(key, value, expire_in).await {
                Ok(()) => Resp::SimpleString("OK".to_string()),
                Err(err) => Resp::SimpleError(format!("ERR {err}")),
            },
            Command::Get(key) => match storage.get(key).await {
                Ok(Some(value)) => Resp::BulkString(value),
                Ok(None) => Resp::Null,
                Err(err) => Resp::SimpleError(format!("ERR {err}")),
            },
        }
    }
}
