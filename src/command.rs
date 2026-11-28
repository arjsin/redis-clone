use std::fmt;
use std::time::Duration;

use bytes::Bytes;

use crate::protocol::{Resp, RespCodec};
use crate::storage::StorageError;
use crate::storage_async::StorageHandle;
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
                    let reply = c.execute(&storage).await.unwrap_or_else(Resp::from);
                    framed.send(reply).await
                }
                Err(e) => framed.send(Resp::from(e)).await,
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

#[derive(Debug)]
pub enum CommandError {
    InvalidFrame,
    EmptyCommand,
    InvalidCommandName,
    UnknownCommand(String),
    WrongArity(&'static str),
    InvalidArguments(&'static str),
    InvalidInteger,
    Syntax,
    InvalidExpire(&'static str),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFrame => f.write_str("ERR invalid frame type"),
            Self::EmptyCommand => f.write_str("ERR unexpected empty array frame"),
            Self::InvalidCommandName => f.write_str("ERR unexpected command type"),
            Self::UnknownCommand(name) => write!(f, "ERR unknown command '{name}'"),
            Self::WrongArity(cmd) => {
                write!(f, "ERR wrong number of arguments for '{cmd}' command")
            }
            Self::InvalidArguments(cmd) => {
                write!(f, "ERR invalid arguments for '{cmd}' command")
            }
            Self::InvalidInteger => f.write_str("ERR value is not an integer or out of range"),
            Self::Syntax => f.write_str("ERR syntax error"),
            Self::InvalidExpire(cmd) => {
                write!(f, "ERR invalid expire time in '{cmd}' command")
            }
        }
    }
}

impl std::error::Error for CommandError {}

impl From<CommandError> for Resp {
    fn from(err: CommandError) -> Self {
        Resp::SimpleError(err.to_string())
    }
}

pub enum Command {
    Ping,
    Echo(Bytes),
    Set {
        key: Bytes,
        value: Bytes,
        expire_in: Option<Duration>,
    },
    Get(Bytes),
    RPush {
        key: Bytes,
        values: Vec<Bytes>,
    },
    LRange {
        key: Bytes,
        start: i64,
        stop: i64,
    },
}

impl TryFrom<Resp> for Command {
    type Error = CommandError;

    fn try_from(value: Resp) -> Result<Self, Self::Error> {
        let Resp::Array(mut v) = value else {
            return Err(CommandError::InvalidFrame);
        };
        if v.is_empty() {
            return Err(CommandError::EmptyCommand);
        }
        let Resp::BulkString(name) = v.remove(0) else {
            return Err(CommandError::InvalidCommandName);
        };
        let name = String::from_utf8_lossy(&name).into_owned();

        match name.to_ascii_lowercase().as_str() {
            "ping" => Ok(Command::Ping),
            "echo" => {
                if v.len() != 1 {
                    return Err(CommandError::WrongArity("echo"));
                }
                let Some(Resp::BulkString(msg)) = v.pop() else {
                    return Err(CommandError::InvalidArguments("echo"));
                };
                Ok(Command::Echo(msg))
            }
            "set" => {
                let expire_in = match v.len() {
                    2 => None,
                    4 => {
                        let Some(Resp::BulkString(ms)) = v.pop() else {
                            return Err(CommandError::InvalidArguments("set"));
                        };
                        let Some(Resp::BulkString(unit)) = v.pop() else {
                            return Err(CommandError::InvalidArguments("set"));
                        };
                        if !unit.eq_ignore_ascii_case(b"px") {
                            return Err(CommandError::Syntax);
                        }
                        let ms: u64 = std::str::from_utf8(&ms)
                            .ok()
                            .and_then(|ms| ms.parse().ok())
                            .ok_or(CommandError::InvalidInteger)?;
                        if ms == 0 {
                            return Err(CommandError::InvalidExpire("set"));
                        }
                        Some(Duration::from_millis(ms))
                    }
                    _ => return Err(CommandError::WrongArity("set")),
                };
                let Some(Resp::BulkString(value)) = v.pop() else {
                    return Err(CommandError::InvalidArguments("set"));
                };
                let Some(Resp::BulkString(key)) = v.pop() else {
                    return Err(CommandError::InvalidArguments("set"));
                };
                Ok(Command::Set {
                    key,
                    value,
                    expire_in,
                })
            }
            "get" => {
                if v.len() != 1 {
                    return Err(CommandError::WrongArity("get"));
                }
                let Some(Resp::BulkString(key)) = v.pop() else {
                    return Err(CommandError::InvalidArguments("get"));
                };
                Ok(Command::Get(key))
            }
            "rpush" => {
                if v.len() < 2 {
                    return Err(CommandError::WrongArity("rpush"));
                }
                let mut args = v.into_iter();
                let Some(Resp::BulkString(key)) = args.next() else {
                    return Err(CommandError::InvalidArguments("rpush"));
                };
                let mut values = Vec::new();
                for arg in args {
                    let Resp::BulkString(value) = arg else {
                        return Err(CommandError::InvalidArguments("rpush"));
                    };
                    values.push(value);
                }
                Ok(Command::RPush { key, values })
            }
            "lrange" => {
                if v.len() != 3 {
                    return Err(CommandError::WrongArity("lrange"));
                }
                let Some(Resp::BulkString(stop)) = v.pop() else {
                    return Err(CommandError::InvalidArguments("lrange"));
                };
                let Some(Resp::BulkString(start)) = v.pop() else {
                    return Err(CommandError::InvalidArguments("lrange"));
                };
                let Some(Resp::BulkString(key)) = v.pop() else {
                    return Err(CommandError::InvalidArguments("lrange"));
                };
                let start = parse_integer(&start)?;
                let stop = parse_integer(&stop)?;
                Ok(Command::LRange { key, start, stop })
            }
            _ => Err(CommandError::UnknownCommand(name)),
        }
    }
}

impl Command {
    async fn execute(self, storage: &StorageHandle) -> Result<Resp, StorageError> {
        match self {
            Command::Ping => Ok(Resp::SimpleString("PONG".to_string())),
            Command::Echo(msg) => Ok(Resp::BulkString(msg)),
            Command::Set {
                key,
                value,
                expire_in,
            } => {
                storage.set(key, value, expire_in).await?;
                Ok(Resp::SimpleString("OK".to_string()))
            }
            Command::Get(key) => Ok(match storage.get(key).await? {
                Some(value) => Resp::BulkString(value),
                None => Resp::Null,
            }),
            Command::RPush { key, values } => {
                Ok(Resp::Integer(storage.rpush(key, values).await? as i64))
            }
            Command::LRange { key, start, stop } => Ok(Resp::Array(
                storage
                    .lrange(key, start, stop)
                    .await?
                    .into_iter()
                    .map(Resp::BulkString)
                    .collect(),
            )),
        }
    }
}

impl From<StorageError> for Resp {
    fn from(err: StorageError) -> Self {
        Resp::SimpleError(err.to_string())
    }
}

fn parse_integer(bytes: &[u8]) -> Result<i64, CommandError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::InvalidInteger)
}
