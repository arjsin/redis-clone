use std::fmt::Write;

use anyhow::{Error, Result};
use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::{Decoder, Encoder},
};

#[derive(Debug)]
pub enum Resp {
    SimpleString(String),
    SimpleError(String),
    BulkString(Vec<u8>),
    Null,
    Array(Vec<Resp>),
    NullArray,
}

pub struct RespCodec;

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn from_slice(slice: &'a [u8]) -> Cursor<'a> {
        Cursor { buf: slice, pos: 0 }
    }

    fn cur(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    fn split_to(&mut self, at: usize) -> Option<&[u8]> {
        let buf = self.cur();
        if buf.len() < at {
            return None;
        }
        let split = &buf[0..at];
        self.pos += at;
        Some(split)
    }

    fn find_crlf(&self) -> Option<usize> {
        self.cur().windows(2).position(|w| w == b"\r\n")
    }

    fn take_line(&mut self) -> Option<&'a [u8]> {
        let len = self.find_crlf()?;
        let buf = self.cur();
        if buf.len() < len + 2 {
            return None;
        }
        let line = &buf[0..len];
        self.pos += len + 2;
        Some(line)
    }

    fn take_line_string(&mut self) -> Result<Option<String>, Error> {
        let Some(line) = self.take_line() else {
            return Ok(None);
        };

        String::from_utf8(line.to_vec()).map(Some).map_err(Into::into)
    }

    fn take_line_i32(&mut self) -> Result<Option<i32>, Error> {
        let Some(line) = self.take_line() else {
            return Ok(None);
        };

        str::from_utf8(line)?
            .parse::<i32>()
            .map(Some)
            .map_err(Into::into)
    }

    fn take_bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        let buf = self.cur();
        if buf.len() < len + 2 {
            return None;
        }
        let bytes = &buf[0..len];
        self.pos += len + 2;
        Some(bytes)
    }
}

fn decode_cursor(cursor: &mut Cursor) -> Result<Option<Resp>> {
    let Some(first_byte) = cursor.split_to(1) else {
        return Ok(None);
    };
    match first_byte[0] {
        // Simple string
        b'+' => {
            let Some(string) = cursor.take_line_string()? else {
                return Ok(None);
            };
            Ok(Some(Resp::SimpleString(string)))
        }
        // Simple error
        b'-' => {
            let Some(error) = cursor.take_line_string()? else {
                return Ok(None);
            };
            Ok(Some(Resp::SimpleError(error)))
        }
        // Bulk string
        b'$' => {
            let Some(len) = cursor.take_line_i32()? else {
                return Ok(None);
            };
            match len {
                -1 => Ok(Some(Resp::Null)),
                n if n >= 0 => {
                    let Some(data) = cursor.take_bytes(len as usize) else {
                        return Ok(None);
                    };
                    let data = data.to_vec();
                    Ok(Some(Resp::BulkString(data)))
                }
                _ => anyhow::bail!("protocol error"),
            }
        }
        // Arrays
        b'*' => {
            let Some(len) = cursor.take_line_i32()? else {
                return Ok(None);
            };
            match len {
                -1 => Ok(Some(Resp::NullArray)),
                n if n >= 0 => {
                    let mut array = vec![];
                    for _ in 0..len {
                        let Some(element) = decode_cursor(cursor)? else {
                            return Ok(None);
                        };
                        array.push(element);
                    }
                    Ok(Some(Resp::Array(array)))
                }
                _ => anyhow::bail!("protocol error"),
            }
        }
        _ => {
            anyhow::bail!("protocol error: unknown type byte 0x{:02x}", first_byte[0])
        }
    }
}

impl Decoder for RespCodec {
    type Item = Resp;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut cursor = Cursor::from_slice(src);
        let resp = decode_cursor(&mut cursor);
        if let Ok(Some(_)) = resp {
            src.advance(cursor.pos);
        }
        resp
    }
}

impl Encoder<Resp> for RespCodec {
    type Error = Error;
    fn encode(&mut self, item: Resp, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Resp::SimpleString(s) => {
                write!(dst, "+{}\r\n", s)?;
            }
            Resp::SimpleError(s) => {
                write!(dst, "-{}\r\n", s)?;
            }
            Resp::BulkString(s) => {
                write!(dst, "${}\r\n", s.len())?;
                dst.extend_from_slice(s.as_slice());
                dst.write_str("\r\n")?;
            }
            Resp::Array(a) => {
                write!(dst, "*{}\r\n", a.len())?;
                for i in a {
                    self.encode(i, dst)?;
                }
            }
            Resp::Null => {
                write!(dst, "$-1\r\n")?;
            }
            Resp::NullArray => {
                write!(dst, "*-1\r\n")?;
            }
        }
        Ok(())
    }
}
