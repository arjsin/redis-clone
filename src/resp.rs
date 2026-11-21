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
    BulkString(String),
    Array(Vec<Resp>),
}

pub struct RespCodec;

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn next_line(buf: &mut BytesMut) -> Option<BytesMut> {
    let len = find_crlf(buf)?;
    let line = buf.split_to(len);
    buf.advance(2);
    Some(line)
}

fn next_string(buf: &mut BytesMut) -> Option<String> {
    let line = next_line(buf)?;
    String::from_utf8(line.to_vec()).ok()
}

fn next_u32(buf: &mut BytesMut) -> Option<u32> {
    let len = next_line(buf)?;
    str::from_utf8(&len.to_vec()).ok()?.parse::<u32>().ok()
}

fn next_chunk(buf: &mut BytesMut, len: usize) -> Option<BytesMut> {
    if buf.len() < len + 2 {
        return None;
    }
    let chunk = buf.split_to(len);
    buf.advance(2);

    Some(chunk)
}

impl Decoder for RespCodec {
    type Item = Resp;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 1 {
            return Ok(None);
        }
        let first_byte = src.split_to(1);
        match first_byte[0] {
            // Simple string
            b'+' => {
                let Some(string) = next_string(src) else {
                    return Ok(None);
                };
                Ok(Some(Resp::SimpleString(string)))
            }
            // Simple error
            b'-' => {
                let Some(error) = next_string(src) else {
                    return Ok(None);
                };
                Ok(Some(Resp::SimpleError(error)))
            }
            // Bulk string
            b'$' => {
                let Some(len) = next_u32(src) else {
                    return Ok(None);
                };
                let Some(data) = next_chunk(src, len as usize) else {
                    return Ok(None);
                };
                let data = String::from_utf8(data.to_vec())?;
                Ok(Some(Resp::BulkString(data)))
            }
            // Arrays
            b'*' => {
                let Some(len) = next_u32(src) else {
                    return Ok(None);
                };
                let mut array = vec![];
                for _ in 0..len {
                    let Some(element) = self.decode(src)? else {
                        return Ok(None);
                    };
                    array.push(element);
                }
                Ok(Some(Resp::Array(array)))
            }
            _ => Ok(None),
        }
    }
}

impl Encoder<Resp> for RespCodec {
    type Error = Error;
    fn encode(&mut self, item: Resp, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Resp::SimpleString(s) => {
                let string = format!("+{}\r\n", s);
                dst.write_str(&string)?;
            }
            Resp::SimpleError(s) => {
                let string = format!("-{}\r\n", s);
                dst.write_str(&string)?;
            }
            Resp::BulkString(s) => {
                let string = format!("${}\r\n{}\r\n", s.len(), s);
                dst.write_str(&string)?;
            }
            Resp::Array(a) => {
                let string = format!("${}\r\n", a.len());
                dst.write_str(&string)?;
                for i in a {
                    self.encode(i, dst)?;
                }
            }
        }
        Ok(())
    }
}
