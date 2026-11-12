use std::{
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let _ = thread::spawn(|| {
            process(stream);
        });
    }
}

fn process(stream: TcpStream) {
    let mut buf_read = BufReader::new(stream);
    let mut buf = [0u8; 512];
    loop {
        let size = buf_read.read(&mut buf).unwrap();
        if size == 0 {
            break;
        }
        let _ = buf_read.get_mut().write_all("+PONG\r\n".as_bytes());
    }
}
