// Uncomment this block to pass the first stage
use std::{
    io::{Read, Write},
    thread,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use redis_starter_rust::handle_input;

const REQUEST_BUFFER_SIZE: usize = 536870912;

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                println!("accepted new connection");
                let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
                while let Ok(n) = stream.read(&mut request_buffer).await {
                    if n == 0 {
                        eprintln!("read {} bytes", n);
                        break;
                    }
                    println!("read {} bytes", n);
                    let response = handle_input(&request_buffer[..n]);
                    stream.try_write(&response);
                    stream.flush();
                }
            }
            Err(e) => {
                eprintln!("error: {:?}", e);
            }
        }
    }
}
