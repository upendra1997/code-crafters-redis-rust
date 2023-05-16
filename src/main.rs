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
                loop {
                    let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
                    if let Ok(n) = stream.read(&mut request_buffer).await {
                        if n == 0 {
                            eprintln!("read {} bytes", n);
                            break;
                        }
                        println!("read {} bytes", n);
                        println!("REQ: {:?}", std::str::from_utf8(&request_buffer[..n]).unwrap());
                        let response = handle_input(&request_buffer[..n]);
                        println!("RES: {:?}", std::str::from_utf8(&response).unwrap());
                        if let Err(e) = stream.write_all(&response).await {
                            eprintln!("Error writing {:?}", e);
                        }
                        stream.flush();
                    } else {
                        eprintln!("error reading from tcp stream");
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("error: {:?}", e);
            }
        }
    }
}
