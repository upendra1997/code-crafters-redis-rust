// Uncomment this block to pass the first stage
use std::{
    io::{Read, Write},
    net::TcpListener,
};

use redis_starter_rust::handle_input;

const REQUEST_BUFFER_SIZE: usize = 536870912;
const RESPONSE_BUFFER_SIZE: usize = 536870912;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
                let mut response_buffer = vec![0u8; RESPONSE_BUFFER_SIZE];
                match stream.read(&mut request_buffer) {
                    Ok(n) => {
                        println!("read {} bytes", n);
                        let response = handle_input(&request_buffer[..n]);
                        stream.write(&response);
                        stream.flush();
                    }
                    Err(e) => {
                        eprintln!("error reading from stream {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
