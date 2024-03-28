use std::env::args;

// Uncomment this block to pass the first stage
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use redis_starter_rust::handle_input;

const REQUEST_BUFFER_SIZE: usize = 536870912;

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");
    let mut port = 6379;
    if std::env::args().len() > 1 && std::env::args().into_iter().nth(1).unwrap() == "--port" {
        port = std::env::args().nth(2).unwrap().parse().unwrap();
    }
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    loop {
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                tokio::spawn(async move {
                    println!("accepted new connection");
                    let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
                    loop {
                        if let Ok(n) = stream.read(&mut request_buffer).await {
                            if n == 0 {
                                eprintln!("read {} bytes", n);
                                break;
                            }
                            println!("read {} bytes", n);
                            println!(
                                "REQ: {:?}",
                                std::str::from_utf8(&request_buffer[..n]).unwrap()
                            );
                            let response = handle_input(&request_buffer[..n]);
                            println!("RES: {:?}", std::str::from_utf8(&response).unwrap());
                            if let Err(e) = stream.write_all(&response).await {
                                eprintln!("Error writing {:?}", e);
                            }
                            stream.flush().await.unwrap();
                        } else {
                            eprintln!("error reading from tcp stream");
                            break;
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("error: {:?}", e);
            }
        }
    }
}
