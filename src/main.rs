// Uncomment this block to pass the first stage
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use redis_starter_rust::resp::{Resp, SerDe};
use redis_starter_rust::{handle_input, NODE};

const REQUEST_BUFFER_SIZE: usize = 536870912;

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind(format!("127.0.0.1:{}", NODE.port))
        .await
        .unwrap();

    if let Some(_) = NODE.master {
        tokio::spawn(async move {
            handle_replication().await;
        });
    }

    loop {
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                tokio::spawn(async move {
                    handle_connection(stream).await;
                });
            }
            Err(e) => {
                eprintln!("error: {:?}", e);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream) {
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
}

async fn handle_replication() {
    let master = NODE.master.clone().unwrap();
    let mut stream = TcpStream::connect(format!("{}:{}", master.host, master.port))
        .await
        .unwrap();
    let ping = SerDe::serialize(Resp::Array(vec![Resp::Binary("ping".as_bytes().into())]));
    let replconf_listen_port = SerDe::serialize(Resp::Array(vec![
        Resp::Binary("REPLCONF".as_bytes().into()),
        Resp::Binary("listening-port".as_bytes().into()),
        Resp::Binary(format!("{}", NODE.port).as_bytes().into()),
    ]));
    let replconf_cap = SerDe::serialize(Resp::Array(vec![Resp::Binary(
        "REPLCONF capa psync2".as_bytes().into(),
    )]));
    stream.write_all(&ping).await.unwrap();
    stream.write_all(&replconf_listen_port).await.unwrap();
    stream.write_all(&replconf_cap).await.unwrap();
}
