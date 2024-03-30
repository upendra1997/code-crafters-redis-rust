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

async fn send_command_to_master(stream: &mut TcpStream, command: &[u8]) {
    let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
    let (mut reader, mut writer) = stream.split();
    writer.write_all(command).await.unwrap();
    writer.flush().await.unwrap();
    let n = reader.read(&mut request_buffer).await.unwrap();
    println!(
        "reply from master: {}",
        std::str::from_utf8(&request_buffer[..n]).unwrap()
    );
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
    let replconf_cap = SerDe::serialize(Resp::Array(vec![
        Resp::Binary("REPLCONF".as_bytes().into()),
        Resp::Binary("capa".as_bytes().into()),
        Resp::Binary("psync2".as_bytes().into()),
    ]));
    let psync_init = SerDe::serialize(Resp::Array(vec![
        Resp::Binary("PSYNC".as_bytes().into()),
        Resp::Binary(format!("{}", master.replicatio_id).as_bytes().into()),
        Resp::Binary(format!("{}", master.offset).as_bytes().into()),
    ]));
    send_command_to_master(&mut stream, &ping).await;
    send_command_to_master(&mut stream, &replconf_listen_port).await;
    send_command_to_master(&mut stream, &replconf_cap).await;
    send_command_to_master(&mut stream, &psync_init).await;
}
