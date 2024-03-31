use redis_starter_rust::resp::{Resp, SerDe};
use redis_starter_rust::{handle_input, NODE};
use std::io::Write;
use std::net::TcpStream as StdTcpStream;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, RwLock};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const REQUEST_BUFFER_SIZE: usize = 536870912;

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind(format!("127.0.0.1:{}", NODE.read().unwrap().port))
        .await
        .unwrap();

    if let Some(_) = NODE.read().unwrap().master {
        tokio::spawn(async move {
            handle_replication().await;
        });
    }

    let mut streams: Arc<RwLock<Vec<StdTcpStream>>> = Arc::new(RwLock::new(vec![]));
    let (sender, reciver): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::sync_channel(1024);
    let streamss = streams.clone();
    tokio::spawn(async move {
        for data in reciver {
            let mut streams = streamss.write().unwrap();
            for (i, stream) in streams.iter_mut().enumerate() {
                println!("sendig {} data to replica {}", data.len(), i);
                stream.write_all(&data);
            }
        }
    });
    loop {
        let sender = sender.clone();
        let streams = streams.clone();
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                tokio::spawn(async move {
                    let stream = handle_connection(stream, sender.clone()).await;
                    println!("sendig commands to replica");
                    let mut streams = streams.write().unwrap();
                    streams.push(stream.into_std().unwrap());
                });
            }
            Err(e) => {
                eprintln!("error: {:?}", e);
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, sender: SyncSender<Vec<u8>>) -> TcpStream {
    println!("accepted new connection");
    let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
    loop {
        if let Ok(n) = stream.read(&mut request_buffer).await {
            if n == 0 {
                eprintln!("read {} bytes", n);
                break stream;
            }
            println!("read {} bytes", n);
            println!(
                "REQ: {:?}",
                std::str::from_utf8(&request_buffer[..n]).unwrap()
            );
            let (tx, rx) = mpsc::sync_channel(1);
            let request = &request_buffer[..n];
            let response = handle_input(request, tx);
            match std::str::from_utf8(&response) {
                Ok(value) => {
                    println!("RES: {:?}", value);
                }
                Err(_) => {
                    println!("RES: {:?}", response);
                }
            }
            if let Err(e) = stream.write_all(&response).await {
                eprintln!("Error writing {:?}", e);
            }
            stream.flush().await.unwrap();
            if rx.try_iter().next().is_some() {
                NODE.write().unwrap().replicas.push(sender.clone());
                break stream;
            }
            //send data to replicas
            let replicas = &NODE.write().unwrap().replicas;
            for sender in replicas {
                sender.send(request.into());
            }
        } else {
            eprintln!("error reading from tcp stream");
            break stream;
        }
    }
}

async fn send_command_to_master(stream: &mut TcpStream, command: &[u8]) {
    let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
    let (mut reader, mut writer) = stream.split();
    writer.write_all(command).await.unwrap();
    writer.flush().await.unwrap();
    let n = reader.read(&mut request_buffer).await.unwrap();
    match std::str::from_utf8(&request_buffer[..n]) {
        Ok(value) => {
            println!("reply from master: {}", value);
        }
        Err(_) => {
            println!("reply from master: {:?}", &request_buffer[..n]);
        }
    }
}

async fn handle_replication() {
    let master = NODE.read().unwrap().master.clone().unwrap();
    let mut stream = TcpStream::connect(format!("{}:{}", master.host, master.port))
        .await
        .unwrap();
    let ping = SerDe::serialize(Resp::Array(vec![Resp::Binary("ping".as_bytes().into())]));
    let replconf_listen_port = SerDe::serialize(Resp::Array(vec![
        Resp::Binary("REPLCONF".as_bytes().into()),
        Resp::Binary("listening-port".as_bytes().into()),
        Resp::Binary(format!("{}", NODE.read().unwrap().port).as_bytes().into()),
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
    let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
    println!("listening to master for commands");
    loop {
        let n = stream.read(&mut request_buffer).await.unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        match std::str::from_utf8(&request_buffer[..n]) {
            Ok(value) => {
                println!("reply from master: {}", value);
            }
            Err(_) => {
                println!("reply from master: {:?}", &request_buffer[..n]);
            }
        }
        let _ = handle_input(&request_buffer[..n], tx);
        let _ = rx.try_recv();
    }
}
