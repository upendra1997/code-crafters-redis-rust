use redis_starter_rust::resp::{Resp, SerDe};
use redis_starter_rust::{handle_input, SignalSender, TcpStreamMessage, NEW_NODE_NOTIFIER, NODE};
use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::net::TcpStream as StdTcpStream;
use std::ops::DerefMut;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, RwLock};
use tokio::sync::RwLock as SendableRwLock;
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

    let streams: Arc<SendableRwLock<BTreeMap<usize, (TcpStream, usize)>>> =
        Arc::new(SendableRwLock::new(BTreeMap::new()));
    let (sender, reciver): (SyncSender<TcpStreamMessage>, Receiver<TcpStreamMessage>) =
        mpsc::sync_channel(1024);
    let mut node = NODE.write().unwrap();
    node.data_sender = Some(sender.clone());
    drop(node);
    let streamss = streams.clone();
    let is_master = NODE.read().unwrap().master.is_none();
    if is_master {
        println!("Node is master");
    } else {
        println!("Node is replica");
    }
    if is_master {
        // TODO:
        // instead of passing TcpStream, we should only pass data, by creating a master replicatoin
        // handler which will push the data to the main sender, and there would be pair of sender
        // and reciver, on each TcpStream, and they will listen using the reciever,
        // need to ensure that, the broadcast does't reach the original sender.
        tokio::spawn(async move {
            let mut command_buffer: Vec<Vec<u8>> = Vec::new();
            for data in reciver {
                // match data {
                //     TcpStreamMessage::CountAcks(_) => {
                //         let mut streams = streamss.write().await;
                //         for (i, (stream, offset)) in streams.iter_mut().enumerate() {
                //         }
                //     }
                //     TcpStreamMessage::Data(data) => {
                let mut is_ack = false;
                if let TcpStreamMessage::Data(data) = data {
                    if data.len() == 0 {
                        continue;
                    }
                    command_buffer.push(data);
                } else {
                    is_ack = true;
                }
                let mut streams = streamss.write().await;
                let mut new_map = BTreeMap::new();
                while let Some(entry) = streams.first_entry() {
                    let (i, (mut stream, mut offset)) = entry.remove_entry();
                    let mut is_uselss = false;
                    let replconf_get_ack = SerDe::serialize(Resp::Array(vec![
                        Resp::Binary("REPLCONF".as_bytes().into()),
                        Resp::Binary("GETACK".as_bytes().into()),
                        Resp::Binary("*".as_bytes().into()),
                    ]));
                    for data in &command_buffer[offset..] {
                        if let Err(e) = stream.write_all(&data).await {
                            println!("removing replica from the master, because of {}", e);
                            is_uselss = true;
                            break;
                        }
                        println!("sendig {} to replica {}", String::from_utf8_lossy(data), i);
                        offset += 1;
                    }

                    if is_ack {
                        stream.write_all(&replconf_get_ack).await;
                        let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
                        let res = stream.read(&mut request_buffer).await;
                        match res {
                            Ok(n) => {
                                let response = &request_buffer[..n];
                                if n == 0 {
                                    is_uselss = true;
                                    continue;
                                }
                                let result = NODE
                                    .write()
                                    .unwrap()
                                    .replicas
                                    .fetch_add(1, Ordering::Relaxed);
                                let (mutex, cvar) = &*NEW_NODE_NOTIFIER.clone();
                                let mutex = mutex.lock().unwrap();
                                cvar.notify_all();
                                drop(mutex);
                                println!(
                                    "Replica {}:{} replied with {}:{}",
                                    i,
                                    result,
                                    n,
                                    String::from_utf8_lossy(response)
                                );
                            }
                            Err(e) => {
                                println!("Replica {} errored with {}", i, e);
                                is_uselss = true;
                            }
                        }
                    }
                    if !is_uselss {
                        new_map.insert(i, (stream, offset));
                    } else {
                        println!("removing replica {}, {:?}", i, stream);
                        //TODO: remove it here
                        new_map.insert(i, (stream, offset));
                    }
                }
                while let Some(entry) = new_map.first_entry() {
                    let (k, v) = entry.remove_entry();
                    streams.insert(k, v);
                }
            }
        });
    }
    loop {
        let sender = sender.clone();
        let streams = streams.clone();
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(async move {
                    if let Some(stream) = handle_connection(stream, sender).await {
                        if is_master {
                            println!("sending commands to replica");
                            let mut streams = streams.write().await;
                            let max = streams.keys().into_iter().max().map(|k| *k).unwrap_or(0);
                            streams.insert(max + 1, (stream, 0));
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

async fn handle_connection(
    mut stream: TcpStream,
    data_sender: SyncSender<TcpStreamMessage>,
) -> Option<TcpStream> {
    let is_master = NODE.read().unwrap().master.is_none();
    println!("accepted new connection");
    let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
    loop {
        match stream.read(&mut request_buffer).await {
            Ok(n) => {
                println!("read {} bytes", n);
                if n == 0 {
                    break None;
                }
                let mut request = &request_buffer[..n];
                loop {
                    if request.len() == 0 {
                        break;
                    }
                    let req = std::str::from_utf8(request).unwrap();
                    println!("REQ: {:?}", req);
                    let (tx, rx) = SignalSender::new();
                    let (response, n) = handle_input(request, tx);
                    let signals = rx.try_recv();
                    match std::str::from_utf8(&response) {
                        Ok(value) => {
                            println!("RES: {:?}", value);
                        }
                        Err(_) => {
                            println!("RES: {:?}", response);
                        }
                    }
                    //send data to replicas before replying to client
                    if is_master && signals.send_to_replica {
                        data_sender
                            .send(TcpStreamMessage::Data(request.into()))
                            .unwrap();
                    }
                    if let Err(e) = stream.write_all(&response).await {
                        eprintln!("Error writing {:?}", e);
                    }
                    stream.flush().await.unwrap();
                    if is_master && signals.new_node {
                        let result = NODE
                            .write()
                            .unwrap()
                            .replicas
                            .fetch_add(1, Ordering::Relaxed);
                        println!("New replica {} is being added by {}", result, req);
                        let (mutex, cvar) = &*NEW_NODE_NOTIFIER.clone();
                        let mutex = mutex.lock().unwrap();
                        cvar.notify_all();
                        drop(mutex);
                        return Some(stream);
                    }
                    request = &request[n..];
                }
            }
            Err(e) => {
                eprintln!("error reading from tcp stream: {:?}", e);
                break None;
            }
        }
    }
}

async fn send_command_to_master(
    stream: &mut TcpStream,
    command: &[u8],
    request_buffer: &mut [u8],
) -> usize {
    // let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
    let (mut reader, mut writer) = stream.split();
    writer.write_all(command).await.unwrap();
    writer.flush().await.unwrap();
    let n = reader.read(request_buffer).await.unwrap();
    println!(
        "reply from master: {}",
        String::from_utf8_lossy(&request_buffer[..n])
    );
    n
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
    let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
    send_command_to_master(&mut stream, &ping, &mut request_buffer).await;
    send_command_to_master(&mut stream, &replconf_listen_port, &mut request_buffer).await;
    send_command_to_master(&mut stream, &replconf_cap, &mut request_buffer).await;
    let n = send_command_to_master(&mut stream, &psync_init, &mut request_buffer).await;
    NODE.write().unwrap().master.as_mut().map(|m| m.offset += 1);
    println!("listening to master for commands");
    let mut request = &request_buffer[..n];
    loop {
        loop {
            let (tx, rx) = SignalSender::new();
            if request.len() == 0 {
                break;
            }
            let (response, n) = handle_input(request, tx);
            let signals = rx.try_recv();
            if signals.count_toward_offset {
                let mut node = NODE.write().unwrap();
                let replica = node.master.as_mut();
                replica.map(|m| m.offset += n as i64);
            }
            if signals.send_to_master {
                match std::str::from_utf8(&response) {
                    Ok(value) => {
                        println!("reply to master: {}", value);
                    }
                    Err(_) => {
                        println!("reply to master: {:?}", &response);
                    }
                }
                if let Err(e) = stream.write_all(&response).await {
                    eprintln!("Error writing {:?}", e);
                }
            }
            stream.flush().await.unwrap();
            request = &request[n..];
        }

        loop {
            let n = stream.read(&mut request_buffer).await.unwrap();
            if n != 0 {
                request = &request_buffer[..n];
                break;
            }
        }
        println!(
            "input from master: {}",
            String::from_utf8_lossy(&request_buffer[..n])
        );
    }
}
