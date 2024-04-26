use redis_starter_rust::resp::{Resp, SerDe};
use redis_starter_rust::{handle_input, SignalReceiver, SignalSender, NEW_NODE_NOTIFIER, NODE};
use std::io::Write;
use std::net::TcpStream as StdTcpStream;
use std::ops::DerefMut;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
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

    let streams: Arc<SendableRwLock<Vec<(Arc<Mutex<TcpStream>>, usize)>>> =
        Arc::new(SendableRwLock::new(vec![]));
    let (sender, reciver): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::sync_channel(1024);
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
                if data.len() == 0 {
                    continue;
                }
                let mut streams = streamss.write().await;
                let mut useless_streams = vec![];
                command_buffer.push(data);
                for (i, (stream, offset)) in streams.iter_mut().enumerate() {
                    for data in &command_buffer[*offset..] {
                        let mut my_stream = stream.lock().await;
                        let result = my_stream.write_all(&data).await;
                        drop(my_stream);
                        if let Err(e) = result {
                            println!("removing replica from the master, because of {}", e);
                            //TODO: should have a hashmap
                            useless_streams.push(i);
                            break;
                        }
                        println!("sendig {} to replica {}", String::from_utf8_lossy(data), i);
                        *offset += 1;
                    }
                }
                for i in useless_streams {
                    streams.remove(i);
                }
            }
        });
    }
    loop {
        let sender = sender.clone();
        let streams = streams.clone();
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(async move { handle_connection(streams, stream, sender).await });
            }
            Err(e) => {
                eprintln!("error: {:?}", e);
            }
        }
    }
}

async fn handle_signals(
    rx: SignalReceiver,
    sender: SyncSender<Vec<u8>>,
    streams: Arc<SendableRwLock<Vec<(Arc<Mutex<TcpStream>>, usize)>>>,
    stream: Arc<Mutex<TcpStream>>,
) {
    let is_master = NODE.read().unwrap().master.is_none();
    let count_ack_reciever = rx.count_acks_recever;
    for s in count_ack_reciever {
        let replconf_get_ack = SerDe::serialize(Resp::Array(vec![
            Resp::Binary("REPLCONF".as_bytes().into()),
            Resp::Binary("GETACK".as_bytes().into()),
            Resp::Binary("*".as_bytes().into()),
        ]));
        sender.send(replconf_get_ack);
        //send values to th ecountt
        /*
        let mut node = NODE.write().unwrap();
        let replicas: &mut Vec<SyncSender<Vec<u8>>> = node.replicas.as_mut();
        println!("New replica {} is being added by {}", replicas.len(), req);
        replicas.push(sender);
        let (mutex, cvar) = &*NEW_NODE_NOTIFIER.clone();
        let mutex = mutex.lock().unwrap();
        cvar.notify_all();
        drop(mutex);
        */
    }
    // let signals = rx.try_recv();
    //send data to replicas before replying to client
    let req = rx.data_recevr.try_recv().unwrap_or(vec![]);
    if is_master && rx.send_to_replica_recever.try_recv().is_ok() {
        sender.send(req).unwrap();
    }
    if is_master && rx.new_node_reciver.try_recv().is_ok() {
        let mut streams = streams.write().await;
        streams.push((stream, 0));
    }
}

async fn handle_connection(
    mut streams: Arc<SendableRwLock<Vec<(Arc<Mutex<TcpStream>>, usize)>>>,
    mut stream: TcpStream,
    sender: SyncSender<Vec<u8>>,
) -> Option<TcpStream> {
    println!("accepted new connection");
    let mut request_buffer = vec![0u8; REQUEST_BUFFER_SIZE];
    let stream = Arc::new(Mutex::new(stream));
    loop {
        let mut my_stream = stream.lock().await;
        let res = my_stream.read(&mut request_buffer).await;
        drop(my_stream);
        match res {
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
                    tokio::spawn(handle_signals(
                        rx,
                        sender.clone(),
                        streams.clone(),
                        stream.clone(),
                    ));
                    let mut req_clone = vec![];
                    req_clone.extend_from_slice(request);
                    tx.data.send(req_clone);
                    let (response, n) = handle_input(request, tx.clone());
                    match std::str::from_utf8(&response) {
                        Ok(value) => {
                            println!("RES: {:?}", value);
                        }
                        Err(_) => {
                            println!("RES: {:?}", response);
                        }
                    }
                    let mut my_stream = stream.try_lock().unwrap();
                    if let Err(e) = my_stream.write_all(&response).await {
                        eprintln!("Error writing {:?}", e);
                    }
                    my_stream.flush().await.unwrap();
                    drop(my_stream);
                    request = &request[n..];
                }
            }
            Err(e) => {
                // https://forum.codecrafters.io/t/ruby-redis-replication-stage-18-error-reading-replica-acknowledgement/84/7
                eprintln!("error reading from tcp stream {}", e);
                break None;
            }
        }
    }
}

async fn send_command_to_replica(
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
        "reply from replica: {}",
        String::from_utf8_lossy(&request_buffer[..n])
    );
    n
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
