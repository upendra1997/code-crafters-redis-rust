use crate::resp::{Resp, SerDe};
use lazy_static::lazy_static;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque},
    str,
    sync::RwLock,
    time::Duration,
};
use tokio::time::Instant;
use tracing::{debug, error, info};

mod rdb;
pub mod resp;

#[derive(Clone)]
pub struct Node {
    pub host: String,
    pub port: usize,
    pub replicatio_id: String,
    pub offset: i64,
}

pub struct State {
    pub master: Option<Node>,
    pub port: usize,
    pub replicas: AtomicUsize,
    pub data_sender: Option<SyncSender<TcpStreamMessage>>,
}

// TODO: create a struct of all the senders, like new_node, send to masetr, send to replica, count
// toward offset and so on.
pub struct SignalSender {
    pub new_node: SyncSender<()>,
    pub send_to_master: SyncSender<()>,
    pub send_to_replica: SyncSender<()>,
    pub count_toward_offset: SyncSender<()>,
}

#[derive(Default)]
pub struct Signal {
    pub new_node: bool,
    pub send_to_master: bool,
    pub send_to_replica: bool,
    pub count_toward_offset: bool,
}

pub struct SignalReceiver {
    pub new_node_reciver: Receiver<()>,
    pub send_to_master_recever: Receiver<()>,
    pub send_to_replica_recever: Receiver<()>,
    pub count_toward_offset_recever: Receiver<()>,
}

impl SignalReceiver {
    pub fn try_recv(&self) -> Signal {
        Signal {
            new_node: self.new_node_reciver.try_recv().is_ok(),
            send_to_master: self.send_to_master_recever.try_recv().is_ok(),
            send_to_replica: self.send_to_replica_recever.try_recv().is_ok(),
            count_toward_offset: self.count_toward_offset_recever.try_recv().is_ok(),
        }
    }
}

impl SignalSender {
    pub fn new() -> (SignalSender, SignalReceiver) {
        let (tx, rx) = mpsc::sync_channel(1);
        let (tx1, rx1) = mpsc::sync_channel(1);
        let (tx2, rx2) = mpsc::sync_channel(1);
        let (tx3, rx3) = mpsc::sync_channel(1);
        let sender = SignalSender {
            new_node: tx,
            send_to_master: tx1,
            send_to_replica: tx2,
            count_toward_offset: tx3,
        };
        let receiver = SignalReceiver {
            new_node_reciver: rx,
            send_to_master_recever: rx1,
            send_to_replica_recever: rx2,
            count_toward_offset_recever: rx3,
        };
        (sender, receiver)
    }
}

const EMPTY_RDB: &[u8; 88] = include_bytes!("resources/empty.rdb");

pub enum TcpStreamMessage {
    CountAcks(()),
    Data(Vec<u8>),
}

lazy_static! {
    pub static ref STORE: RwLock<HashMap<Vec<u8>, Vec<u8>>> = RwLock::new(HashMap::new());
    pub static ref EXPIRY: RwLock<BinaryHeap<(Reverse<Instant>, Vec<u8>)>> =
        RwLock::new(BinaryHeap::new());
    pub static ref NEW_NODE_NOTIFIER: Arc<(Mutex<()>, Condvar)> =
        Arc::new((Mutex::new(()), Condvar::new()));
    pub static ref NODE: RwLock<State> = {
        let mut port = 6379;
        if std::env::args().len() > 1 && std::env::args().into_iter().nth(1).unwrap() == "--port" {
            port = std::env::args().nth(2).unwrap().parse().unwrap();
        }
        let master = match std::env::args()
            .into_iter()
            .position(|arg| arg.eq("--replicaof"))
        {
            Some(i) => Some(Node {
                host: std::env::args().nth(i + 1).unwrap(),
                port: std::env::args().nth(i + 2).unwrap().parse().unwrap(),
                replicatio_id: "?".to_owned(),
                offset: -1,
            }),
            None => None,
        };

        RwLock::new(State {
            master: master,
            port: port,
            replicas: AtomicUsize::new(0),
            data_sender: None,
        })
    };
}

fn make_error(str: &str) -> Vec<u8> {
    SerDe::serialize(Resp::Error(Cow::Borrowed(str)))
}

fn handle_command(
    command: impl AsRef<[u8]>,
    mut arguments: VecDeque<Resp>,
    signal: SignalSender,
) -> Vec<u8> {
    let replica = &NODE.read().unwrap().master;
    let is_master = replica.is_none();
    let command = command.as_ref();
    let command = str::from_utf8(command);
    if let Err(_e) = command {
        return make_error("command is not valid utf8");
    }
    let command = command.unwrap().to_uppercase();
    let result = match command.as_ref() {
        "PING" => {
            signal.count_toward_offset.send(()).unwrap();
            SerDe::serialize(Resp::String("PONG".into()))
        }
        "REPLCONF" => {
            if is_master {
                SerDe::serialize(Resp::String("OK".into()))
            } else {
                signal.send_to_master.send(()).unwrap();
                signal.count_toward_offset.send(()).unwrap();
                SerDe::serialize(Resp::Array(
                    [
                        "REPLCONF".as_bytes().into(),
                        "ACK".as_bytes().into(),
                        format!("{}", replica.as_ref().unwrap().offset)
                            .as_bytes()
                            .into(),
                    ]
                    .into(),
                ))
            }
        }
        "COMMAND" => {
            let commands = vec![
                "PING".as_bytes().into(),
                vec!["ping responds with pong".as_bytes().into()].into(),
                "ECHO".as_bytes().into(),
                vec!["echo <msg>".as_bytes().into()].into(),
                "SET".as_bytes().into(),
                vec!["set <key> <value> [px <expiry-in-ms>]".as_bytes().into()].into(),
                "GET".as_bytes().into(),
                vec!["get <key>".as_bytes().into()].into(),
            ];
            SerDe::serialize(Into::<Resp>::into(commands))
        }
        "PSYNC" => {
            let _repl_id = arguments.pop_front();
            let _offest = arguments.pop_front();
            signal.new_node.send(()).unwrap();
            let mut res = [
                SerDe::serialize(Resp::String(
                    format!("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0").into(),
                )),
                SerDe::serialize(Resp::Binary(Cow::Borrowed(EMPTY_RDB))),
            ]
            .concat();
            // remove \r\n from the binary data.
            res.pop().unwrap();
            res.pop().unwrap();
            res
        }
        "INFO" => {
            let commands = match NODE.read().unwrap().master {
                None => "role:master\n",
                Some(_) => "role:slave\n",
            };
            let first = arguments.pop_front();
            match first {
                Some(Resp::Binary(first)) => {
                    let first = first.to_ascii_uppercase();
                    if first == b"REPLICATION" {
                        let replicaton_info =  "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\n";
                        SerDe::serialize(Into::<Resp>::into(
                            [commands, replicaton_info].concat().as_bytes(),
                        ))
                    } else {
                        return make_error("info only supports replication as argumnt");
                    }
                }
                None => SerDe::serialize(Into::<Resp>::into(commands.as_bytes())),
                _ => return make_error("protocol error"),
            }
        }
        "ECHO" => {
            let first = arguments.pop_front();
            if first.is_none() {
                return make_error("echo must be provided with a <msg>");
            } else {
                let first = first.unwrap();
                SerDe::serialize(first)
            }
        }
        "WAIT" => {
            if let Resp::Binary(n) = arguments.pop_front().unwrap() {
                let n = str::from_utf8(n.as_ref())
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                if let Resp::Binary(millisecond) = arguments.pop_front().unwrap() {
                    if n == 0 {
                    } else {
                        let millis = str::from_utf8(millisecond.as_ref())
                            .unwrap()
                            .parse::<u64>()
                            .unwrap();
                        // let lock = NODE.write().unwrap();
                        // let _old_replicas = lock.replicas.swap(0, Ordering::SeqCst);
                        // debug!("Number of the replicas: {}", _old_replicas);
                        // drop(lock);
                        NODE.read()
                            .unwrap()
                            .data_sender
                            .as_ref()
                            .map(|sender| sender.send(TcpStreamMessage::CountAcks(())));
                        let (mutex, cvar) = &*NEW_NODE_NOTIFIER.clone();
                        let _lock = mutex.lock().unwrap();
                        let _ = cvar
                            .wait_timeout_while(_lock, Duration::from_millis(millis), |_| {
                                NODE.read().unwrap().replicas.load(Ordering::SeqCst) <= n
                            })
                            .unwrap();
                        drop(mutex);
                    }
                }
            }
            SerDe::serialize(Resp::Integer(
                NODE.read().unwrap().replicas.load(Ordering::SeqCst) as i64,
            ))
        }
        "SET" => {
            signal.send_to_replica.send(()).unwrap();
            signal.count_toward_offset.send(()).unwrap();
            let first = arguments.pop_front();
            if first.is_none() {
                return make_error("SET must be provided with a <key>");
            }
            let first = first.unwrap();
            let second = arguments.pop_front();
            if second.is_none() {
                return make_error("SET must be provided with a <value>");
            }
            let second = second.unwrap();
            let key: Vec<u8> = first.into();
            let value = second.into();
            STORE.write().unwrap().insert(key.clone(), value);
            let third = arguments.pop_front();
            if let Some(Resp::Binary(px)) = third {
                if px.to_ascii_uppercase() != b"PX" {
                    return make_error("error `SET <key> <value> PX <EXPIRY>` expected `PX`");
                }
                let fourth = arguments.pop_front();
                if fourth.is_none() {
                    return make_error("SET must be provided with a <EXPIRY> time");
                }
                let time = fourth.unwrap();
                if let Resp::Binary(time) = time {
                    if let Ok(Ok(time)) =
                        str::from_utf8(time.as_ref()).map(|time| time.parse::<u64>())
                    {
                        let expiry = Instant::now()
                            .checked_add(Duration::from_millis(time))
                            .unwrap();
                        EXPIRY.write().unwrap().push((Reverse(expiry), key));
                    } else {
                        return make_error("expiry time is not in range");
                    }
                }
            }
            SerDe::serialize(Resp::String(Cow::Borrowed("OK")))
        }
        "GET" => {
            let first = arguments.pop_front();
            if first.is_none() {
                return make_error("GET must be provided with a <key>");
            }
            let key = first.unwrap().into();
            let mut store = STORE.write().unwrap();
            {
                let mut expiry = EXPIRY.write().unwrap();
                while let Some(expire_key) = expiry.peek().map(|(time, _)| time.0 <= Instant::now())
                {
                    if expire_key {
                        let (_, key) = expiry.pop().unwrap();
                        store.remove(&key);
                    } else {
                        break;
                    }
                }
            }
            let result = store.get::<Vec<u8>>(&key).cloned();
            if let Some(result) = result {
                result
            } else {
                SerDe::serialize(Resp::Null)
            }
        }
        _ => {
            error!("invalid command {:?}", command);
            SerDe::serialize(Resp::Error(Cow::Owned(format!(
                "invalid command {:?}",
                command
            ))))
        }
    };
    result
}

pub fn handle_input(request_buffer: &[u8], signals: SignalSender) -> (Vec<u8>, usize) {
    let (input, n) = SerDe::deserialize(request_buffer);
    info!(
        "handling input <<{}>>",
        String::from_utf8_lossy(&request_buffer[..n])
    );
    let result = match input {
        Resp::Array(vec) => {
            let mut arguments = VecDeque::from(vec);
            let command = arguments.pop_front();
            if command.is_none() {
                make_error("no command provided");
            }
            let command = command.unwrap();
            if let Resp::Binary(command) = &command {
                handle_command(command, arguments, signals)
            } else {
                vec![]
            }
        }
        Resp::Binary(data) => {
            let rdb_file = rdb::Rdb::from(data.as_ref());
            debug!("proccessed rdb file data: {:?}", rdb_file.store);
            let mut store = STORE.write().unwrap();
            for (k, v) in rdb_file.store {
                store.insert(k, v);
            }
            vec![]
        }
        Resp::Error(_) => todo!(),
        Resp::Integer(_) => todo!(),
        Resp::String(_) => {
            info!(
                "ignoring string command: {}",
                String::from_utf8_lossy(&request_buffer[..n])
            );
            vec![]
        }
        Resp::Null => todo!(),
        Resp::Ignore(i) => vec![i],
    };
    (result, n)
}
