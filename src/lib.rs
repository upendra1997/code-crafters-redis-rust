use crate::resp::{Resp, SerDe};
use lazy_static::lazy_static;
use std::sync::mpsc::SyncSender;
use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque},
    str,
    sync::RwLock,
    time::Duration,
};
use tokio::time::Instant;

pub mod resp;

#[derive(Clone)]
pub struct Node {
    pub host: String,
    pub port: usize,
    pub replicatio_id: String,
    pub offset: i64,
}

#[derive(Clone)]
pub struct State {
    pub master: Option<Node>,
    pub port: usize,
    pub replicas: Vec<SyncSender<Vec<u8>>>,
}

const EMPTY_RDB: &[u8; 88] = include_bytes!("resources/empty.rdb");

lazy_static! {
    pub static ref STORE: RwLock<HashMap<Vec<u8>, Vec<u8>>> = RwLock::new(HashMap::new());
    pub static ref EXPIRY: RwLock<BinaryHeap<(Reverse<Instant>, Vec<u8>)>> =
        RwLock::new(BinaryHeap::new());
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
            replicas: vec![],
        })
    };
}

fn make_error(str: &str) -> (Vec<u8>, bool) {
    (SerDe::serialize(Resp::Error(Cow::Borrowed(str))), false)
}

fn handle_command(
    command: impl AsRef<[u8]>,
    mut arguments: VecDeque<Resp>,
    sender: SyncSender<()>,
) -> (Vec<u8>, bool) {
    let is_master = NODE.read().unwrap().master.is_none();
    let command = command.as_ref();
    let command = str::from_utf8(command);
    if let Err(_e) = command {
        return make_error("command is not valid utf8");
    }
    let command = command.unwrap().to_uppercase();
    let mut send_to_replicas = false;
    let mut send_to_master = false;
    let result = match command.as_ref() {
        "PING" => SerDe::serialize(Resp::String("PONG".into())),
        "REPLCONF" => {
            if is_master {
                SerDe::serialize(Resp::String("OK".into()))
            } else {
                send_to_master = true;
                SerDe::serialize(Resp::Array(
                    [
                        "replconf".as_bytes().into(),
                        "ack".as_bytes().into(),
                        "0".as_bytes().into(),
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
            sender.send(()).unwrap();
            [
                SerDe::serialize(Resp::String(
                    format!("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0").into(),
                )),
                SerDe::serialize(Resp::File(Cow::Borrowed(EMPTY_RDB))),
            ]
            .concat()
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
        "SET" => {
            send_to_replicas = true;
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
            eprintln!("invalid command {:?}", command);
            SerDe::serialize(Resp::Error(Cow::Owned(format!(
                "invalid command {:?}",
                command
            ))))
        }
    };
    if is_master {
        (result, send_to_replicas)
    } else {
        (result, send_to_master)
    }
}

pub fn handle_input(request_buffer: &[u8], sender: SyncSender<()>) -> (Vec<u8>, bool) {
    let (input, _) = SerDe::deserialize(request_buffer);
    match input {
        Resp::Array(vec) => {
            let mut arguments = VecDeque::from(vec);
            let command = arguments.pop_front();
            if command.is_none() {
                return make_error("no command provided");
            }
            let command = command.unwrap();
            if let Resp::Binary(command) = &command {
                handle_command(command, arguments, sender)
            } else {
                (vec![], false)
            }
        }
        Resp::Binary(_) => todo!(),
        Resp::Error(_) => todo!(),
        Resp::Integer(_) => todo!(),
        Resp::String(_) => todo!(),
        Resp::Null => todo!(),
        Resp::File(_data) => {
            // let (tx, rx) = mpsc::sync_channel(1);
            // let result = handle_input(&data, tx);
            // let _ = rx.try_recv();
            // result
            (vec![], false)
        }
    }
}
