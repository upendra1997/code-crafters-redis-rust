use crate::resp::{Resp, SerDe};
use lazy_static::lazy_static;
use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque},
    str,
    sync::Mutex,
    time::Duration,
};
use tokio::time::Instant;

mod resp;

lazy_static! {
    static ref STORE: Mutex<HashMap<Vec<u8>, Vec<u8>>> = Mutex::new(HashMap::new());
    static ref EXPIRY: Mutex<BinaryHeap<(Reverse<Instant>, Vec<u8>)>> =
        Mutex::new(BinaryHeap::new());
}

struct Node {
    host: String,
    port: usize,
}

fn make_error(str: &str) -> Resp {
    Resp::Error(Cow::Borrowed(str))
}

fn handle_command(command: impl AsRef<[u8]>, mut arguments: VecDeque<Resp>) -> Vec<u8> {
    let command = command.as_ref();
    let command = str::from_utf8(command);
    if let Err(_e) = command {
        return SerDe::serialize(make_error("command is not valid utf8"));
    }
    let command = command.unwrap().to_uppercase();
    match command.as_ref() {
        "PING" => SerDe::serialize(Resp::String("PONG".into())),
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
        "INFO" => {
            let master = match std::env::args()
                .into_iter()
                .position(|arg| arg.eq("--replicaof"))
            {
                Some(i) => Some(Node {
                    host: std::env::args().nth(i + 1).unwrap(),
                    port: std::env::args().nth(i + 2).unwrap().parse().unwrap(),
                }),
                None => None,
            };

            let commands = match master {
                None => "role:master".as_bytes(),
                Some(_) => "role:slave".as_bytes(),
            };
            let first = arguments.pop_front();
            match first {
                Some(Resp::Binary(first)) => {
                    let first = first.to_ascii_uppercase();
                    if first == b"REPLICATION" {
                        SerDe::serialize(Into::<Resp>::into("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0".as_bytes()))
                    } else {
                        SerDe::serialize(make_error("info only supports replication as argumnt"))
                    }
                }
                None => SerDe::serialize(Into::<Resp>::into(commands)),
                _ => SerDe::serialize(make_error("protocol error")),
            }
        }
        "ECHO" => {
            let first = arguments.pop_front();
            if first.is_none() {
                return SerDe::serialize(make_error("echo must be provided with a <msg>"));
            }
            let first = first.unwrap();
            SerDe::serialize(first)
        }
        "SET" => {
            let first = arguments.pop_front();
            if first.is_none() {
                return SerDe::serialize(make_error("SET must be provided with a <key>"));
            }
            let first = first.unwrap();
            let second = arguments.pop_front();
            if second.is_none() {
                return SerDe::serialize(make_error("SET must be provided with a <value>"));
            }
            let second = second.unwrap();
            let key: Vec<u8> = first.into();
            let value = second.into();
            STORE.lock().unwrap().insert(key.clone(), value);
            let third = arguments.pop_front();
            if let Some(Resp::Binary(px)) = third {
                if px.to_ascii_uppercase() != b"PX" {
                    return SerDe::serialize(make_error(
                        "error `SET <key> <value> PX <EXPIRY>` expected `PX`",
                    ));
                }
                let fourth = arguments.pop_front();
                if fourth.is_none() {
                    return SerDe::serialize(make_error(
                        "SET must be provided with a <EXPIRY> time",
                    ));
                }
                let time = fourth.unwrap();
                if let Resp::Binary(time) = time {
                    if let Ok(Ok(time)) =
                        str::from_utf8(time.as_ref()).map(|time| time.parse::<u64>())
                    {
                        let expiry = Instant::now()
                            .checked_add(Duration::from_millis(time))
                            .unwrap();
                        EXPIRY.lock().unwrap().push((Reverse(expiry), key));
                    } else {
                        return SerDe::serialize(make_error("expiry time is not in range"));
                    }
                }
            }
            SerDe::serialize(Resp::String(Cow::Borrowed("OK")))
        }
        "GET" => {
            let first = arguments.pop_front();
            if first.is_none() {
                return SerDe::serialize(make_error("GET must be provided with a <key>"));
            }
            let key = first.unwrap().into();
            let mut store = STORE.lock().unwrap();
            {
                let mut expiry = EXPIRY.lock().unwrap();
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
    }
}

pub fn handle_input(request_buffer: &[u8]) -> Vec<u8> {
    let (input, _) = SerDe::deserialize(request_buffer);
    match input {
        Resp::Array(vec) => {
            let mut arguments = VecDeque::from(vec);
            let command = arguments.pop_front();
            if command.is_none() {
                return SerDe::serialize(make_error("no command provided"));
            }
            let command = command.unwrap();
            if let Resp::Binary(command) = &command {
                handle_command(command, arguments)
            } else {
                vec![]
            }
        }
        Resp::Binary(_) => todo!(),
        Resp::Error(_) => todo!(),
        Resp::Integer(_) => todo!(),
        Resp::String(_) => todo!(),
        Resp::Null => todo!(),
    }
}
