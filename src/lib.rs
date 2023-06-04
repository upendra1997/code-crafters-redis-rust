use crate::resp::{Resp, SerDe};
use lazy_static::lazy_static;
use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    sync::Mutex,
};

mod resp;

lazy_static! {
    static ref STORE: Mutex<HashMap<Vec<u8>, Vec<u8>>> = Mutex::new(HashMap::new());
}

fn make_error(str: &str) -> Resp {
    Resp::Error(Cow::Borrowed(str))
}

fn handle_command(command: impl AsRef<[u8]>, mut arguments: VecDeque<Resp>) -> Vec<u8> {
    let command = command.as_ref();
    let command = std::str::from_utf8(command);
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
                vec!["set <key> <value>".as_bytes().into()].into(),
                "GET".as_bytes().into(),
                vec!["get <key>".as_bytes().into()].into(),
            ];
            SerDe::serialize(Into::<Resp>::into(commands))
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
            STORE.lock().unwrap().insert(first.into(), second.into());
            SerDe::serialize(Resp::String(Cow::Borrowed("OK")))
        }
        "GET" => {
            let first = arguments.pop_front();
            if first.is_none() {
                return SerDe::serialize(make_error("GET must be provided with a <key>"));
            }
            let first = first.unwrap();
            let result = STORE.lock().unwrap().get::<Vec<u8>>(&first.into()).cloned();
            if let Some(result) = result {
                result
            } else {
                SerDe::serialize(Resp::Error(Cow::Borrowed("Key not found")))
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
