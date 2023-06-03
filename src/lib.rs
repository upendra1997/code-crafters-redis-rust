use crate::resp::{Resp, SerDe};
use std::{borrow::Cow, collections::VecDeque};

mod resp;

pub fn handle_input(request_buffer: &[u8]) -> Vec<u8> {
    let (input, _) = SerDe::deserialize(request_buffer);
    match input {
        Resp::Array(vec) => {
            let mut arguments = VecDeque::from(vec);
            let command = arguments.pop_front().unwrap();
            if let Resp::Binary(command) = &command {
                let command = std::str::from_utf8(command).unwrap().to_uppercase();
                match command.as_ref() {
                    "PING" => SerDe::serialize(Resp::String("PONG".into())),
                    "COMMAND" => {
                        let commands = vec![
                            "PING".as_bytes().into(),
                            vec!["respond with pong".as_bytes().into()].into(),
                            "ECHO".as_bytes().into(),
                            vec!["response with some message".as_bytes().into()].into(),
                        ];
                        SerDe::serialize(Into::<Resp>::into(commands))
                    }
                    "ECHO" => {
                        let first = arguments.pop_front().unwrap();
                        SerDe::serialize(first)
                    }
                    _ => {
                        eprintln!("invalid command {:?}", command);
                        SerDe::serialize(Resp::Error(Cow::Owned(format!(
                            "invalid command {:?}",
                            command
                        ))))
                    }
                }
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
