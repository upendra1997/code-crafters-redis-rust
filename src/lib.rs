use std::collections::VecDeque;

mod resp;

pub fn handle_input(request_buffer: &[u8]) -> Vec<u8> {
    let (input, _) = resp::SerDe::deserialize(request_buffer);
    match input {
        resp::Resp::Array(vec) => {
            let mut arguments = VecDeque::from(vec);
            let command = arguments.pop_front().unwrap();
            if let resp::Resp::Binary(command) = &command {
                let command = std::str::from_utf8(command).unwrap().to_uppercase();
                match command.as_ref() {
                    "PING" => resp::SerDe::serialize(resp::Resp::String("PONG".into())),
                    "COMMAND" => resp::SerDe::serialize(resp::Resp::Array(vec![
                        resp::Resp::String("PING".into()),
                        resp::Resp::String("ECHO".into()),
                    ])),
                    "ECHO" => {
                        let first = arguments.pop_front().unwrap();
                        resp::SerDe::serialize(first)
                    }
                    _ => {
                        eprintln!("invalid command {:?}", command);
                        vec![]
                    }
                }
            } else {
                vec![]
            }
        }
        resp::Resp::Binary(_) => todo!(),
        resp::Resp::Error(_) => todo!(),
        resp::Resp::Integer(_) => todo!(),
        resp::Resp::String(_) => todo!(),
        resp::Resp::Null => todo!(),
    }
}
