mod resp;

pub fn handle_input(request_buffer: &[u8]) -> Vec<u8> {
    let (input, _) = resp::SerDe::deserialize(request_buffer);
    match input {
        resp::Resp::Array(vec) => {
            if let resp::Resp::Binary(command) = &vec[0] {
                let command = std::str::from_utf8(command).unwrap().to_uppercase();
                match &command[..] {
                    "PING" => resp::SerDe::serialize(resp::Resp::String("PONG".into())),
                    "COMMAND" => {
                        resp::SerDe::serialize(resp::Resp::Array(vec![resp::Resp::Binary("PING".as_bytes().into())]))
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
