use resp::{SerDe, RESP};

pub fn handle_input(request_buffer: &[u8]) -> Vec<u8> {
    let (input, _) = RESP::deserialize(request_buffer);
    match input {
        RESP::Array(vec) => {
            if let RESP::Binary(command) = &vec[0] {
                let command = std::str::from_utf8(command).unwrap().to_uppercase();
                match &command[..] {
                    "PING" => b"+PONG\r\n".to_vec(),
                    "COMMAND" => b"+PONG\r\n".to_vec(),
                    _ => {
                        eprintln!("invalid command {:?}", command);
                        vec![]
                    }
                }
            } else {
                vec![]
            }
        }
        RESP::Binary(_) => todo!(),
        RESP::Error(_) => todo!(),
        RESP::Integer(_) => todo!(),
        RESP::String(_) => todo!(),
        RESP::Null => todo!(),
    }
}

mod resp;
