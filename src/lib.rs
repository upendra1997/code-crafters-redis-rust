mod RESP;

use RESP::{SerDe, RESP as resp};

pub fn handle_input(request_buffer: &[u8]) -> Vec<u8> {
    let (input, _) = resp::deserialize(request_buffer);
    match input {
        resp::Array(vec) => {
            if let resp::Binary(command) = &vec[0] {
                let command = std::str::from_utf8(command).unwrap().to_uppercase();
                match &command[..] {
                    "PING" => resp::serialize(resp::String("PONG".into())),
                    "COMMAND" => resp::serialize(resp::Array(vec![resp::Binary("PING".as_bytes().into())])),
                    _ => {
                        eprintln!("invalid command {:?}", command);
                        vec![]
                    }
                }
            } else {
                vec![]
            }
        }
        resp::Binary(_) => todo!(),
        resp::Error(_) => todo!(),
        resp::Integer(_) => todo!(),
        resp::String(_) => todo!(),
        resp::Null => todo!(),
    }
}
