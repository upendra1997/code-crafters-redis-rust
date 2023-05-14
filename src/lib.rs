use resp::{SerDe, RESP};

pub fn handle_input(request_buffer: &[u8]) -> Vec<u8> {
    let (input, _) = RESP::deserialize(request_buffer);
    match input {
        RESP::Array(vec) => {
            if let RESP::Binary(command) = &vec[0] {
                if command.starts_with(b"PING") {
                    b"+PONG\r\n".to_vec()
                } else {
                    eprintln!("{:?}", std::str::from_utf8(command).unwrap());
                    vec![]
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
