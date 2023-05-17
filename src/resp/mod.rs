use std::borrow::Cow;

pub trait SerDe {
    type Input;
    type Output;
    fn deserialize(input: Self::Input) -> (Self, usize)
    where
        Self: Sized;

    fn serialize(input: Self) -> Self::Output
    where
        Self: Sized;
}

pub enum Resp<'a> {
    String(Cow<'a, str>),
    Binary(Cow<'a, [u8]>),
    Error(Cow<'a, str>),
    Array(Vec<Resp<'a>>),
    Integer(i64),
    Null,
}

fn parse_simple_string(request_buffer: &[u8]) -> (Resp, usize) {
    let pos = request_buffer
        .windows(2)
        .position(|arr| arr[0] == b'\r' && arr[1] == b'\n');
    match pos {
        Some(pos) => match std::str::from_utf8(&request_buffer[..pos]) {
            Ok(str) => (Resp::String(Cow::Borrowed(str)), pos + 2),
            Err(_e) => panic!("Error parsing RESP simple string - invalid utf8"),
        },
        None => panic!("invalid RESP simple string"),
    }
}

fn parse_error(request_buffer: &[u8]) -> (Resp, usize) {
    let pos = request_buffer
        .windows(2)
        .position(|arr| arr[0] == b'\r' && arr[1] == b'\n');
    match pos {
        Some(pos) => match std::str::from_utf8(&request_buffer[..pos]) {
            Ok(str) => (Resp::Error(Cow::Borrowed(str)), pos + 2),
            Err(_e) => panic!("Error parsing RESP error - invalid utf8"),
        },
        None => panic!("invalid RESP error"),
    }
}

fn parse_integer(request_buffer: &[u8]) -> (Resp, usize) {
    let pos = request_buffer
        .windows(2)
        .position(|arr| arr[0] == b'\r' && arr[1] == b'\n');
    match pos {
        Some(pos) => match std::str::from_utf8(&request_buffer[..pos]) {
            Ok(str) => match str.parse::<i64>() {
                Ok(n) => (Resp::Integer(n), pos + 2),
                Err(_e) => panic!("Error parsing RESP integers"),
            },
            Err(_e) => panic!("Error parsing RESP integers - invalid utf8"),
        },
        None => panic!("invalid RESP error"),
    }
}

fn parse_bulk_string(request_buffer: &[u8]) -> (Resp, usize) {
    let pos = request_buffer
        .windows(2)
        .position(|arr| arr[0] == b'\r' && arr[1] == b'\n');
    let number = pos
        .and_then(|pos| std::str::from_utf8(&request_buffer[..pos]).ok())
        .and_then(|res| res.parse::<isize>().ok());
    match (number, pos) {
        (Some(-1), Some(pos)) => (Resp::Null, pos + 2),
        (Some(n), Some(p)) => (
            Resp::Binary(Cow::Borrowed(
                &request_buffer[(p + 2)..(p + 2 + n as usize)],
            )),
            p + 4 + n as usize,
        ),
        _ => panic!("invalid RESP bulk string"),
    }
}

fn parse_array(request_buffer: &[u8]) -> (Resp, usize) {
    let pos = request_buffer
        .windows(2)
        .position(|arr| arr[0] == b'\r' && arr[1] == b'\n');
    let number = pos
        .and_then(|pos| std::str::from_utf8(&request_buffer[..pos]).ok())
        .and_then(|res| res.parse::<usize>().ok());
    if pos.is_none() {
        panic!("invalid RESP array")
    }
    if number.is_none() {
        panic!("invalid RESP array")
    }
    let pos = pos.unwrap();
    let number = number.unwrap();
    let mut array = Vec::with_capacity(number);
    let mut index = pos + 2;
    for _i in 0..number {
        let (result, length) = Resp::deserialize(&request_buffer[index..]);
        array.push(result);
        index += length;
    }
    (Resp::Array(array), index)
}

fn parse_free_form(_request_buffer: &[u8]) -> (Resp, usize) {
    (Resp::Null, 0)
}

impl<'a> SerDe for Resp<'a> {
    type Input = &'a [u8];
    type Output = Vec<u8>;

    fn deserialize(input: Self::Input) -> (Self, usize)
    where
        Self: Sized,
    {
        let (result, length) = match input[0] {
            b'+' => parse_simple_string(&input[1..]),
            b'-' => parse_error(&input[1..]),
            b':' => parse_integer(&input[1..]),
            b'$' => parse_bulk_string(&input[1..]),
            b'*' => parse_array(&input[1..]),
            _ => {
                let (result, length) = parse_free_form(input);
                (result, length - 1)
            }
        };
        (result, length + 1)
    }

    fn serialize(input: Self) -> Self::Output
    where
        Self: Sized,
    {
        match input {
            Resp::String(string) => format!("+{}\r\n", string).as_bytes().into(),
            Resp::Binary(blob) => [
                format!("${}\r\n", blob.len()).as_bytes(),
                blob.as_ref(),
                "\r\n".as_bytes(),
            ]
            .concat(),
            Resp::Error(err) => format!("-{}\r\n", err).as_bytes().into(),
            Resp::Array(array) => [
                format!("*{}\r\n", array.len()).as_bytes(),
                &array
                    .into_iter()
                    .map(Self::serialize)
                    .collect::<Vec<Vec<u8>>>()
                    .concat(),
            ]
            .concat(),
            Resp::Integer(number) => format!(":{}\r\n", number).as_bytes().into(),
            Resp::Null => "$-1\r\n".to_string().as_bytes().into(),
        }
    }
}
