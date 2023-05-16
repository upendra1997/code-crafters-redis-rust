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

pub enum RESP<'a> {
    String(Cow<'a, str>),
    Binary(Cow<'a, [u8]>),
    Error(Cow<'a, str>),
    Array(Vec<RESP<'a>>),
    Integer(i64),
    Null,
}

fn parse_simple_string(request_buffer: &[u8]) -> (RESP, usize) {
    let pos = request_buffer
        .windows(2)
        .position(|arr| arr[0] == b'\r' && arr[1] == b'\n');
    match pos {
        Some(pos) => match std::str::from_utf8(&request_buffer[..pos]) {
            Ok(str) => (RESP::String(Cow::Borrowed(str)), pos + 2),
            Err(_e) => panic!("Error parsing RESP simple string - invalid utf8"),
        },
        None => panic!("invalid RESP simple string"),
    }
}

fn parse_error(request_buffer: &[u8]) -> (RESP, usize) {
    let pos = request_buffer
        .windows(2)
        .position(|arr| arr[0] == b'\r' && arr[1] == b'\n');
    match pos {
        Some(pos) => match std::str::from_utf8(&request_buffer[..pos]) {
            Ok(str) => (RESP::Error(Cow::Borrowed(str)), pos + 2),
            Err(_e) => panic!("Error parsing RESP error - invalid utf8"),
        },
        None => panic!("invalid RESP error"),
    }
}

fn parse_integer(request_buffer: &[u8]) -> (RESP, usize) {
    let pos = request_buffer
        .windows(2)
        .position(|arr| arr[0] == b'\r' && arr[1] == b'\n');
    match pos {
        Some(pos) => match std::str::from_utf8(&request_buffer[..pos]) {
            Ok(str) => match i64::from_str_radix(str, 10) {
                Ok(n) => (RESP::Integer(n), pos + 2),
                Err(_e) => panic!("Error parsing RESP integers"),
            },
            Err(_e) => panic!("Error parsing RESP integers - invalid utf8"),
        },
        None => panic!("invalid RESP error"),
    }
}

fn parse_bulk_string(request_buffer: &[u8]) -> (RESP, usize) {
    let pos = request_buffer
        .windows(2)
        .position(|arr| arr[0] == b'\r' && arr[1] == b'\n');
    let number = pos
        .and_then(|pos| std::str::from_utf8(&request_buffer[..pos]).ok())
        .and_then(|res| isize::from_str_radix(res, 10).ok());
    match (number, pos) {
        (Some(-1), Some(pos)) => (RESP::Null, pos + 2),
        (Some(n), Some(p)) => (
            RESP::Binary(Cow::Borrowed(
                &request_buffer[(p + 2)..(p + 2 + n as usize)],
            )),
            p + 4 + n as usize,
        ),
        _ => panic!("invalid RESP bulk string"),
    }
}
fn parse_array(request_buffer: &[u8]) -> (RESP, usize) {
    let pos = request_buffer
        .windows(2)
        .position(|arr| arr[0] == b'\r' && arr[1] == b'\n');
    let number = pos
        .and_then(|pos| std::str::from_utf8(&request_buffer[..pos]).ok())
        .and_then(|res| usize::from_str_radix(res, 10).ok());
    if let None = pos {
        panic!("invalid RESP array")
    }
    if let None = number {
        panic!("invalid RESP array")
    }
    let pos = pos.unwrap();
    let number = number.unwrap();
    let mut array = Vec::with_capacity(number);
    let mut index = pos + 2;
    for i in 0..number {
        let (result, length) = RESP::deserialize(&request_buffer[index..]);
        array.push(result);
        index += length;
    }
    (RESP::Array(array), index)
}

fn parse_free_form(_request_buffer: &[u8]) -> (RESP, usize) {
    (RESP::Null, 0)
}

impl<'a> SerDe for RESP<'a> {
    type Input = &'a [u8];
    type Output = Vec<u8>;

    fn deserialize(input: Self::Input) -> (Self, usize)
    where
        Self: Sized,
    {
        match input[0] {
            b'+' => {
                let (result, length) = parse_simple_string(&input[1..]);
                (result, length)
            }
            b'-' => {
                let (result, length) = parse_error(&input[1..]);
                (result, length)
            }
            b':' => {
                let (result, length) = parse_integer(&input[1..]);
                (result, length)
            }
            b'$' => {
                let (result, length) = parse_bulk_string(&input[1..]);
                (result, length)
            }
            b'*' => {
                let (result, length) = parse_array(&input[1..]);
                (result, length)
            }
            _ => {
                let (result, length) = parse_free_form(&input);
                (result, length)
            }
        }
    }

    fn serialize(input: Self) -> Self::Output
    where
        Self: Sized,
    {
        match input {
            RESP::String(string) => format!("+{}\r\n", string).as_bytes().into(),
            RESP::Binary(blob) => [
                format!("${}\r\n", blob.len()).as_bytes(),
                blob.as_ref(),
                "\r\n".as_bytes(),
            ]
            .concat()
            .into(),
            RESP::Error(err) => format!("-{}\r\n", err).as_bytes().into(),
            RESP::Array(array) => [
                format!("*{}\r\n", array.len()).as_bytes(),
                &array
                    .into_iter()
                    .map(|elem| Self::serialize(elem))
                    .collect::<Vec<Vec<u8>>>()
                    .concat(),
            ]
            .concat()
            .into(),
            RESP::Integer(number) => format!(":{}\r\n", number).as_bytes().into(),
            RESP::Null => format!("$-1\r\n").as_bytes().into(),
        }
    }
}
