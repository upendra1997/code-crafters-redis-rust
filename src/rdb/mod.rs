use std::{borrow::Cow, collections::HashMap};

use crate::resp::Resp;

#[derive(Debug, PartialEq, Eq)]
pub struct Rdb {
    pub store: HashMap<Vec<u8>, Vec<u8>>,
}

const MAGIC: [u8; 9] = [0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x33];

// 00000000: 5245 4449 5330 3031 31fa 0972 6564 6973  REDIS0011..redis
// 00000010: 2d76 6572 0537 2e32 2e34 fa0a 7265 6469  -ver.7.2.4..redi
// 00000020: 732d 6269 7473 c040 fa05 6374 696d 65c2  s-bits.@..ctime.
// 00000030: 9160 0c66 fa08 7573 6564 2d6d 656d c260  .`.f..used-mem.`
// 00000040: b10f 00fa 0861 6f66 2d62 6173 65c0 00fe  .....aof-base...
// 00000050: 00fb 0200 0003 6261 7203 6261 7a00 0366  ......bar.baz..f
// 00000060: 6f6f 0362 6172 ff3b 00de 8a4e b99d 31    oo.bar.;...N..1
//

#[derive(Debug, Hash, PartialEq, Eq)]
enum LengthEncodedValue {
    // String(String),
    I8(i8),
    I16(i16),
    I32(i32),
    USIZE(usize),
    ERROR,
}

impl From<LengthEncodedValue> for usize {
    fn from(value: LengthEncodedValue) -> Self {
        match value {
            LengthEncodedValue::I8(x) => x as usize,
            LengthEncodedValue::I16(x) => x as usize,
            LengthEncodedValue::I32(x) => x as usize,
            LengthEncodedValue::USIZE(x) => x,
            LengthEncodedValue::ERROR => 0,
        }
    }
}

enum ValueEncoded {
    String,
    ERROR,
}

impl From<u8> for ValueEncoded {
    fn from(value: u8) -> Self {
        if value == 0 {
            ValueEncoded::String
        } else {
            ValueEncoded::ERROR
        }
    }
}

fn string_encoding(data: &[u8]) -> (&[u8], usize) {
    let (length, size) = length_encoding(data);
    let length = usize::from(length);
    let (key, data) = data.split_at(size).1.split_at(length);
    (key, size + length)
    // (String::from_utf8(key.to_vec()).unwrap(), size + length)
}

fn length_encoding(data: &[u8]) -> (LengthEncodedValue, usize) {
    let (encoding, data) = data.split_at(1);
    let encoding = encoding.first().unwrap();
    let length_encoding_6_bits = 0b00111111;
    let length_encoding_14_bits = 0b01111111;
    let length_encoding_32_bits = 0b10111111;
    let i8_as_string = 0b11000000;
    let i16_as_string = 0b11000001;
    let i32_as_string = 0b11000010;
    let compressed_string = 0b11000011;
    if *encoding & length_encoding_6_bits == *encoding {
        let length = (encoding & length_encoding_6_bits) as usize;
        (LengthEncodedValue::USIZE(length), 1)
    } else if *encoding & length_encoding_14_bits == *encoding {
        let (other_length_byte, data) = data.split_first().unwrap();
        // (0 | first) << 8 | second
        let length =
            u16::from_be_bytes([(encoding & length_encoding_6_bits), *other_length_byte]) as usize;
        (LengthEncodedValue::USIZE(length), 1 + 1)
    } else if *encoding & length_encoding_32_bits == *encoding {
        let (other_length_bytes, data) = data.split_at(4);
        // println!("{:#08b}", encoding);
        // println!("{:#0x?}", other_length_bytes);
        // println!("{:08b}", data[0]);
        // println!("{:08b}", data[1]);
        // println!("{:08b}", data[2]);
        // println!("{:08b}", data[3]);
        let mut array = [0; 4];
        array.copy_from_slice(other_length_bytes);
        let length = u32::from_be_bytes(array) as usize;
        (LengthEncodedValue::USIZE(length), 1 + 4)
    } else if i8_as_string == *encoding {
        let (next_byte, data) = data.split_first().unwrap();
        (
            LengthEncodedValue::I8(i8::from_be_bytes([*next_byte])),
            1 + 1,
        )
    } else if i16_as_string == *encoding {
        let (bytes, data) = data.split_at(2);
        let mut array = [0; 2];
        array.copy_from_slice(bytes);
        (LengthEncodedValue::I16(i16::from_be_bytes(array)), 1 + 2)
    } else if i32_as_string == *encoding {
        let (bytes, data) = data.split_at(4);
        let mut array = [0; 4];
        array.copy_from_slice(bytes);
        (LengthEncodedValue::I32(i32::from_be_bytes(array)), 1 + 4)
    } else if *encoding & compressed_string == *encoding {
        todo!("handle compressed values")
    } else {
        (LengthEncodedValue::ERROR, 0)
    }
}
fn parse_auxiliary_fields(data: &[u8]) -> (HashMap<String, String>, usize) {
    let mut current_pos = 0;
    let mut data = data;
    let mut result = HashMap::new();
    while data.len() > 0 && data[0] == 0xfa {
        data = &data[1..];
        current_pos += 1;
        let (key, size) = string_encoding(data);
        data = &data[size..];
        current_pos += size;
        let (value, size) = string_encoding(data);
        data = &data[size..];
        current_pos += size;
        result.insert(
            String::from_utf8(key.into()).unwrap(),
            String::from_utf8(value.into()).unwrap(),
        );
    }
    println!("{:?}", result);
    (result, current_pos)
}

impl From<&[u8]> for Rdb {
    fn from(value: &[u8]) -> Self {
        // println!("{:#0x?}", value);
        let (magic_headers, mut data) = value.split_at(MAGIC.len());
        // println!("{:#0x?}", magic_headers);
        let auxiliary_fields_start = data.into_iter().position(|&x| x == 0xfa).unwrap();
        // println!("{:#0x?}", auxiliary_fields_start);
        data = data.split_at(auxiliary_fields_start).1;
        // TODO: move it into a loop where we have multiple dbs
        let data_base_selectior_frame_start = data.into_iter().position(|&x| x == 0xfe).unwrap();
        let (auxiliary_fields, mut data) = data.split_at(data_base_selectior_frame_start);
        // println!("{:#0x?}", auxiliary_fields);
        let (_, _) = parse_auxiliary_fields(auxiliary_fields);
        let (database_selecter, mut data) = data.split_first().unwrap();
        let (database_selected, mut data) = data.split_first().unwrap();
        println!("database selected: {:#0x?}", database_selected);
        let (resize_db, mut data) = data.split_first().unwrap();
        // println!("{:08b}", data[1]);
        // println!("{:08b}", data[2]);
        // println!("{:08b}", data[3]);
        let (size_of_hash_table, size) = length_encoding(data);
        println!("size of hash table: {:?}", size_of_hash_table);
        data = &data[size..];
        let (size_of_expired_hash_table, size) = length_encoding(data);
        println!(
            "size of expired hash table: {:?}",
            size_of_expired_hash_table
        );
        data = &data[size..];

        // scan keys
        // let key_value_pair_seconds_start = data.into_iter().position(|&x| x == 0xfd).unwrap();
        // let (_, data) = data.split_at(key_value_pair_seconds_start);
        // let (expiry_time_in_seconds, data) = data.split_at(4);
        // println!("{:#0x?}", expiry_time_in_seconds);
        // scan keys

        // scan keys
        // let key_value_pair_milliseconds_start = data.into_iter().position(|&x| x == 0xfd).unwrap();
        // let (_, data) = data.split_at(key_value_pair_milliseconds_start);
        // let (expiry_time_in_milliseconds, data) = data.split_at(4);
        // println!("{:#0x?}", expiry_time_in_milliseconds);
        // scan keys

        let mut store = HashMap::new();
        while data.len() > 0 {
            let (value_type, mut temp_data) = data.split_first().unwrap();
            match ValueEncoded::from(*value_type) {
                ValueEncoded::String => {
                    let (key, size) = string_encoding(temp_data);
                    println!("got key from rdb: {:?}", key);
                    let (_, temp_data1) = temp_data.split_at(size);
                    temp_data = temp_data1;
                    let (value, size) = string_encoding(temp_data);
                    store.insert(
                        Resp::Binary(Cow::Borrowed(key)).into(),
                        Resp::Binary(Cow::Borrowed(value)).into(),
                    );
                    println!("got value from rdb: {:?}", value);

                    let (_, temp_data1) = temp_data1.split_at(size);
                    temp_data = temp_data1;
                }
                ValueEncoded::ERROR => break,
            }
            data = temp_data;
        }
        if data[0] != 0xff {
            println!("{:#0x?}", data);
            panic!("unread data in rdb");
        }
        // let (key_value_pair_seconds, data) = data.split_at(key_value_pair_millis_start);
        // println!("{:#0x?}", key_value_pair_seconds);
        // let key_value_pair_seconds_start = data.into_iter().position(|&x| x == 0xfd).unwrap();
        // let (key_value_pair_seconds, data) = data.split_at(key_value_pair_seconds_start);
        // println!("{:#0x?}", key_value_pair_seconds);
        Rdb { store }
    }
}

/// implement a parser for https://rdb.fnordig.de/file_format.html
///
///
/// ----------------------------#
/// 52 45 44 49 53              # Magic String "REDIS"
/// 30 30 30 33                 # RDB Version Number as ASCII string. "0003" = 3
/// ----------------------------
/// FA                          # Auxiliary field
/// $string-encoded-key         # May contain arbitrary metadata
/// $string-encoded-value       # such as Redis version, creation time, used memory, ...
/// ----------------------------
/// FE 00                       # Indicates database selector. db number = 00
/// FB                          # Indicates a resizedb field
/// $length-encoded-int         # Size of the corresponding hash table
/// $length-encoded-int         # Size of the corresponding expire hash table
/// ----------------------------# Key-Value pair starts
/// FD $unsigned-int            # "expiry time in seconds", followed by 4 byte unsigned int
/// $value-type                 # 1 byte flag indicating the type of value
/// $string-encoded-key         # The key, encoded as a redis string
/// $encoded-value              # The value, encoding depends on $value-type
/// ----------------------------
/// FC $unsigned long           # "expiry time in ms", followed by 8 byte unsigned long
/// $value-type                 # 1 byte flag indicating the type of value
/// $string-encoded-key         # The key, encoded as a redis string
/// $encoded-value              # The value, encoding depends on $value-type
/// ----------------------------
/// $value-type                 # key-value pair without expiry
/// $string-encoded-key
/// $encoded-value
/// ----------------------------
/// FE $length-encoding         # Previous db ends, next db starts.
/// ----------------------------
/// ...                         # Additional key-value pairs, databases, ...
/// FF                          ## End of RDB file indicator
/// 8-byte-checksum             ## CRC64 checksum of the entire file.
///
///

#[cfg(test)]
mod tests {
    use crate::rdb::Rdb;

    #[test]
    fn it_works() {
        let rdb_data = include_bytes!("dump.rdb");
        let result = Rdb::from(rdb_data.as_slice());
        assert_eq!(2, 3);
        assert_eq!(result, result);
    }
}
