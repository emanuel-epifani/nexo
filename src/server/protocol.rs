//! RESP (REdis Serialization Protocol) Parser and Encoder

use std::str;

// ========================================
// TYPES
// ========================================

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    Null,
}

#[derive(Debug, Clone)]
pub enum Response {
    Ok,
    Integer(i64),
    BulkString(Vec<u8>),
    Null,
    Error(String),
}

// ========================================
// PARSER
// ========================================

pub fn parse_resp(buf: &[u8]) -> Result<(RespValue, usize), String> {
    if buf.is_empty() {
        return Err("Empty buffer".to_string());
    }

    match buf[0] {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        _ => Err(format!("Invalid RESP type: {}", buf[0] as char)),
    }
}

fn parse_simple_string(buf: &[u8]) -> Result<(RespValue, usize), String> {
    let end = find_crlf(buf, 1)?;
    let s = str::from_utf8(&buf[1..end])
        .map_err(|e| format!("Invalid UTF-8: {}", e))?
        .to_string();
    Ok((RespValue::SimpleString(s), end + 2))
}

fn parse_error(buf: &[u8]) -> Result<(RespValue, usize), String> {
    let end = find_crlf(buf, 1)?;
    let s = str::from_utf8(&buf[1..end])
        .map_err(|e| format!("Invalid UTF-8: {}", e))?
        .to_string();
    Ok((RespValue::Error(s), end + 2))
}

fn parse_integer(buf: &[u8]) -> Result<(RespValue, usize), String> {
    let end = find_crlf(buf, 1)?;
    let s = str::from_utf8(&buf[1..end])
        .map_err(|e| format!("Invalid UTF-8: {}", e))?;
    let n = s
        .parse::<i64>()
        .map_err(|e| format!("Invalid integer: {}", e))?;
    Ok((RespValue::Integer(n), end + 2))
}

fn parse_bulk_string(buf: &[u8]) -> Result<(RespValue, usize), String> {
    let end = find_crlf(buf, 1)?;
    let len_str = str::from_utf8(&buf[1..end])
        .map_err(|e| format!("Invalid UTF-8: {}", e))?;
    let len = len_str
        .parse::<i64>()
        .map_err(|e| format!("Invalid bulk string length: {}", e))?;

    if len == -1 {
        return Ok((RespValue::Null, end + 2));
    }

    let len = len as usize;
    let start = end + 2;
    let data_end = start + len;

    if buf.len() < data_end + 2 {
        return Err("Incomplete bulk string".to_string());
    }

    if &buf[data_end..data_end + 2] != b"\r\n" {
        return Err("Missing CRLF after bulk string data".to_string());
    }

    let data = buf[start..data_end].to_vec();
    Ok((RespValue::BulkString(data), data_end + 2))
}

fn parse_array(buf: &[u8]) -> Result<(RespValue, usize), String> {
    let end = find_crlf(buf, 1)?;
    let len_str = str::from_utf8(&buf[1..end])
        .map_err(|e| format!("Invalid UTF-8: {}", e))?;
    let len = len_str
        .parse::<usize>()
        .map_err(|e| format!("Invalid array length: {}", e))?;

    let mut elements = Vec::with_capacity(len);
    let mut pos = end + 2;

    for _ in 0..len {
        let (value, consumed) = parse_resp(&buf[pos..])?;
        elements.push(value);
        pos += consumed;
    }

    Ok((RespValue::Array(elements), pos))
}

fn find_crlf(buf: &[u8], start: usize) -> Result<usize, String> {
    for i in start..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Ok(i);
        }
    }
    Err("CRLF not found".to_string())
}

// ========================================
// ENCODER
// ========================================

pub fn encode_resp(response: &Response) -> Vec<u8> {
    match response {
        Response::Ok => b"+OK\r\n".to_vec(),

        Response::Integer(n) => format!(":{}\r\n", n).into_bytes(),

        Response::BulkString(data) => {
            let mut result = format!("${}\r\n", data.len()).into_bytes();
            result.extend_from_slice(data);
            result.extend_from_slice(b"\r\n");
            result
        }

        Response::Null => b"$-1\r\n".to_vec(),

        Response::Error(msg) => format!("-ERR {}\r\n", msg).into_bytes(),
    }
}

// ========================================
// HELPER: Convert RespValue to String Array
// ========================================

impl RespValue {
    pub fn into_string_array(self) -> Result<Vec<String>, String> {
        match self {
            RespValue::Array(elements) => {
                let mut result = Vec::with_capacity(elements.len());
                for elem in elements {
                    match elem {
                        RespValue::BulkString(data) => {
                            let s = String::from_utf8(data)
                                .map_err(|e| format!("Invalid UTF-8 in bulk string: {}", e))?;
                            result.push(s);
                        }
                        RespValue::SimpleString(s) => {
                            result.push(s);
                        }
                        _ => return Err(format!("Expected string in array, got {:?}", elem)),
                    }
                }
                Ok(result)
            }
            _ => Err(format!("Expected array, got {:?}", self)),
        }
    }
}
