use crate::server::protocol::*;
use bytes::Bytes;
use std::convert::TryInto;

pub struct PayloadCursor {
    data: Bytes,
    offset: usize,
}

impl PayloadCursor {
    pub fn new(data: Bytes) -> Self {
        Self { data, offset: 0 }
    }

    pub fn has_remaining(&self, len: usize) -> bool {
        self.offset + len <= self.data.len()
    }

    pub fn read_u8(&mut self) -> Result<u8, String> {
        if !self.has_remaining(1) {
            return Err("Payload too short for u8".to_string());
        }
        let val = self.data[self.offset];
        self.offset += 1;
        Ok(val)
    }

    pub fn read_u32(&mut self) -> Result<u32, String> {
        if !self.has_remaining(4) {
            return Err("Payload too short for u32".to_string());
        }
        let val = u32::from_be_bytes(self.data[self.offset..self.offset + 4].try_into().unwrap());
        self.offset += 4;
        Ok(val)
    }

    pub fn read_u64(&mut self) -> Result<u64, String> {
        if !self.has_remaining(8) {
            return Err("Payload too short for u64".to_string());
        }
        let val = u64::from_be_bytes(self.data[self.offset..self.offset + 8].try_into().unwrap());
        self.offset += 8;
        Ok(val)
    }

    pub fn read_string(&mut self) -> Result<String, String> {
        let len = self.read_u32()? as usize;
        if !self.has_remaining(len) {
            return Err(format!("Incomplete string: expected {} bytes", len));
        }
        let s = std::str::from_utf8(&self.data[self.offset..self.offset + len])
            .map_err(|e| format!("Invalid UTF-8 in string: {}", e))?;
        self.offset += len;
        Ok(s.to_string())
    }

    pub fn read_remaining(&mut self) -> Bytes {
        let b = self.data.slice(self.offset..);
        self.offset = self.data.len();
        b
    }

    pub fn read_uuid_bytes(&mut self) -> Result<[u8; 16], String> {
        if !self.has_remaining(16) {
            return Err("Payload too short for UUID".to_string());
        }
        let val = self.data[self.offset..self.offset + 16].try_into().unwrap();
        self.offset += 16;
        Ok(val)
    }

    pub fn len(&self) -> usize {
        self.data.len() - self.offset
    }
}
