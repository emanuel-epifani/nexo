use crate::server::protocol::ParseError;
use bytes::{Buf, Bytes};

pub struct PayloadCursor {
    data: Bytes,
}

impl PayloadCursor {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    pub fn has_remaining(&self, len: usize) -> bool {
        self.data.remaining() >= len
    }

    pub fn read_u8(&mut self) -> Result<u8, ParseError> {
        if !self.has_remaining(1) {
            return Err(ParseError::Invalid("Payload too short for u8".into()));
        }
        Ok(self.data.get_u8())
    }

    pub fn read_u32(&mut self) -> Result<u32, ParseError> {
        if !self.has_remaining(4) {
            return Err(ParseError::Invalid("Payload too short for u32".into()));
        }
        Ok(self.data.get_u32())
    }

    pub fn read_u64(&mut self) -> Result<u64, ParseError> {
        if !self.has_remaining(8) {
            return Err(ParseError::Invalid("Payload too short for u64".into()));
        }
        Ok(self.data.get_u64())
    }

    pub fn read_string(&mut self) -> Result<String, ParseError> {
        let len = self.read_u32()? as usize;
        if !self.has_remaining(len) {
            return Err(ParseError::Invalid(format!("Incomplete string: expected {} bytes", len)));
        }
        let chunk = self.data.copy_to_bytes(len);
        let s = std::str::from_utf8(&chunk).map_err(|e| ParseError::Invalid(format!("Invalid UTF-8 in string: {}", e)))?;
        Ok(s.to_string())
    }

    pub fn read_remaining(&mut self) -> Bytes {
        let len = self.data.remaining();
        self.data.copy_to_bytes(len)
    }

    pub fn read_uuid_bytes(&mut self) -> Result<[u8; 16], ParseError> {
        if !self.has_remaining(16) {
            return Err(ParseError::Invalid("Payload too short for UUID".into()));
        }
        let mut val = [0u8; 16];
        self.data.copy_to_slice(&mut val);
        Ok(val)
    }

    pub fn len(&self) -> usize {
        self.data.remaining()
    }
}
