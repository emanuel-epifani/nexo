//! On-wire payload codec: the single place that defines the binary layout of
//! broker payloads, in both directions.
//!
//! Conventions (writer and reader MUST stay in sync):
//!   - integers: big-endian (u8 / u32 / u64)
//!   - bool: 1 byte (0 / 1)
//!   - UUID: 16 raw bytes, no length prefix
//!   - byte block / string: u32 BE length prefix + raw bytes (UTF-8 for str)
//!
//! `PayloadWriter` serializes, `PayloadCursor` parses. Keep the put_*/read_*
//! pairs aligned so the layout stays consistent across every broker.

use crate::protocol::ParseError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

// ==========================================
// WRITE SIDE
// ==========================================

#[derive(Default)]
pub struct PayloadWriter {
    buf: BytesMut,
}

impl PayloadWriter {
    pub fn new() -> Self {
        Self { buf: BytesMut::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self { buf: BytesMut::with_capacity(cap) }
    }

    pub fn put_u8(&mut self, v: u8) -> &mut Self {
        self.buf.put_u8(v);
        self
    }

    pub fn put_bool(&mut self, v: bool) -> &mut Self {
        self.buf.put_u8(v as u8);
        self
    }

    pub fn put_u32(&mut self, v: u32) -> &mut Self {
        self.buf.put_u32(v);
        self
    }

    pub fn put_u16(&mut self, v: u16) -> &mut Self {
        self.buf.put_u16(v);
        self
    }

    pub fn put_u64(&mut self, v: u64) -> &mut Self {
        self.buf.put_u64(v);
        self
    }

    pub fn put_i64(&mut self, v: i64) -> &mut Self {
        self.buf.put_i64(v);
        self
    }

    /// 16-byte UUID, no length prefix.
    pub fn put_uuid(&mut self, bytes: &[u8; 16]) -> &mut Self {
        self.buf.put_slice(bytes);
        self
    }

    /// Raw bytes, no length prefix.
    pub fn put_raw(&mut self, bytes: &[u8]) -> &mut Self {
        self.buf.put_slice(bytes);
        self
    }

    /// Length-prefixed (u32 BE) byte block.
    pub fn put_bytes(&mut self, bytes: &[u8]) -> &mut Self {
        self.buf.put_u32(bytes.len() as u32);
        self.buf.put_slice(bytes);
        self
    }

    /// Length-prefixed (u32 BE) UTF-8 string.
    pub fn put_str(&mut self, s: &str) -> &mut Self {
        self.put_bytes(s.as_bytes())
    }

    pub fn into_bytes(self) -> Bytes {
        self.buf.freeze()
    }
}

// ==========================================
// READ SIDE
// ==========================================

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

    pub fn read_u16(&mut self) -> Result<u16, ParseError> {
        if !self.has_remaining(2) {
            return Err(ParseError::Invalid("Payload too short for u16".into()));
        }
        Ok(self.data.get_u16())
    }

    pub fn read_bytes(&mut self, len: usize) -> Result<Bytes, ParseError> {
        if !self.has_remaining(len) {
            return Err(ParseError::Invalid(format!("Payload too short: expected {} bytes", len)));
        }
        Ok(self.data.copy_to_bytes(len))
    }

    pub fn read_u64(&mut self) -> Result<u64, ParseError> {
        if !self.has_remaining(8) {
            return Err(ParseError::Invalid("Payload too short for u64".into()));
        }
        Ok(self.data.get_u64())
    }

    pub fn read_i64(&mut self) -> Result<i64, ParseError> {
        if !self.has_remaining(8) {
            return Err(ParseError::Invalid("Payload too short for i64".into()));
        }
        Ok(self.data.get_i64())
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
