use bytes::{BufMut, Bytes, BytesMut};

/// Symmetric counterpart to [`PayloadCursor`](super::cursor::PayloadCursor):
/// the single place where broker `ToWire` impls serialize binary payloads,
/// so the on-wire layout (big-endian ints, u32 length-prefixed blocks) stays
/// consistent across every broker.
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

    pub fn put_u64(&mut self, v: u64) -> &mut Self {
        self.buf.put_u64(v);
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
