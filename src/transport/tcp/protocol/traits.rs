use bytes::Bytes;

pub trait ToWire {
    fn to_wire(&self) -> Bytes;
}
