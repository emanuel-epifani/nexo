use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use super::errors::ParseError;
use super::frame::{
    FrameHeader, InboundFrame, OutboundFrame, Response, STATUS_DATA, STATUS_ERR, STATUS_NULL,
    STATUS_OK, TYPE_PUSH, TYPE_RESPONSE,
};

#[derive(Debug, Default)]
pub struct NexoCodec;

impl NexoCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Decoder for NexoCodec {
    type Item = InboundFrame;
    type Error = ParseError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < FrameHeader::SIZE {
            return Ok(None);
        }

        let header_ref: &FrameHeader = match bytemuck::try_from_bytes(&src[..FrameHeader::SIZE]) {
            Ok(header) => header,
            Err(_) => {
                return Err(ParseError::Invalid(
                    "Header alignment or size mismatch".to_string(),
                ))
            }
        };

        let payload_len = header_ref.payload_len() as usize;
        let total_len = FrameHeader::SIZE + payload_len;

        if src.len() < total_len {
            return Ok(None);
        }

        let header = *header_ref;
        let frame_bytes = src.split_to(total_len).freeze();
        let payload = frame_bytes.slice(FrameHeader::SIZE..);

        Ok(Some(InboundFrame { header, payload }))
    }
}

impl Encoder<OutboundFrame> for NexoCodec {
    type Error = ParseError;

    fn encode(&mut self, item: OutboundFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            OutboundFrame::Response { id, response } => {
                let (status, payload) = match response {
                    Response::Ok => (STATUS_OK, Bytes::new()),
                    Response::Null => (STATUS_NULL, Bytes::new()),
                    Response::Error(msg) => {
                        let mut buf = BytesMut::with_capacity(4 + msg.len());
                        buf.put_u32(msg.len() as u32);
                        buf.put_slice(msg.as_bytes());
                        (STATUS_ERR, buf.freeze())
                    }
                    Response::Data(data) => (STATUS_DATA, data),
                };

                dst.put_u8(TYPE_RESPONSE);
                dst.put_u8(status);
                dst.put_u32(id);
                dst.put_u32(payload.len() as u32);
                dst.extend_from_slice(&payload);
            }
            OutboundFrame::Push {
                id,
                push_type,
                payload,
            } => {
                dst.put_u8(TYPE_PUSH);
                dst.put_u8(push_type);
                dst.put_u32(id);
                dst.put_u32(payload.len() as u32);
                dst.extend_from_slice(&payload);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::protocol::frame::{STATUS_DATA, TYPE_RESPONSE};

    const TEST_ID: u32 = 42;
    const TEST_PAYLOAD: &[u8] = b"nexo-test-payload";

    #[test]
    fn round_trip_encode_decode_response_data() {
        let response = Response::Data(Bytes::copy_from_slice(TEST_PAYLOAD));
        let mut codec = NexoCodec::new();
        let mut encoded = BytesMut::new();

        codec
            .encode(
                OutboundFrame::Response {
                    id: TEST_ID,
                    response,
                },
                &mut encoded,
            )
            .expect("encode should succeed");

        let mut decode_buf = encoded.clone();
        let frame = codec
            .decode(&mut decode_buf)
            .expect("decode should succeed")
            .expect("frame should be complete");

        assert_eq!(frame.header.frame_type, TYPE_RESPONSE);
        assert_eq!(frame.header.meta, STATUS_DATA);
        assert_eq!(frame.header.id(), TEST_ID);
        assert_eq!(frame.payload, TEST_PAYLOAD);
    }

    #[test]
    fn parse_frame_returns_none_for_incomplete_header() {
        const INCOMPLETE_HEADER_SIZE: usize = FrameHeader::SIZE - 1;
        let mut codec = NexoCodec::new();
        let mut buf = BytesMut::from(&vec![0u8; INCOMPLETE_HEADER_SIZE][..]);

        let parsed = codec.decode(&mut buf).expect("decode should succeed");

        assert!(parsed.is_none());
    }

    #[test]
    fn parse_frame_returns_none_for_incomplete_payload() {
        let response = Response::Data(Bytes::copy_from_slice(TEST_PAYLOAD));
        let mut codec = NexoCodec::new();
        let mut encoded = BytesMut::new();

        codec
            .encode(
                OutboundFrame::Response {
                    id: TEST_ID,
                    response,
                },
                &mut encoded,
            )
            .expect("encode should succeed");

        let truncated_len = FrameHeader::SIZE + 1;
        let mut truncated = BytesMut::from(&encoded[..truncated_len]);

        let parsed = codec
            .decode(&mut truncated)
            .expect("decode should succeed");

        assert!(parsed.is_none());
    }
}
