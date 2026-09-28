//! End-to-end tests for the TCP transport over real loopback sockets:
//! response ordering under writer batching, request/no-response semantics,
//! protocol-error teardown, push delivery, and disconnect handling.

use bytes::{BufMut, Bytes};
use nexo::config::Config;
use nexo::protocol::{
    ErrorCode, PayloadWriter, OP_DEBUG_ECHO, OP_MAP_GET, OP_MAP_SET, OP_PUB, OP_SUB,
    PROTOCOL_VERSION, STATUS_DATA, STATUS_ERR, STATUS_NULL, STATUS_OK, TYPE_PUSH_PUBSUB,
    TYPE_REQUEST, TYPE_REQUEST_NO_RESPONSE,
};
use nexo::transport::tcp::connection::handle_connection;
use nexo::NexoEngine;
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

const TEST_MAX_PAYLOAD: usize = 4096;
const HEADER_SIZE: usize = 11;

// ==========================================
// WIRE HELPERS (raw client side)
// ==========================================

fn frame(frame_type: u8, meta: u8, id: u32, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
    buf.put_u8(PROTOCOL_VERSION);
    buf.put_u8(frame_type);
    buf.put_u8(meta);
    buf.put_u32(id);
    buf.put_u32(payload.len() as u32);
    buf.extend_from_slice(payload);
    buf
}

fn str_field(s: &str) -> Vec<u8> {
    let mut w = PayloadWriter::new();
    w.put_str(s);
    w.into_bytes().to_vec()
}

fn set_payload(key: &str, value: &[u8]) -> Vec<u8> {
    let mut w = PayloadWriter::new();
    w.put_str(key).put_u8(0); // flags: no TTL
    w.put_raw(value);
    w.into_bytes().to_vec()
}

fn pub_payload(topic: &str, data: &[u8]) -> Vec<u8> {
    let mut w = PayloadWriter::new();
    w.put_str(topic).put_u8(0); // flags: no retain/clear/ttl
    w.put_raw(data);
    w.into_bytes().to_vec()
}

struct RawFrame {
    frame_type: u8,
    meta: u8,
    id: u32,
    payload: Bytes,
}

async fn read_frame(s: &mut TcpStream) -> std::io::Result<RawFrame> {
    let mut header = [0u8; HEADER_SIZE];
    s.read_exact(&mut header).await?;
    let len = u32::from_be_bytes(header[7..11].try_into().expect("4-byte len")) as usize;
    let mut payload = vec![0u8; len];
    s.read_exact(&mut payload).await?;
    Ok(RawFrame {
        frame_type: header[1],
        meta: header[2],
        id: u32::from_be_bytes(header[3..7].try_into().expect("4-byte id")),
        payload: Bytes::from(payload),
    })
}

// ==========================================
// SERVER HARNESS
// ==========================================

async fn spawn_server() -> (SocketAddr, NexoEngine, TempDir) {
    let temp = tempfile::tempdir().expect("create tempdir");
    let mut config = Config::global().clone();
    for dir in ["queue", "pubsub", "stream"] {
        std::fs::create_dir_all(temp.path().join(dir)).expect("create persistence dir");
    }
    config.queue.persistence_path = temp.path().join("queue").to_string_lossy().into_owned();
    config.pubsub.persistence_path = temp.path().join("pubsub").to_string_lossy().into_owned();
    config.stream.persistence_path = temp.path().join("stream").to_string_lossy().into_owned();
    config.server.max_payload_size = TEST_MAX_PAYLOAD;

    let engine = NexoEngine::new(&config).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let server_config = config.server.clone();
    let accept_engine = engine.clone();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(handle_connection(
                socket,
                accept_engine.clone(),
                server_config.clone(),
            ));
        }
    });

    (addr, engine, temp)
}

// ==========================================
// TESTS
// ==========================================

#[tokio::test]
async fn pipelined_requests_keep_response_order() {
    let (addr, engine, _t) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();

    // All three frames in a single write: responses must come back in the
    // same order even when the writer coalesces them into one flush.
    let mut batch = frame(TYPE_REQUEST, OP_MAP_SET, 1, &set_payload("k", b"v1"));
    batch.extend_from_slice(&frame(
        TYPE_REQUEST,
        OP_MAP_SET,
        2,
        &set_payload("k", b"v2"),
    ));
    batch.extend_from_slice(&frame(TYPE_REQUEST, OP_MAP_GET, 3, &str_field("k")));
    s.write_all(&batch).await.unwrap();

    let r1 = read_frame(&mut s).await.unwrap();
    let r2 = read_frame(&mut s).await.unwrap();
    let r3 = read_frame(&mut s).await.unwrap();

    assert_eq!((r1.meta, r1.id), (STATUS_OK, 1));
    assert_eq!((r2.meta, r2.id), (STATUS_OK, 2));
    assert_eq!((r3.meta, r3.id), (STATUS_DATA, 3));
    assert_eq!(&r3.payload[..], b"v2");

    engine.shutdown().await;
}

#[tokio::test]
async fn no_response_frame_executes_without_reply() {
    let (addr, engine, _t) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();

    let mut batch = frame(
        TYPE_REQUEST_NO_RESPONSE,
        OP_MAP_SET,
        1,
        &set_payload("k", b"v"),
    );
    batch.extend_from_slice(&frame(TYPE_REQUEST, OP_MAP_GET, 2, &str_field("k")));
    s.write_all(&batch).await.unwrap();

    // The SET ran (inline, in arrival order) but produced no frame; the only
    // response on the wire is the GET's.
    let r = read_frame(&mut s).await.unwrap();
    assert_eq!((r.meta, r.id), (STATUS_DATA, 2));
    assert_eq!(&r.payload[..], b"v");
    assert!(timeout(Duration::from_millis(300), read_frame(&mut s))
        .await
        .is_err());

    engine.shutdown().await;
}

#[tokio::test]
async fn invalid_version_closes_connection() {
    let (addr, engine, _t) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();

    let mut bad = frame(TYPE_REQUEST, OP_MAP_GET, 1, &str_field("k"));
    bad[0] = 0xFF; // wrong PROTOCOL_VERSION
    s.write_all(&bad).await.unwrap();

    // Protocol errors are fatal: server must close the socket.
    assert!(read_frame(&mut s).await.is_err());

    engine.shutdown().await;
}

#[tokio::test]
async fn oversized_payload_closes_connection() {
    let (addr, engine, _t) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();

    // The decoder rejects on the header alone; no payload bytes needed.
    let mut bad = Vec::new();
    bad.put_u8(PROTOCOL_VERSION);
    bad.put_u8(TYPE_REQUEST);
    bad.put_u8(OP_MAP_GET);
    bad.put_u32(1);
    bad.put_u32((TEST_MAX_PAYLOAD + 1) as u32);
    s.write_all(&bad).await.unwrap();

    assert!(read_frame(&mut s).await.is_err());

    engine.shutdown().await;
}

#[tokio::test]
async fn unknown_opcode_returns_error_and_stays_alive() {
    let (addr, engine, _t) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();

    s.write_all(&frame(TYPE_REQUEST, 0x7F, 1, &[]))
        .await
        .unwrap();
    let r = read_frame(&mut s).await.unwrap();
    assert_eq!((r.meta, r.id), (STATUS_ERR, 1));
    assert_eq!(r.payload[0], ErrorCode::ProtocolError as u8);

    // A dispatch-level error is not fatal: the connection keeps working.
    s.write_all(&frame(TYPE_REQUEST, OP_MAP_GET, 2, &str_field("missing")))
        .await
        .unwrap();
    let r = read_frame(&mut s).await.unwrap();
    assert_eq!((r.meta, r.id), (STATUS_NULL, 2));

    engine.shutdown().await;
}

#[tokio::test]
async fn unsupported_frame_type_returns_error_and_stays_alive() {
    let (addr, engine, _t) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();

    s.write_all(&frame(0x7F, OP_MAP_GET, 1, &str_field("k")))
        .await
        .unwrap();
    let r = read_frame(&mut s).await.unwrap();
    assert_eq!((r.meta, r.id), (STATUS_ERR, 1));
    assert_eq!(r.payload[0], ErrorCode::ProtocolError as u8);

    s.write_all(&frame(TYPE_REQUEST, OP_MAP_GET, 2, &str_field("missing")))
        .await
        .unwrap();
    let r = read_frame(&mut s).await.unwrap();
    assert_eq!((r.meta, r.id), (STATUS_NULL, 2));

    engine.shutdown().await;
}

#[tokio::test]
async fn pubsub_push_flow_and_error_disconnect() {
    let (addr, engine, _t) = spawn_server().await;
    let mut a = TcpStream::connect(addr).await.unwrap();
    let mut b = TcpStream::connect(addr).await.unwrap();

    a.write_all(&frame(TYPE_REQUEST, OP_SUB, 1, &str_field("t")))
        .await
        .unwrap();
    let r = read_frame(&mut a).await.unwrap();
    assert_eq!(r.meta, STATUS_OK);

    b.write_all(&frame(TYPE_REQUEST, OP_PUB, 1, &pub_payload("t", b"hello")))
        .await
        .unwrap();
    let r = read_frame(&mut b).await.unwrap();
    assert_eq!(r.meta, STATUS_OK);

    let push = read_frame(&mut a).await.unwrap();
    assert_eq!(push.frame_type, TYPE_PUSH_PUBSUB);

    // Protocol error on A: server must tear the connection down and clean up
    // its pub/sub registration (zombie cleanup on next publish must not fail).
    let mut bad = frame(TYPE_REQUEST, OP_SUB, 2, &str_field("t"));
    bad[0] = 0xFF;
    a.write_all(&bad).await.unwrap();
    assert!(read_frame(&mut a).await.is_err());

    b.write_all(&frame(TYPE_REQUEST, OP_PUB, 2, &pub_payload("t", b"again")))
        .await
        .unwrap();
    let r = read_frame(&mut b).await.unwrap();
    assert_eq!(r.meta, STATUS_OK);

    engine.shutdown().await;
}

#[tokio::test]
async fn payload_at_max_size_is_accepted() {
    let (addr, engine, _t) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();

    // Exactly at the limit must pass — only len > max is rejected. Exercises
    // the per-connection max_payload_size injected into the codec.
    let data = vec![b'x'; TEST_MAX_PAYLOAD];
    s.write_all(&frame(TYPE_REQUEST, OP_DEBUG_ECHO, 1, &data))
        .await
        .unwrap();

    let r = read_frame(&mut s).await.unwrap();
    assert_eq!((r.meta, r.id), (STATUS_DATA, 1));
    assert_eq!(r.payload.len(), TEST_MAX_PAYLOAD);

    engine.shutdown().await;
}

#[tokio::test]
async fn large_pipeline_preserves_order_and_losses_none() {
    let (addr, engine, _t) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.unwrap();

    // 2048 pipelined requests exceed the outbound channel capacity (1024) and
    // the writer's drain batch (256), so this crosses several batch/queue
    // boundaries — any frame loss or reorder would show up here.
    const N: u32 = 2048;
    let mut batch = Vec::new();
    for i in 0..N {
        batch.extend_from_slice(&frame(TYPE_REQUEST, OP_DEBUG_ECHO, i, &i.to_be_bytes()));
    }
    s.write_all(&batch).await.unwrap();

    for i in 0..N {
        let r = read_frame(&mut s).await.unwrap();
        assert_eq!((r.meta, r.id), (STATUS_DATA, i));
        assert_eq!(&r.payload[..], &i.to_be_bytes());
    }

    engine.shutdown().await;
}

#[tokio::test]
async fn abrupt_client_drop_does_not_affect_others() {
    let (addr, engine, _t) = spawn_server().await;
    let mut a = TcpStream::connect(addr).await.unwrap();
    let mut b = TcpStream::connect(addr).await.unwrap();

    a.write_all(&frame(TYPE_REQUEST, OP_SUB, 1, &str_field("t")))
        .await
        .unwrap();
    assert_eq!(read_frame(&mut a).await.unwrap().meta, STATUS_OK);

    // Abrupt close without any protocol-level goodbye.
    drop(a);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publishing to a topic whose only subscriber vanished mid-flight must
    // still succeed — the dead client is cleaned up as a zombie.
    b.write_all(&frame(TYPE_REQUEST, OP_PUB, 1, &pub_payload("t", b"x")))
        .await
        .unwrap();
    let r = read_frame(&mut b).await.unwrap();
    assert_eq!(r.meta, STATUS_OK);

    engine.shutdown().await;
}
