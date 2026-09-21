use bytes::{BufMut, Bytes};
use nexo::brokers::pub_sub::PubSubManager;
use nexo::config::Config;
use nexo::protocol::{
    PayloadWriter, OP_PUB, OP_SUB, OP_UNSUB, PROTOCOL_VERSION, TYPE_PUSH_PUBSUB, TYPE_REQUEST,
};
use nexo::transport::tcp::connection::handle_connection;
use nexo::NexoEngine;
use rusqlite::{params, Connection};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

const HEADER_SIZE: usize = 11;

fn manager_config(temp: &TempDir) -> nexo::brokers::pub_sub::config::PubSubConfig {
    let mut config = Config::global().pubsub.clone();
    config.persistence_path = temp.path().to_string_lossy().into_owned();
    config.cleanup_interval_seconds = 3_600;
    config.retained_flush_ms = 50;
    config
}

fn frame(opcode: u8, id: u32, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
    buf.put_u8(PROTOCOL_VERSION);
    buf.put_u8(TYPE_REQUEST);
    buf.put_u8(opcode);
    buf.put_u32(id);
    buf.put_u32(payload.len() as u32);
    buf.extend_from_slice(payload);
    buf
}

fn str_field(value: &str) -> Vec<u8> {
    let mut writer = PayloadWriter::new();
    writer.put_str(value);
    writer.into_bytes().to_vec()
}

fn pub_payload(topic: &str, data: &[u8]) -> Vec<u8> {
    let mut writer = PayloadWriter::new();
    writer.put_str(topic).put_u8(0).put_raw(data);
    writer.into_bytes().to_vec()
}

struct RawFrame {
    frame_type: u8,
    id: u32,
}

async fn read_frame(stream: &mut TcpStream) -> std::io::Result<RawFrame> {
    let mut header = [0_u8; HEADER_SIZE];
    stream.read_exact(&mut header).await?;
    let payload_len = u32::from_be_bytes(header[7..11].try_into().unwrap()) as usize;
    let mut payload = vec![0_u8; payload_len];
    stream.read_exact(&mut payload).await?;
    Ok(RawFrame {
        frame_type: header[1],
        id: u32::from_be_bytes(header[3..7].try_into().unwrap()),
    })
}

async fn spawn_server() -> (SocketAddr, NexoEngine, TempDir) {
    let temp = tempfile::tempdir().unwrap();
    let mut config = Config::global().clone();
    config.queue.persistence_path = temp.path().join("queue").to_string_lossy().into_owned();
    config.pubsub.persistence_path = temp.path().join("pubsub").to_string_lossy().into_owned();
    config.stream.persistence_path = temp.path().join("stream").to_string_lossy().into_owned();
    std::fs::create_dir_all(&config.queue.persistence_path).unwrap();
    std::fs::create_dir_all(&config.pubsub.persistence_path).unwrap();
    std::fs::create_dir_all(&config.stream.persistence_path).unwrap();

    let engine = NexoEngine::new(&config).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accept_engine = engine.clone();
    let server_config = config.server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            let _ = handle_connection(socket, accept_engine, server_config).await;
        }
    });
    (address, engine, temp)
}

#[tokio::test]
async fn duplicate_sub_must_not_replay_retained_twice() {
    let temp = tempfile::tempdir().unwrap();
    let manager = PubSubManager::new(Arc::new(manager_config(&temp)));
    manager
        .publish("dup/topic", Bytes::from_static(b"value"), true, false, None)
        .unwrap();
    let (sender, mut receiver) = mpsc::channel(8);
    manager.connect("client", sender);

    manager.subscribe("client", "dup/topic").unwrap();
    assert!(receiver.recv().await.is_some());
    manager.subscribe("client", "dup/topic").unwrap();

    assert!(timeout(Duration::from_millis(100), receiver.recv()).await.is_err());
}

#[tokio::test]
async fn retained_replay_must_not_disconnect_a_runnable_consumer_on_burst_size() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = manager_config(&temp);
    config.push_channel_capacity = 2;
    let manager = PubSubManager::new(Arc::new(config));
    for index in 0..3 {
        manager
            .publish(
                &format!("replay/{index}"),
                Bytes::from_static(b"value"),
                true,
                false,
                None,
            )
            .unwrap();
    }
    let (sender, mut receiver) = mpsc::channel(2);
    manager.connect("client", sender);
    let drain = tokio::spawn(async move { while receiver.recv().await.is_some() {} });
    tokio::task::yield_now().await;

    assert!(manager.subscribe("client", "replay/#").is_ok());
    assert!(manager.exists("client"));
    drain.abort();
}

#[tokio::test]
async fn shutdown_must_stop_old_tasks_from_overwriting_a_new_manager() {
    let temp = tempfile::tempdir().unwrap();
    let mut old_config = manager_config(&temp);
    old_config.retained_flush_ms = 5_000;
    old_config.cleanup_interval_seconds = 1;
    let old = PubSubManager::new(Arc::new(old_config));
    old.publish(
        "old/topic",
        Bytes::from_static(b"old"),
        true,
        false,
        Some(1),
    )
    .unwrap();
    old.shutdown();
    drop(old);

    let current = PubSubManager::new(Arc::new(manager_config(&temp)));
    current
        .publish("old/topic", Bytes::new(), false, true, None)
        .unwrap();
    current
        .publish(
            "current/topic",
            Bytes::from_static(b"current"),
            true,
            false,
            Some(60),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    tokio::time::sleep(Duration::from_millis(5_100)).await;

    let recovered = PubSubManager::new(Arc::new(manager_config(&temp)));
    let (sender, mut receiver) = mpsc::channel(8);
    recovered.connect("reader", sender);
    recovered.subscribe("reader", "current/topic").unwrap();
    let message = timeout(Duration::from_millis(250), receiver.recv())
        .await
        .expect("current retained value must still be persisted")
        .expect("channel must stay open");
    assert_eq!(message.payload, Bytes::from_static(b"current"));
}

#[tokio::test]
async fn failed_flush_must_be_retried_without_another_publish() {
    let temp = tempfile::tempdir().unwrap();
    let database_path = temp.path().join("retained.db");
    let seed = Connection::open(&database_path).unwrap();
    seed.execute(
        "CREATE TABLE retained (path TEXT PRIMARY KEY, data BLOB NOT NULL, expires_at INTEGER)",
        [],
    )
    .unwrap();
    seed.execute(
        "INSERT INTO retained (path, data, expires_at) VALUES (?1, ?2, NULL)",
        params!["retry/topic", b"old"],
    )
    .unwrap();
    drop(seed);

    let mut config = manager_config(&temp);
    config.retained_flush_ms = 200;
    let manager = PubSubManager::new(Arc::new(config));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let lock = Connection::open(&database_path).unwrap();
    lock.execute_batch("BEGIN EXCLUSIVE").unwrap();
    manager
        .publish(
            "retry/topic",
            Bytes::from_static(b"new"),
            true,
            false,
            Some(60),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    lock.execute_batch("COMMIT").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let recovered = PubSubManager::new(Arc::new(manager_config(&temp)));
    let (sender, mut receiver) = mpsc::channel(8);
    recovered.connect("reader", sender);
    recovered.subscribe("reader", "retry/topic").unwrap();
    let message = receiver.recv().await.unwrap();
    assert_eq!(message.payload, Bytes::from_static(b"new"));
}

#[tokio::test]
async fn malformed_retained_schema_must_not_silently_start_empty() {
    let temp = tempfile::tempdir().unwrap();
    let database_path = temp.path().join("retained.db");
    let seed = Connection::open(&database_path).unwrap();
    seed.execute(
        "CREATE TABLE retained (path TEXT PRIMARY KEY, data BLOB NOT NULL)",
        [],
    )
    .unwrap();
    seed.execute(
        "INSERT INTO retained (path, data) VALUES (?1, ?2)",
        params!["startup/topic", b"persisted"],
    )
    .unwrap();
    drop(seed);

    let manager = PubSubManager::new(Arc::new(manager_config(&temp)));
    let (sender, mut receiver) = mpsc::channel(8);
    manager.connect("reader", sender);
    manager.subscribe("reader", "startup/topic").unwrap();
    let message = timeout(Duration::from_millis(250), receiver.recv())
        .await
        .expect("startup must not hide all retained data after a storage/schema error")
        .expect("channel must stay open");
    assert_eq!(message.payload, Bytes::from_static(b"persisted"));
}

#[tokio::test]
async fn pub_before_sub_in_one_tcp_batch_must_not_reach_the_later_subscription() {
    let (address, engine, _temp) = spawn_server().await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut batch = frame(OP_PUB, 1, &pub_payload("ordered/topic", b"before"));
    batch.extend_from_slice(&frame(OP_SUB, 2, &str_field("ordered/topic")));
    stream.write_all(&batch).await.unwrap();

    let mut pushes = 0;
    let mut responses = 0;
    while responses < 2 {
        let received = timeout(Duration::from_secs(1), read_frame(&mut stream))
            .await
            .unwrap()
            .unwrap();
        if received.frame_type == TYPE_PUSH_PUBSUB {
            pushes += 1;
        } else if received.id == 1 || received.id == 2 {
            responses += 1;
        }
    }
    assert_eq!(pushes, 0);
    engine.shutdown().await;
}

#[tokio::test]
async fn pub_before_unsub_in_one_tcp_batch_must_reach_the_existing_subscription() {
    let (address, engine, _temp) = spawn_server().await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(&frame(OP_SUB, 1, &str_field("ordered/topic")))
        .await
        .unwrap();
    let _ = read_frame(&mut stream).await.unwrap();

    let mut batch = frame(OP_PUB, 2, &pub_payload("ordered/topic", b"before-stop"));
    batch.extend_from_slice(&frame(OP_UNSUB, 3, &str_field("ordered/topic")));
    stream.write_all(&batch).await.unwrap();

    let mut pushes = 0;
    let mut responses = 0;
    while responses < 2 {
        let received = timeout(Duration::from_secs(1), read_frame(&mut stream))
            .await
            .unwrap()
            .unwrap();
        if received.frame_type == TYPE_PUSH_PUBSUB {
            pushes += 1;
        } else if received.id == 2 || received.id == 3 {
            responses += 1;
        }
    }
    assert_eq!(pushes, 1);
    engine.shutdown().await;
}
