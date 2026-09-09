//! Wire-parity codec tests: the Rust server decodes SDK-encoded frames
//! and the parsed command fields must match the fixture input JSON.

use base64::Engine;
use bytes::Bytes;
use serde_json::Value;
use std::time::Instant;

use nexo::brokers::pub_sub::tcp::PubSubCommand;
use nexo::brokers::queue::tcp::QueueCommand;
use nexo::brokers::store::tcp::{MapCmd, StoreCommand};
use nexo::brokers::stream::options::SeekTarget;
use nexo::brokers::stream::tcp::StreamCommand;
use nexo::protocol::wire::{PayloadCursor, PayloadWriter};

const HEADER_LEN: usize = 11;

fn load_fixtures() -> Vec<Value> {
    let json = std::fs::read_to_string("sdk/codec-fixtures.json").expect("read fixtures");
    serde_json::from_str::<Vec<Value>>(&json).expect("parse fixtures")
}

fn decode_frame(b64: &str) -> (u8, Bytes) {
    let frame = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .expect("base64 decode");
    let opcode = frame[2];
    let payload = Bytes::copy_from_slice(&frame[HEADER_LEN..]);
    (opcode, payload)
}

fn dt_byte(s: &str) -> u8 {
    match s {
        "raw" => 0,
        "string" => 1,
        "json" => 2,
        _ => panic!("unknown data_type: {s}"),
    }
}

fn expected_payload(data_type: &str, payload_b64: &str) -> Bytes {
    let mut buf = vec![dt_byte(data_type)];
    if !payload_b64.is_empty() {
        buf.extend_from_slice(
            &base64::engine::general_purpose::STANDARD
                .decode(payload_b64)
                .expect("base64 decode payload"),
        );
    }
    Bytes::from(buf)
}

fn parse_uuid(hex: &str) -> uuid::Uuid {
    uuid::Uuid::parse_str(hex).unwrap_or_else(|e| panic!("invalid uuid {hex}: {e}"))
}

// ─── Store ───────────────────────────────────────────────────────────

#[test]
fn store_fixtures() {
    for f in load_fixtures()
        .into_iter()
        .filter(|f| f["broker"] == "store")
    {
        let id = f["id"].as_str().unwrap();
        let (opcode, payload) = decode_frame(f["expected_bytes"].as_str().unwrap());
        let mut c = PayloadCursor::new(payload);
        let cmd = StoreCommand::parse(opcode, &mut c)
            .unwrap_or_else(|e| panic!("parse failed for {id}: {e}"));
        assert_eq!(c.len(), 0, "leftover bytes for {id}");

        let inp = &f["input"];
        match cmd {
            StoreCommand::Map(nexo::brokers::store::tcp::MapCmd::Set { key, ttl, value }) => {
                assert_eq!(key, inp["key"].as_str().unwrap(), "{id}: key");
                let exp_ttl = inp["ttl"].as_u64();
                assert_eq!(ttl, exp_ttl, "{id}: ttl");
                let v = &inp["value"];
                let exp = expected_payload(
                    v["data_type"].as_str().unwrap(),
                    v["payload"].as_str().unwrap(),
                );
                assert_eq!(value, exp, "{id}: value");
            }
            StoreCommand::Map(nexo::brokers::store::tcp::MapCmd::Get { key }) => {
                assert_eq!(key, inp["key"].as_str().unwrap(), "{id}: key");
            }
            StoreCommand::Map(nexo::brokers::store::tcp::MapCmd::Del { key }) => {
                assert_eq!(key, inp["key"].as_str().unwrap(), "{id}: key");
            }
            StoreCommand::Map(nexo::brokers::store::tcp::MapCmd::Incr { key, delta }) => {
                assert_eq!(key, inp["key"].as_str().unwrap(), "{id}: key");
                assert_eq!(delta, inp["delta"].as_i64().unwrap(), "{id}: delta");
            }
            StoreCommand::Map(nexo::brokers::store::tcp::MapCmd::ClearAll) => {}
            StoreCommand::Map(nexo::brokers::store::tcp::MapCmd::ClearPrefix { prefix }) => {
                assert_eq!(prefix, inp["prefix"].as_str().unwrap(), "{id}: prefix");
            }
        }
    }
}

// ─── PubSub ──────────────────────────────────────────────────────────

#[test]
fn pubsub_fixtures() {
    for f in load_fixtures()
        .into_iter()
        .filter(|f| f["broker"] == "pubsub")
    {
        let id = f["id"].as_str().unwrap();
        let (opcode, payload) = decode_frame(f["expected_bytes"].as_str().unwrap());
        let mut c = PayloadCursor::new(payload);
        let cmd = PubSubCommand::parse(opcode, &mut c)
            .unwrap_or_else(|e| panic!("parse failed for {id}: {e}"));
        assert_eq!(c.len(), 0, "leftover bytes for {id}");

        let inp = &f["input"];
        match cmd {
            PubSubCommand::Publish {
                topic,
                retain,
                clear,
                ttl,
                payload,
            } => {
                assert_eq!(topic, inp["topic"].as_str().unwrap(), "{id}: topic");
                assert_eq!(retain, inp["retain"].as_bool().unwrap(), "{id}: retain");
                if let Some(clear_val) = inp.get("clear").and_then(|v| v.as_bool()) {
                    assert_eq!(clear, clear_val, "{id}: clear");
                }
                assert_eq!(ttl, inp["ttl"].as_u64().map(|v| v as u32), "{id}: ttl");
                let d = &inp["data"];
                let exp = expected_payload(
                    d["data_type"].as_str().unwrap(),
                    d["payload"].as_str().unwrap(),
                );
                assert_eq!(payload, exp, "{id}: payload");
            }
            PubSubCommand::Subscribe { topic } => {
                assert_eq!(topic, inp["topic"].as_str().unwrap(), "{id}: topic");
            }
            PubSubCommand::Unsubscribe { topic } => {
                assert_eq!(topic, inp["topic"].as_str().unwrap(), "{id}: topic");
            }
        }
    }
}

// ─── Queue ───────────────────────────────────────────────────────────

#[test]
fn queue_fixtures() {
    for f in load_fixtures()
        .into_iter()
        .filter(|f| f["broker"] == "queue")
    {
        let id = f["id"].as_str().unwrap();
        let (opcode, payload) = decode_frame(f["expected_bytes"].as_str().unwrap());
        let mut c = PayloadCursor::new(payload);
        let cmd = QueueCommand::parse(opcode, &mut c)
            .unwrap_or_else(|e| panic!("parse failed for {id}: {e}"));
        assert_eq!(c.len(), 0, "leftover bytes for {id}");

        let inp = &f["input"];
        let q = inp["queue"].as_str().unwrap();
        match cmd {
            QueueCommand::Create { q_name, options } => {
                assert_eq!(q_name, q, "{id}: queue");
                assert_eq!(
                    options.visibility_timeout_ms,
                    inp["visibility_timeout_ms"].as_u64(),
                    "{id}: vis_timeout",
                );
                assert_eq!(
                    options.max_deliveries,
                    inp["max_deliveries"].as_u64().map(|v| v as u32),
                    "{id}: max_deliveries",
                );
            }
            QueueCommand::Push { q_name, items } => {
                assert_eq!(q_name, q, "{id}: queue");
                let exp_items = inp["items"].as_array().unwrap();
                assert_eq!(items.len(), exp_items.len(), "{id}: item count");
                for (i, (actual, exp)) in items.iter().zip(exp_items).enumerate() {
                    assert_eq!(
                        actual.priority,
                        exp.get("priority")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u8),
                        "{id}: item {i} priority",
                    );
                    let ap = expected_payload(
                        exp["data_type"].as_str().unwrap(),
                        exp["payload"].as_str().unwrap(),
                    );
                    assert_eq!(actual.payload, ap, "{id}: item {i} payload");
                }
            }
            QueueCommand::Consume {
                q_name,
                batch_size,
                wait_ms,
            } => {
                assert_eq!(q_name, q, "{id}: queue");
                assert_eq!(
                    batch_size,
                    inp["batch_size"].as_u64().unwrap() as usize,
                    "{id}: batch_size"
                );
                assert_eq!(wait_ms, inp["wait_ms"].as_u64().unwrap(), "{id}: wait_ms");
            }
            QueueCommand::Ack {
                id: uid,
                delivery_token,
                q_name,
            } => {
                assert_eq!(q_name, q, "{id}: queue");
                assert_eq!(
                    uid,
                    parse_uuid(inp["message_id"].as_str().unwrap()),
                    "{id}: message_id"
                );
                assert_eq!(
                    delivery_token,
                    inp["delivery_token"].as_u64().unwrap(),
                    "{id}: delivery_token"
                );
            }
            QueueCommand::Nack {
                id: uid,
                delivery_token,
                q_name,
                reason,
            } => {
                assert_eq!(q_name, q, "{id}: queue");
                assert_eq!(
                    uid,
                    parse_uuid(inp["message_id"].as_str().unwrap()),
                    "{id}: message_id"
                );
                assert_eq!(
                    delivery_token,
                    inp["delivery_token"].as_u64().unwrap(),
                    "{id}: delivery_token"
                );
                assert_eq!(reason, inp["reason"].as_str().unwrap(), "{id}: reason");
            }
            QueueCommand::Exists { q_name } | QueueCommand::Describe { q_name } => {
                assert_eq!(q_name, q, "{id}: queue");
            }
            QueueCommand::Delete { q_name } => {
                assert_eq!(q_name, q, "{id}: queue");
            }
            QueueCommand::PeekDLQ {
                q_name,
                limit,
                offset,
            } => {
                assert_eq!(q_name, q, "{id}: queue");
                assert_eq!(
                    limit,
                    inp["limit"].as_u64().unwrap() as usize,
                    "{id}: limit"
                );
                assert_eq!(
                    offset,
                    inp["offset"].as_u64().unwrap() as usize,
                    "{id}: offset"
                );
            }
            QueueCommand::MoveToQueue { q_name, message_id } => {
                assert_eq!(q_name, q, "{id}: queue");
                assert_eq!(
                    message_id,
                    parse_uuid(inp["message_id"].as_str().unwrap()),
                    "{id}: message_id"
                );
            }
            QueueCommand::DeleteDLQ { q_name, message_id } => {
                assert_eq!(q_name, q, "{id}: queue");
                assert_eq!(
                    message_id,
                    parse_uuid(inp["message_id"].as_str().unwrap()),
                    "{id}: message_id"
                );
            }
            QueueCommand::PurgeDLQ { q_name } => {
                assert_eq!(q_name, q, "{id}: queue");
            }
        }
    }
}

// ─── Stream ──────────────────────────────────────────────────────────

#[test]
fn stream_fixtures() {
    for f in load_fixtures()
        .into_iter()
        .filter(|f| f["broker"] == "stream")
    {
        let id = f["id"].as_str().unwrap();
        let (opcode, payload) = decode_frame(f["expected_bytes"].as_str().unwrap());
        let mut c = PayloadCursor::new(payload);
        let cmd = StreamCommand::parse(opcode, &mut c)
            .unwrap_or_else(|e| panic!("parse failed for {id}: {e}"));
        assert_eq!(c.len(), 0, "leftover bytes for {id}");

        let inp = &f["input"];
        let name = inp["stream"].as_str().unwrap();
        match cmd {
            StreamCommand::Create { name: t, options } => {
                assert_eq!(t, name, "{id}: name");
                let ret = options.retention;
                assert_eq!(
                    ret.as_ref().and_then(|r| r.max_age_ms),
                    inp["max_age_ms"].as_u64(),
                    "{id}: max_age_ms",
                );
                assert_eq!(
                    ret.as_ref().and_then(|r| r.max_bytes),
                    inp["max_bytes"].as_u64(),
                    "{id}: max_bytes",
                );
            }
            StreamCommand::Publish { name: t, items } => {
                assert_eq!(t, name, "{id}: name");
                let exp_items = inp["items"].as_array().unwrap();
                assert_eq!(items.len(), exp_items.len(), "{id}: item count");
                for (i, (actual, exp)) in items.iter().zip(exp_items).enumerate() {
                    match &actual.key {
                        Some(k) => {
                            let exp_key = exp["key"].as_str().unwrap();
                            assert_eq!(k.as_ref(), exp_key.as_bytes(), "{id}: item {i} key");
                        }
                        None => {
                            assert!(exp["key"].is_null(), "{id}: item {i} expected null key");
                        }
                    }
                    let ap = expected_payload(
                        exp["data_type"].as_str().unwrap(),
                        exp["payload"].as_str().unwrap(),
                    );
                    assert_eq!(actual.payload, ap, "{id}: item {i} payload");
                }
            }
            StreamCommand::Fetch {
                name: t,
                group,
                consumer_id,
                generation,
                limit,
                wait_ms,
            } => {
                assert_eq!(t, name, "{id}: name");
                assert_eq!(group, inp["group"].as_str().unwrap(), "{id}: group");
                assert_eq!(
                    consumer_id,
                    inp["consumer_id"].as_str().unwrap(),
                    "{id}: consumer_id"
                );
                assert_eq!(
                    generation,
                    inp["generation"].as_u64().unwrap(),
                    "{id}: generation"
                );
                assert_eq!(
                    limit,
                    inp["batch_size"].as_u64().unwrap() as u32,
                    "{id}: batch_size"
                );
                assert_eq!(
                    wait_ms,
                    inp["wait_ms"].as_u64().unwrap() as u32,
                    "{id}: wait_ms"
                );
            }
            StreamCommand::Join { name: t, group } => {
                assert_eq!(t, name, "{id}: name");
                assert_eq!(group, inp["group"].as_str().unwrap(), "{id}: group");
            }
            StreamCommand::Ack {
                name: t,
                group,
                consumer_id,
                generation,
                seq,
            } => {
                assert_eq!(t, name, "{id}: name");
                assert_eq!(group, inp["group"].as_str().unwrap(), "{id}: group");
                assert_eq!(
                    consumer_id,
                    inp["consumer_id"].as_str().unwrap(),
                    "{id}: consumer_id"
                );
                assert_eq!(
                    generation,
                    inp["generation"].as_u64().unwrap(),
                    "{id}: generation"
                );
                assert_eq!(seq, inp["seq"].as_u64().unwrap(), "{id}: seq");
            }
            StreamCommand::Seek {
                name: t,
                group,
                target,
            } => {
                assert_eq!(t, name, "{id}: name");
                assert_eq!(group, inp["group"].as_str().unwrap(), "{id}: group");
                let exp_target = match inp["target"].as_str().unwrap() {
                    "beginning" => SeekTarget::Beginning,
                    "end" => SeekTarget::End,
                    other => panic!("{id}: unknown seek target {other}"),
                };
                assert_eq!(target, exp_target, "{id}: target");
            }
            StreamCommand::Exists { name: t } | StreamCommand::Describe { name: t } => {
                assert_eq!(t, name, "{id}: name");
            }
            StreamCommand::Delete { name: t } => {
                assert_eq!(t, name, "{id}: name");
            }
            StreamCommand::Leave {
                name: t,
                group,
                consumer_id,
                generation,
            } => {
                assert_eq!(t, name, "{id}: name");
                assert_eq!(group, inp["group"].as_str().unwrap(), "{id}: group");
                assert_eq!(
                    consumer_id,
                    inp["consumer_id"].as_str().unwrap(),
                    "{id}: consumer_id"
                );
                assert_eq!(
                    generation,
                    inp["generation"].as_u64().unwrap(),
                    "{id}: generation"
                );
            }
            StreamCommand::PeekDls {
                name: t,
                group,
                limit,
                offset,
            } => {
                assert_eq!(t, name, "{id}: name");
                assert_eq!(group, inp["group"].as_str().unwrap(), "{id}: group");
                assert_eq!(limit, inp["limit"].as_u64().unwrap() as u32, "{id}: limit");
                assert_eq!(
                    offset,
                    inp["offset"].as_u64().unwrap() as u32,
                    "{id}: offset"
                );
            }
            StreamCommand::MoveToStream {
                name: t,
                group,
                seq,
            } => {
                assert_eq!(t, name, "{id}: name");
                assert_eq!(group, inp["group"].as_str().unwrap(), "{id}: group");
                assert_eq!(seq, inp["seq"].as_u64().unwrap(), "{id}: seq");
            }
            StreamCommand::DeleteDls {
                name: t,
                group,
                seq,
            } => {
                assert_eq!(t, name, "{id}: name");
                assert_eq!(group, inp["group"].as_str().unwrap(), "{id}: group");
                assert_eq!(seq, inp["seq"].as_u64().unwrap(), "{id}: seq");
            }
            StreamCommand::PurgeDls { name: t, group } => {
                assert_eq!(t, name, "{id}: name");
                assert_eq!(group, inp["group"].as_str().unwrap(), "{id}: group");
            }
        }
    }
}

// ─── Codec Benchmark ─────────────────────────────────────────────────
// Round-trip: encode → parse → verify, N iterations. Prints ops/sec.
// Run with: cargo test --test codec_fixtures codec_benchmark -- --nocapture --test-threads=1

#[test]
fn codec_benchmark() {
    const N: u32 = 100_000;

    // Scenario 1: Small (store GET — one string)
    {
        let mut ok = 0u32;
        let t0 = Instant::now();
        for _ in 0..N {
            let mut w = PayloadWriter::new();
            w.put_str("foo");
            let payload = w.into_bytes();
            let mut c = PayloadCursor::new(payload);
            if let Ok(StoreCommand::Map(MapCmd::Get { key })) = StoreCommand::parse(0x03, &mut c) {
                if key == "foo" && c.len() == 0 {
                    ok += 1;
                }
            }
        }
        let elapsed = t0.elapsed();
        let ops_sec = N as f64 / elapsed.as_secs_f64();
        println!("CODEC BENCH  store_get (small)     {N:>7} iter | {ops_sec:>12.0} ops/sec | {elapsed:?}");
        assert_eq!(ok, N, "round-trip correctness failed");
    }

    // Scenario 2: Medium (store SET JSON — string + flags + any payload)
    let json_bytes = br#"{"x":1}"#;
    {
        let mut ok = 0u32;
        let t0 = Instant::now();
        for _ in 0..N {
            let mut w = PayloadWriter::new();
            w.put_str("foo");
            w.put_u8(0); // no TTL
            w.put_u8(2); // DataType::JSON
            w.put_raw(json_bytes);
            let payload = w.into_bytes();
            let mut c = PayloadCursor::new(payload);
            if let Ok(StoreCommand::Map(MapCmd::Set { key, ttl, value })) =
                StoreCommand::parse(0x02, &mut c)
            {
                if key == "foo"
                    && ttl.is_none()
                    && value.len() == json_bytes.len() + 1
                    && c.len() == 0
                {
                    ok += 1;
                }
            }
        }
        let elapsed = t0.elapsed();
        let ops_sec = N as f64 / elapsed.as_secs_f64();
        println!("CODEC BENCH  store_set_json (med)  {N:>7} iter | {ops_sec:>12.0} ops/sec | {elapsed:?}");
        assert_eq!(ok, N, "round-trip correctness failed");
    }

    // Scenario 3: Batch (queue PUSH 100 items with priority)
    {
        let mut ok = 0u32;
        let t0 = Instant::now();
        for _ in 0..N {
            let mut w = PayloadWriter::new();
            w.put_str("emails");
            w.put_u32(100);
            for i in 0..100u8 {
                w.put_u8(0x01); // has priority
                w.put_u8(i);
                w.put_u32(5);
                w.put_raw(b"hello");
            }
            let payload = w.into_bytes();
            let mut c = PayloadCursor::new(payload);
            if let Ok(QueueCommand::Push { q_name, items }) = QueueCommand::parse(0x11, &mut c) {
                if q_name == "emails" && items.len() == 100 && c.len() == 0 {
                    ok += 1;
                }
            }
        }
        let elapsed = t0.elapsed();
        let ops_sec = N as f64 / elapsed.as_secs_f64();
        println!("CODEC BENCH  queue_push_batch (lg) {N:>7} iter | {ops_sec:>12.0} ops/sec | {elapsed:?}");
        assert_eq!(ok, N, "round-trip correctness failed");
    }
}
