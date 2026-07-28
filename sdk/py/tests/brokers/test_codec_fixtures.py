import base64
import json
import os
import time

import pytest

from nexo.codec import Cursor, FrameWriter
from nexo.protocol import DataType, FrameType, PROTOCOL_VERSION

_FIXTURES_PATH = os.path.join(os.path.dirname(__file__), "../..", "..", "codec-fixtures.json")
FIXTURES = json.load(open(_FIXTURES_PATH))


def _frame_type_number(name: str) -> int:
    if name == "REQUEST":
        return FrameType.REQUEST
    if name == "REQUEST_NO_RESPONSE":
        return FrameType.REQUEST_NO_RESPONSE
    if name == "RESPONSE":
        return FrameType.RESPONSE
    raise ValueError(name)


def _payload_to_object(value: dict) -> object:
    raw = base64.b64decode(value["payload"])
    dtype = value["data_type"]
    if dtype == "raw":
        return raw
    if dtype == "string":
        return raw.decode("utf-8")
    if dtype == "json":
        return json.loads(raw.decode("utf-8")) if raw else None
    raise ValueError(dtype)


def _object_to_payload(obj: object) -> dict:
    if isinstance(obj, (bytes, bytearray, memoryview)):
        if isinstance(obj, memoryview):
            obj = bytes(obj)
        return {"data_type": "raw", "payload": base64.b64encode(obj).decode()}
    if isinstance(obj, str):
        return {"data_type": "string", "payload": base64.b64encode(obj.encode("utf-8")).decode()}
    s = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    return {"data_type": "json", "payload": base64.b64encode(s).decode()}


def _write_any(w: FrameWriter, value: dict) -> None:
    w.any(_payload_to_object(value))


def _write_any_with_len(w: FrameWriter, value: dict) -> None:
    w.any_with_len(_payload_to_object(value))


def _read_value(dtype: int, data: bytes) -> dict:
    if dtype == DataType.RAW:
        return {"data_type": "raw", "payload": base64.b64encode(data).decode()}
    if dtype == DataType.STRING:
        s = data.decode("utf-8")
        return {"data_type": "string", "payload": base64.b64encode(s.encode("utf-8")).decode()}
    if dtype == DataType.JSON:
        s = data.decode("utf-8")
        parsed = json.loads(s) if s else None
        compact = json.dumps(parsed, separators=(",", ":"))
        return {"data_type": "json", "payload": base64.b64encode(compact.encode("utf-8")).decode()}
    raise ValueError(dtype)


def _read_any_no_len(c: Cursor) -> dict:
    dtype = c.read_u8()
    data = c.buf[c.offset :]
    c.offset = len(c.buf)
    return _read_value(dtype, data)


def _read_any_with_len(c: Cursor) -> dict:
    length = c.read_u32()
    dtype = c.read_u8()
    data = c.read_buffer(length - 1)
    return _read_value(dtype, data)


def _write_stream_item(w: FrameWriter, item: dict) -> None:
    key = item.get("key")
    if key is None:
        w.u16(0)
    else:
        key_bytes = key.encode("utf-8") if isinstance(key, str) else bytes(key)
        w.u16(len(key_bytes))
        w.raw_bytes(key_bytes)
    _write_any_with_len(w, item)


def _read_stream_item(c: Cursor) -> dict:
    key_len = c.read_u16()
    key = c.read_buffer(key_len).decode("utf-8") if key_len > 0 else None
    value = _read_any_with_len(c)
    return {"key": key, **value}


def _encode_fixture(fixture: dict) -> bytes:
    w = FrameWriter()
    w.begin()
    inp = fixture["input"]
    fid = fixture["id"]

    if fid in ("STORE_MAP_SET_JSON", "STORE_MAP_SET_STRING_TTL"):
        has_ttl = inp.get("ttl") is not None
        w.string(inp["key"]).u8(1 if has_ttl else 0)
        if has_ttl:
            w.u64(inp["ttl"])
        _write_any(w, inp["value"])

    elif fid in ("STORE_MAP_GET", "STORE_MAP_DEL"):
        w.string(inp["key"])

    elif fid in (
        "PUBSUB_PUB_JSON",
        "PUBSUB_PUB_STRING_RETAIN",
        "PUBSUB_PUB_RAW_TTL",
        "PUBSUB_PUB_JSON_RETAIN_TTL",
    ):
        flags = 0
        if inp.get("retain"):
            flags |= 0x01
        if inp.get("ttl") is not None:
            flags |= 0x02
        w.string(inp["topic"]).u8(flags)
        if inp.get("ttl") is not None:
            w.u32(inp["ttl"])
        _write_any(w, inp["data"])

    elif fid == "PUBSUB_CLEAR":
        w.string(inp["topic"]).u8(0x04)
        _write_any(w, inp["data"])

    elif fid in ("PUBSUB_SUB", "PUBSUB_UNSUB"):
        w.string(inp["topic"])

    elif fid in ("QUEUE_CREATE", "QUEUE_CREATE_FULL"):
        has_vto = inp.get("visibility_timeout_ms") is not None
        has_retries = inp.get("max_deliveries") is not None
        flags = (0x01 if has_vto else 0x00) | (0x02 if has_retries else 0x00)
        w.string(inp["queue"]).u8(flags)
        if has_vto:
            w.u64(inp["visibility_timeout_ms"])
        if has_retries:
            w.u32(inp["max_deliveries"])

    elif fid in ("QUEUE_EXISTS", "QUEUE_DELETE", "QUEUE_PURGE_DLQ"):
        w.string(inp["queue"])

    elif fid in ("QUEUE_PUSH_JSON", "QUEUE_PUSH_STRING_PRIORITY", "QUEUE_PUSH_BATCH"):
        items = inp["items"]
        w.string(inp["queue"]).u32(len(items))
        for item in items:
            priority = item.get("priority")
            has_priority = priority is not None
            w.u8(1 if has_priority else 0)
            if has_priority:
                w.u8(priority)
            _write_any_with_len(w, item)

    elif fid == "QUEUE_CONSUME":
        w.string(inp["queue"]).u32(inp["batch_size"]).u32(inp["wait_ms"])

    elif fid == "QUEUE_ACK":
        w.uuid(inp["message_id"]).u64(inp["delivery_token"]).string(inp["queue"])

    elif fid == "QUEUE_NACK":
        w.uuid(inp["message_id"]).u64(inp["delivery_token"]).string(inp["queue"]).string(inp["reason"])

    elif fid == "QUEUE_PEEK_DLQ":
        w.string(inp["queue"]).u32(inp["limit"]).u32(inp["offset"])

    elif fid in ("QUEUE_MOVE_TO_QUEUE", "QUEUE_DELETE_DLQ"):
        w.string(inp["queue"]).uuid(inp["message_id"])

    elif fid in ("STREAM_CREATE", "STREAM_CREATE_RETENTION"):
        has_age = inp.get("max_age_ms") is not None
        has_bytes = inp.get("max_bytes") is not None
        flags = (0x01 if has_age else 0x00) | (0x02 if has_bytes else 0x00)
        w.string(inp["stream"]).u8(flags)
        if has_age:
            w.u64(inp["max_age_ms"])
        if has_bytes:
            w.u64(inp["max_bytes"])

    elif fid in ("STREAM_EXISTS", "STREAM_DELETE", "STREAM_PURGE_DLT"):
        w.string(inp["stream"])
        if fid == "STREAM_PURGE_DLT":
            w.string(inp["group"])

    elif fid in ("STREAM_PUB_JSON", "STREAM_PUB_STRING_KEY", "STREAM_PUB_BATCH"):
        items = inp["items"]
        w.string(inp["stream"]).u32(len(items))
        for item in items:
            _write_stream_item(w, item)

    elif fid == "STREAM_FETCH":
        (
            w.string(inp["stream"])
            .string(inp["group"])
            .string(inp["consumer_id"])
            .u64(inp["generation"])
            .u32(inp["batch_size"])
            .u32(inp["wait_ms"])
        )

    elif fid == "STREAM_JOIN":
        w.string(inp["stream"]).string(inp["group"])

    elif fid == "STREAM_ACK":
        (
            w.string(inp["stream"])
            .string(inp["group"])
            .string(inp["consumer_id"])
            .u64(inp["generation"])
            .u64(inp["seq"])
        )


    elif fid == "STREAM_SEEK_END":
        w.string(inp["stream"]).string(inp["group"]).u8(0 if inp["target"] == "beginning" else 1)

    elif fid == "STREAM_LEAVE":
        w.string(inp["stream"]).string(inp["group"]).string(inp["consumer_id"]).u64(inp["generation"])

    elif fid == "STREAM_PEEK_DLT":
        w.string(inp["stream"]).string(inp["group"]).u32(inp["limit"]).u32(inp["offset"])

    elif fid in ("STREAM_MOVE_TO_STREAM", "STREAM_DELETE_DLT"):
        w.string(inp["stream"]).string(inp["group"]).u64(inp["seq"])

    else:
        raise RuntimeError(f"No encoder for fixture {fid}")

    return w.finish(fixture["correlation_id"], fixture["opcode"], _frame_type_number(fixture["frame_type"]))


def _decode_fixture(fixture: dict) -> object:
    frame = base64.b64decode(fixture["expected_bytes"])
    c = Cursor(frame)
    version = c.read_u8()
    frame_type = c.read_u8()
    opcode = c.read_u8()
    corr_id = c.read_u32()
    payload_len = c.read_u32()

    assert version == PROTOCOL_VERSION
    assert frame_type == _frame_type_number(fixture["frame_type"])
    assert opcode == fixture["opcode"]
    assert corr_id == fixture["correlation_id"]

    payload = frame[c.offset : c.offset + payload_len]
    c = Cursor(payload)
    fid = fixture["id"]

    if fid in ("STORE_MAP_SET_JSON", "STORE_MAP_SET_STRING_TTL"):
        key = c.read_string()
        flags = c.read_u8()
        ttl = c.read_u64() if flags & 0x01 else None
        value = _read_any_no_len(c)
        return {"key": key, "ttl": ttl, "value": value}

    if fid in ("STORE_MAP_GET", "STORE_MAP_DEL"):
        return {"key": c.read_string()}

    if fid in (
        "PUBSUB_PUB_JSON",
        "PUBSUB_PUB_STRING_RETAIN",
        "PUBSUB_PUB_RAW_TTL",
        "PUBSUB_PUB_JSON_RETAIN_TTL",
        "PUBSUB_CLEAR",
    ):
        topic = c.read_string()
        flags = c.read_u8()
        retain = bool(flags & 0x01)
        ttl = c.read_u32() if flags & 0x02 else None
        data = _read_any_no_len(c)
        return {"topic": topic, "retain": retain, "ttl": ttl, "data": data}

    if fid in ("PUBSUB_SUB", "PUBSUB_UNSUB"):
        return {"topic": c.read_string()}

    if fid in ("QUEUE_CREATE", "QUEUE_CREATE_FULL"):
        queue = c.read_string()
        flags = c.read_u8()
        vto = c.read_u64() if flags & 0x01 else None
        retries = c.read_u32() if flags & 0x02 else None
        return {"queue": queue, "visibility_timeout_ms": vto, "max_deliveries": retries}

    if fid in ("QUEUE_EXISTS", "QUEUE_DELETE", "QUEUE_PURGE_DLQ"):
        return {"queue": c.read_string()}

    if fid in ("QUEUE_PUSH_JSON", "QUEUE_PUSH_STRING_PRIORITY", "QUEUE_PUSH_BATCH"):
        queue = c.read_string()
        count = c.read_u32()
        items = []
        for _ in range(count):
            flags = c.read_u8()
            priority = c.read_u8() if flags & 0x01 else None
            value = _read_any_with_len(c)
            items.append({"priority": priority, **value} if priority is not None else value)
        return {"queue": queue, "items": items}

    if fid == "QUEUE_CONSUME":
        return {
            "queue": c.read_string(),
            "batch_size": c.read_u32(),
            "wait_ms": c.read_u32(),
        }

    if fid == "QUEUE_ACK":
        return {"message_id": c.read_uuid(), "delivery_token": c.read_u64(), "queue": c.read_string()}

    if fid == "QUEUE_NACK":
        return {"message_id": c.read_uuid(), "delivery_token": c.read_u64(), "queue": c.read_string(), "reason": c.read_string()}

    if fid == "QUEUE_PEEK_DLQ":
        return {"queue": c.read_string(), "limit": c.read_u32(), "offset": c.read_u32()}

    if fid in ("QUEUE_MOVE_TO_QUEUE", "QUEUE_DELETE_DLQ"):
        return {"queue": c.read_string(), "message_id": c.read_uuid()}

    if fid in ("STREAM_CREATE", "STREAM_CREATE_RETENTION"):
        stream = c.read_string()
        flags = c.read_u8()
        max_age = c.read_u64() if flags & 0x01 else None
        max_bytes = c.read_u64() if flags & 0x02 else None
        return {"stream": stream, "max_age_ms": max_age, "max_bytes": max_bytes}

    if fid in ("STREAM_EXISTS", "STREAM_DELETE"):
        return {"stream": c.read_string()}

    if fid == "STREAM_PURGE_DLT":
        return {"stream": c.read_string(), "group": c.read_string()}

    if fid in ("STREAM_PUB_JSON", "STREAM_PUB_STRING_KEY", "STREAM_PUB_BATCH"):
        stream = c.read_string()
        count = c.read_u32()
        items = [_read_stream_item(c) for _ in range(count)]
        return {"stream": stream, "items": items}

    if fid == "STREAM_FETCH":
        return {
            "stream": c.read_string(),
            "group": c.read_string(),
            "consumer_id": c.read_string(),
            "generation": c.read_u64(),
            "batch_size": c.read_u32(),
            "wait_ms": c.read_u32(),
        }

    if fid == "STREAM_JOIN":
        return {"stream": c.read_string(), "group": c.read_string()}

    if fid == "STREAM_ACK":
        return {
            "stream": c.read_string(),
            "group": c.read_string(),
            "consumer_id": c.read_string(),
            "generation": c.read_u64(),
            "seq": c.read_u64(),
        }


    if fid == "STREAM_SEEK_END":
        return {
            "stream": c.read_string(),
            "group": c.read_string(),
            "target": "beginning" if c.read_u8() == 0 else "end",
        }

    if fid == "STREAM_LEAVE":
        return {
            "stream": c.read_string(),
            "group": c.read_string(),
            "consumer_id": c.read_string(),
            "generation": c.read_u64(),
        }

    if fid == "STREAM_PEEK_DLT":
        return {
            "stream": c.read_string(),
            "group": c.read_string(),
            "limit": c.read_u32(),
            "offset": c.read_u32(),
        }

    if fid in ("STREAM_MOVE_TO_STREAM", "STREAM_DELETE_DLT"):
        return {
            "stream": c.read_string(),
            "group": c.read_string(),
            "seq": c.read_u64(),
        }

    raise RuntimeError(f"No decoder for fixture {fid}")


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda f: f["id"])
def test_encode_matches_expected_bytes(fixture: dict) -> None:
    encoded = _encode_fixture(fixture)
    assert encoded == base64.b64decode(fixture["expected_bytes"])


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda f: f["id"])
def test_decode_matches_expected_input(fixture: dict) -> None:
    decoded = _decode_fixture(fixture)
    assert decoded == fixture["input"]


# ─── Codec Benchmark ─────────────────────────────────────────────────
# Round-trip: encode → decode → verify, N iterations. Prints ops/sec.
# Run with: pytest test_codec_fixtures.py -k benchmark -s

N_BENCH = 100_000


def test_bench_store_get_small():
    ok = 0
    t0 = time.perf_counter()
    for _ in range(N_BENCH):
        w = FrameWriter()
        w.begin()
        w.string("foo")
        frame = w.finish(1, 0x03, FrameType.REQUEST)
        payload = frame[11:]
        c = Cursor(payload)
        key = c.read_string()
        if key == "foo" and c.offset == len(payload):
            ok += 1
    elapsed = time.perf_counter() - t0
    ops_sec = N_BENCH / elapsed
    print(f"CODEC BENCH  store_get (small)     {N_BENCH:>7} iter | {ops_sec:>12.0f} ops/sec | {elapsed*1000:.1f}ms")
    assert ok == N_BENCH


def test_bench_store_set_json_medium():
    json_bytes = b'{"x":1}'
    ok = 0
    t0 = time.perf_counter()
    for _ in range(N_BENCH):
        w = FrameWriter()
        w.begin()
        w.string("foo")
        w.u8(0)  # no TTL flags
        w.u8(DataType.JSON)
        w.raw_bytes(json_bytes)
        frame = w.finish(1, 0x02, FrameType.REQUEST)
        payload = frame[11:]
        c = Cursor(payload)
        key = c.read_string()
        flags = c.read_u8()
        ttl = c.read_u64() if flags & 0x01 else None
        value = c.buf[c.offset:]
        c.offset = len(c.buf)
        if key == "foo" and ttl is None and len(value) == len(json_bytes) + 1 and c.offset == len(payload):
            ok += 1
    elapsed = time.perf_counter() - t0
    ops_sec = N_BENCH / elapsed
    print(f"CODEC BENCH  store_set_json (med)  {N_BENCH:>7} iter | {ops_sec:>12.0f} ops/sec | {elapsed*1000:.1f}ms")
    assert ok == N_BENCH


def test_bench_queue_push_batch_large():
    ok = 0
    t0 = time.perf_counter()
    for _ in range(N_BENCH):
        w = FrameWriter()
        w.begin()
        w.string("emails")
        w.u32(100)
        for j in range(100):
            w.u8(0x01)  # has priority
            w.u8(j)
            w.u32(5)
            w.raw_bytes(b"hello")
        frame = w.finish(1, 0x11, FrameType.REQUEST)
        payload = frame[11:]
        c = Cursor(payload)
        q_name = c.read_string()
        count = c.read_u32()
        for _ in range(count):
            flags = c.read_u8()
            if flags & 0x01:
                c.read_u8()
            plen = c.read_u32()
            c.read_buffer(plen)
        if q_name == "emails" and count == 100 and c.offset == len(payload):
            ok += 1
    elapsed = time.perf_counter() - t0
    ops_sec = N_BENCH / elapsed
    print(f"CODEC BENCH  queue_push_batch (lg) {N_BENCH:>7} iter | {ops_sec:>12.0f} ops/sec | {elapsed*1000:.1f}ms")
    assert ok == N_BENCH
