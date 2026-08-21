import { describe, it, expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import { FrameWriter, Cursor } from '../../src/protocol/codec';
import { FrameType, DataType, HEADER_SIZE, PROTOCOL_VERSION } from '../../src/protocol/generated';

interface PayloadValue {
  data_type: 'raw' | 'string' | 'json';
  payload: string;
}

interface Fixture {
  id: string;
  broker: string;
  opcode: number;
  frame_type: 'REQUEST' | 'REQUEST_NO_RESPONSE' | 'RESPONSE';
  correlation_id: number;
  input: any;
  expected_bytes: string;
}

const fixturesPath = path.join(__dirname, '../../../codec-fixtures.json');
const fixtures: Fixture[] = JSON.parse(fs.readFileSync(fixturesPath, 'utf-8'));

function frameTypeNumber(ft: string): number {
  if (ft === 'REQUEST') return FrameType.REQUEST;
  if (ft === 'REQUEST_NO_RESPONSE') return FrameType.REQUEST_NO_RESPONSE;
  if (ft === 'RESPONSE') return FrameType.RESPONSE;
  throw new Error(`Unknown frame_type: ${ft}`);
}

function decodeBase64(b64: string): Buffer {
  return Buffer.from(b64, 'base64');
}

function payloadToValue(v: PayloadValue): any {
  const raw = decodeBase64(v.payload);
  if (v.data_type === 'raw') return raw;
  if (v.data_type === 'string') return raw.toString('utf-8');
  if (v.data_type === 'json') {
    const s = raw.toString('utf-8');
    return s ? JSON.parse(s) : null;
  }
  throw new Error(`Unknown data_type: ${v.data_type}`);
}

function bytesToValue(type: number, data: Buffer): PayloadValue {
  if (type === DataType.RAW) {
    return { data_type: 'raw', payload: data.toString('base64') };
  }
  if (type === DataType.STRING) {
    const s = data.toString('utf-8');
    return { data_type: 'string', payload: Buffer.from(s, 'utf-8').toString('base64') };
  }
  if (type === DataType.JSON) {
    const s = data.toString('utf-8');
    const parsed = s ? JSON.parse(s) : null;
    const compact = JSON.stringify(parsed);
    return { data_type: 'json', payload: Buffer.from(compact, 'utf-8').toString('base64') };
  }
  throw new Error(`Unknown DataType: ${type}`);
}

function writeAny(w: FrameWriter, v: PayloadValue): void {
  w.any(payloadToValue(v));
}

function writeAnyWithLen(w: FrameWriter, v: PayloadValue): void {
  w.anyWithLen(payloadToValue(v));
}

function readAnyValueNoLen(c: Cursor): PayloadValue {
  const type = c.readU8();
  const start = c.offset;
  const end = c.buf.length;
  c.offset = end;
  return bytesToValue(type, c.buf.subarray(start, end));
}

function readAnyValueWithLen(c: Cursor): PayloadValue {
  const len = c.readU32();
  const type = c.readU8();
  const data = c.readBuffer(len - 1);
  return bytesToValue(type, data);
}

function writeStreamItem(w: FrameWriter, item: any): void {
  const key = item.key as string | null;
  if (key === null || key === undefined) {
    w.u16(0);
  } else {
    const keyBytes = Buffer.from(key, 'utf-8');
    w.u16(keyBytes.length);
    w.bytes(keyBytes);
  }
  writeAnyWithLen(w, item as PayloadValue);
}

function readStreamItem(c: Cursor): any {
  const keyLen = c.readU16();
  const key = keyLen > 0 ? c.readBuffer(keyLen).toString('utf-8') : null;
  const value = readAnyValueWithLen(c);
  return { key, ...value };
}

function u64n(v: bigint | number): number | bigint {
  return typeof v === 'bigint' ? Number(v) : v;
}

function encodeFixture(fixture: Fixture): Buffer {
  const w = new FrameWriter();
  w.begin();
  const inp = fixture.input;

  switch (fixture.id) {
    // STORE
    case 'STORE_MAP_SET_JSON':
    case 'STORE_MAP_SET_STRING_TTL': {
      const hasTtl = inp.ttl !== null && inp.ttl !== undefined;
      w.string(inp.key).u8(hasTtl ? 1 : 0);
      if (hasTtl) w.u64(BigInt(inp.ttl));
      writeAny(w, inp.value as PayloadValue);
      break;
    }
    case 'STORE_MAP_GET':
    case 'STORE_MAP_DEL':
      w.string(inp.key);
      break;

    // PUBSUB
    case 'PUBSUB_PUB_JSON':
    case 'PUBSUB_PUB_STRING_RETAIN':
    case 'PUBSUB_PUB_RAW_TTL':
    case 'PUBSUB_PUB_JSON_RETAIN_TTL': {
      let flags = 0;
      if (inp.retain) flags |= 0x01;
      if (inp.ttl !== null && inp.ttl !== undefined) flags |= 0x02;
      w.string(inp.topic).u8(flags);
      if (inp.ttl !== null && inp.ttl !== undefined) w.u32(inp.ttl);
      writeAny(w, inp.data as PayloadValue);
      break;
    }
    case 'PUBSUB_CLEAR': {
      w.string(inp.topic).u8(0x04);
      writeAny(w, inp.data as PayloadValue);
      break;
    }
    case 'PUBSUB_SUB':
    case 'PUBSUB_UNSUB':
      w.string(inp.topic);
      break;

    // QUEUE
    case 'QUEUE_CREATE':
    case 'QUEUE_CREATE_FULL': {
      const hasVto = inp.visibility_timeout_ms !== null && inp.visibility_timeout_ms !== undefined;
      const hasRetries = inp.max_deliveries !== null && inp.max_deliveries !== undefined;
      let flags = 0;
      if (hasVto) flags |= 0x01;
      if (hasRetries) flags |= 0x02;
      w.string(inp.queue).u8(flags);
      if (hasVto) w.u64(BigInt(inp.visibility_timeout_ms));
      if (hasRetries) w.u32(inp.max_deliveries);
      break;
    }
    case 'QUEUE_EXISTS':
    case 'QUEUE_DELETE':
    case 'QUEUE_PURGE_DLQ':
      w.string(inp.queue);
      break;
    case 'QUEUE_PUSH_JSON':
    case 'QUEUE_PUSH_STRING_PRIORITY':
    case 'QUEUE_PUSH_BATCH': {
      const items = inp.items as any[];
      w.string(inp.queue).u32(items.length);
      for (const item of items) {
        const priority = item.priority;
        const hasPriority = priority !== null && priority !== undefined;
        w.u8(hasPriority ? 1 : 0);
        if (hasPriority) w.u8(priority);
        writeAnyWithLen(w, item as PayloadValue);
      }
      break;
    }
    case 'QUEUE_CONSUME':
      w.string(inp.queue).u32(inp.batch_size).u32(inp.wait_ms);
      break;
    case 'QUEUE_ACK':
      w.uuid(inp.message_id).u64(BigInt(inp.delivery_token)).string(inp.queue);
      break;
    case 'QUEUE_NACK':
      w.uuid(inp.message_id).u64(BigInt(inp.delivery_token)).string(inp.queue).string(inp.reason);
      break;
    case 'QUEUE_PEEK_DLQ':
      w.string(inp.queue).u32(inp.limit).u32(inp.offset);
      break;
    case 'QUEUE_MOVE_TO_QUEUE':
    case 'QUEUE_DELETE_DLQ':
      w.string(inp.queue).uuid(inp.message_id);
      break;

    // STREAM
    case 'STREAM_CREATE':
    case 'STREAM_CREATE_RETENTION': {
      const hasMaxAge = inp.max_age_ms !== null && inp.max_age_ms !== undefined;
      const hasMaxBytes = inp.max_bytes !== null && inp.max_bytes !== undefined;
      let flags = 0;
      if (hasMaxAge) flags |= 0x01;
      if (hasMaxBytes) flags |= 0x02;
      w.string(inp.stream).u8(flags);
      if (hasMaxAge) w.u64(BigInt(inp.max_age_ms));
      if (hasMaxBytes) w.u64(BigInt(inp.max_bytes));
      break;
    }
    case 'STREAM_EXISTS':
    case 'STREAM_DELETE':
      w.string(inp.stream);
      break;
    case 'STREAM_PURGE_DLT':
      w.string(inp.stream).string(inp.group);
      break;
    case 'STREAM_PUB_JSON':
    case 'STREAM_PUB_STRING_KEY':
    case 'STREAM_PUB_BATCH': {
      const items = inp.items as any[];
      w.string(inp.stream).u32(items.length);
      for (const item of items) writeStreamItem(w, item);
      break;
    }
    case 'STREAM_FETCH':
      w.string(inp.stream)
        .string(inp.group)
        .string(inp.consumer_id)
        .u64(BigInt(inp.generation))
        .u32(inp.batch_size)
        .u32(inp.wait_ms);
      break;
    case 'STREAM_JOIN':
      w.string(inp.stream).string(inp.group);
      break;
    case 'STREAM_ACK':
      w.string(inp.stream)
        .string(inp.group)
        .string(inp.consumer_id)
        .u64(BigInt(inp.generation))
        .u64(BigInt(inp.seq));
      break;
    case 'STREAM_SEEK_END':
      w.string(inp.stream).string(inp.group).u8(inp.target === 'beginning' ? 0 : 1);
      break;
    case 'STREAM_LEAVE':
      w.string(inp.stream).string(inp.group).string(inp.consumer_id).u64(BigInt(inp.generation));
      break;
    case 'STREAM_PEEK_DLT':
      w.string(inp.stream).string(inp.group).u32(inp.limit).u32(inp.offset);
      break;
    case 'STREAM_MOVE_TO_STREAM':
    case 'STREAM_DELETE_DLT':
      w.string(inp.stream).string(inp.group).u64(BigInt(inp.seq));
      break;

    default:
      throw new Error(`No encoder for fixture ${fixture.id}`);
  }

  return w.finish(fixture.correlation_id, fixture.opcode, frameTypeNumber(fixture.frame_type));
}

function decodeFixture(fixture: Fixture): any {
  const frame = Buffer.from(fixture.expected_bytes, 'base64');
  const cursor = new Cursor(frame);
  const version = cursor.readU8();
  const frameType = cursor.readU8();
  const opcode = cursor.readU8();
  const corrId = cursor.readU32();
  const payloadLen = cursor.readU32();
  expect(version).toBe(PROTOCOL_VERSION);
  expect(frameType).toBe(frameTypeNumber(fixture.frame_type));
  expect(opcode).toBe(fixture.opcode);
  expect(corrId).toBe(fixture.correlation_id);

  const payload = frame.subarray(cursor.offset, cursor.offset + payloadLen);
  const c = new Cursor(payload);
  const inp = fixture.input;

  switch (fixture.id) {
    // STORE
    case 'STORE_MAP_SET_JSON':
    case 'STORE_MAP_SET_STRING_TTL': {
      const key = c.readString();
      const flags = c.readU8();
      const ttl = flags & 0x01 ? Number(c.readU64()) : null;
      const value = readAnyValueNoLen(c);
      return { key, ttl, value };
    }
    case 'STORE_MAP_GET':
    case 'STORE_MAP_DEL':
      return { key: c.readString() };

    // PUBSUB
    case 'PUBSUB_PUB_JSON':
    case 'PUBSUB_PUB_STRING_RETAIN':
    case 'PUBSUB_PUB_RAW_TTL':
    case 'PUBSUB_PUB_JSON_RETAIN_TTL':
    case 'PUBSUB_CLEAR': {
      const topic = c.readString();
      const flags = c.readU8();
      const retain = !!(flags & 0x01);
      const ttl = flags & 0x02 ? c.readU32() : null;
      const data = readAnyValueNoLen(c);
      return { topic, retain, ttl, data };
    }
    case 'PUBSUB_SUB':
    case 'PUBSUB_UNSUB':
      return { topic: c.readString() };

    // QUEUE
    case 'QUEUE_CREATE':
    case 'QUEUE_CREATE_FULL': {
      const queue = c.readString();
      const flags = c.readU8();
      const visibilityTimeoutMs = flags & 0x01 ? Number(c.readU64()) : null;
      const maxDeliveries = flags & 0x02 ? c.readU32() : null;
      return { queue, visibility_timeout_ms: visibilityTimeoutMs, max_deliveries: maxDeliveries };
    }
    case 'QUEUE_EXISTS':
    case 'QUEUE_DELETE':
    case 'QUEUE_PURGE_DLQ':
      return { queue: c.readString() };
    case 'QUEUE_PUSH_JSON':
    case 'QUEUE_PUSH_STRING_PRIORITY':
    case 'QUEUE_PUSH_BATCH': {
      const queue = c.readString();
      const count = c.readU32();
      const items: any[] = [];
      for (let i = 0; i < count; i++) {
        const flags = c.readU8();
        const priority = flags & 0x01 ? c.readU8() : null;
        const value = readAnyValueWithLen(c);
        items.push(priority !== null ? { priority, ...value } : value);
      }
      return { queue, items };
    }
    case 'QUEUE_CONSUME':
      return { queue: c.readString(), batch_size: c.readU32(), wait_ms: c.readU32() };
    case 'QUEUE_ACK': {
      const messageId = c.readUUID();
      const deliveryToken = Number(c.readU64());
      const queue = c.readString();
      return { queue, message_id: messageId, delivery_token: deliveryToken };
    }
    case 'QUEUE_NACK': {
      const messageId = c.readUUID();
      const deliveryToken = Number(c.readU64());
      const queue = c.readString();
      const reason = c.readString();
      return { queue, message_id: messageId, delivery_token: deliveryToken, reason };
    }
    case 'QUEUE_PEEK_DLQ':
      return { queue: c.readString(), limit: c.readU32(), offset: c.readU32() };
    case 'QUEUE_MOVE_TO_QUEUE':
    case 'QUEUE_DELETE_DLQ': {
      const queue = c.readString();
      const messageId = c.readUUID();
      return { queue, message_id: messageId };
    }

    // STREAM
    case 'STREAM_CREATE':
    case 'STREAM_CREATE_RETENTION': {
      const stream = c.readString();
      const flags = c.readU8();
      const maxAgeMs = flags & 0x01 ? Number(c.readU64()) : null;
      const maxBytes = flags & 0x02 ? Number(c.readU64()) : null;
      return { stream, max_age_ms: maxAgeMs, max_bytes: maxBytes };
    }
    case 'STREAM_EXISTS':
    case 'STREAM_DELETE':
      return { stream: c.readString() };
    case 'STREAM_PURGE_DLT':
      return { stream: c.readString(), group: c.readString() };
    case 'STREAM_PUB_JSON':
    case 'STREAM_PUB_STRING_KEY':
    case 'STREAM_PUB_BATCH': {
      const stream = c.readString();
      const count = c.readU32();
      const items: any[] = [];
      for (let i = 0; i < count; i++) items.push(readStreamItem(c));
      return { stream, items };
    }
    case 'STREAM_FETCH':
      return {
        stream: c.readString(),
        group: c.readString(),
        consumer_id: c.readString(),
        generation: Number(c.readU64()),
        batch_size: c.readU32(),
        wait_ms: c.readU32(),
      };
    case 'STREAM_JOIN':
      return { stream: c.readString(), group: c.readString() };
    case 'STREAM_ACK':
      return {
        stream: c.readString(),
        group: c.readString(),
        consumer_id: c.readString(),
        generation: Number(c.readU64()),
        seq: Number(c.readU64()),
      };
    case 'STREAM_SEEK_END': {
      const stream = c.readString();
      const group = c.readString();
      const target = c.readU8() === 0 ? 'beginning' : 'end';
      return { stream, group, target };
    }
    case 'STREAM_LEAVE':
      return {
        stream: c.readString(),
        group: c.readString(),
        consumer_id: c.readString(),
        generation: Number(c.readU64()),
      };
    case 'STREAM_PEEK_DLT':
      return { stream: c.readString(), group: c.readString(), limit: c.readU32(), offset: c.readU32() };
    case 'STREAM_MOVE_TO_STREAM':
    case 'STREAM_DELETE_DLT': {
      const stream = c.readString();
      const group = c.readString();
      const seq = Number(c.readU64());
      return { stream, group, seq };
    }

    default:
      throw new Error(`No decoder for fixture ${fixture.id}`);
  }
}

for (const fixture of fixtures) {
  describe(fixture.id, () => {
    it('encode matches expected bytes', () => {
      const encoded = encodeFixture(fixture);
      expect(encoded).toEqual(Buffer.from(fixture.expected_bytes, 'base64'));
    });

    it('decode matches expected input', () => {
      const decoded = decodeFixture(fixture);
      expect(decoded).toEqual(fixture.input);
    });
  });
}

// ─── Codec Benchmark ─────────────────────────────────────────────────
// Round-trip: encode → decode → verify, N iterations. Prints ops/sec.
// Run with: npx vitest run test-codec-fixtures.test.ts -t "benchmark"

describe('codec benchmark', () => {
  const N = 100_000;

  it('store_get (small)', () => {
    let ok = 0;
    const t0 = performance.now();
    for (let i = 0; i < N; i++) {
      const w = new FrameWriter();
      w.begin();
      w.string('foo');
      const frame = w.finish(1, 0x03, FrameType.REQUEST);
      const payload = frame.subarray(HEADER_SIZE);
      const c = new Cursor(payload);
      const key = c.readString();
      if (key === 'foo' && c.offset === payload.length) ok++;
    }
    const elapsed = performance.now() - t0;
    const opsSec = N / (elapsed / 1000);
    console.log(`CODEC BENCH  store_get (small)     ${N.toString().padStart(7)} iter | ${opsSec.toFixed(0).padStart(12)} ops/sec | ${elapsed.toFixed(1)}ms`);
    expect(ok).toBe(N);
  });

  it('store_set_json (medium)', () => {
    const jsonBytes = Buffer.from('{"x":1}', 'utf8');
    let ok = 0;
    const t0 = performance.now();
    for (let i = 0; i < N; i++) {
      const w = new FrameWriter();
      w.begin();
      w.string('foo');
      w.u8(0); // no TTL flags
      w.u8(DataType.JSON);
      w.bytes(jsonBytes);
      const frame = w.finish(1, 0x02, FrameType.REQUEST);
      const payload = frame.subarray(HEADER_SIZE);
      const c = new Cursor(payload);
      const key = c.readString();
      const flags = c.readU8();
      const ttl = flags & 0x01 ? c.readU64() : null;
      const value = c.buf.subarray(c.offset);
      c.offset = c.buf.length;
      if (key === 'foo' && ttl === null && value.length === jsonBytes.length + 1 && c.offset === payload.length) ok++;
    }
    const elapsed = performance.now() - t0;
    const opsSec = N / (elapsed / 1000);
    console.log(`CODEC BENCH  store_set_json (med)  ${N.toString().padStart(7)} iter | ${opsSec.toFixed(0).padStart(12)} ops/sec | ${elapsed.toFixed(1)}ms`);
    expect(ok).toBe(N);
  });

  it('queue_push_batch (large)', () => {
    let ok = 0;
    const t0 = performance.now();
    for (let i = 0; i < N; i++) {
      const w = new FrameWriter();
      w.begin();
      w.string('emails');
      w.u32(100);
      for (let j = 0; j < 100; j++) {
        w.u8(0x01); // has priority
        w.u8(j);
        w.u32(5);
        w.bytes(Buffer.from('hello'));
      }
      const frame = w.finish(1, 0x11, FrameType.REQUEST);
      const payload = frame.subarray(HEADER_SIZE);
      const c = new Cursor(payload);
      const qName = c.readString();
      const count = c.readU32();
      for (let j = 0; j < count; j++) {
        const flags = c.readU8();
        if (flags & 0x01) c.readU8();
        const len = c.readU32();
        c.readBuffer(len);
      }
      if (qName === 'emails' && count === 100 && c.offset === payload.length) ok++;
    }
    const elapsed = performance.now() - t0;
    const opsSec = N / (elapsed / 1000);
    console.log(`CODEC BENCH  queue_push_batch (lg) ${N.toString().padStart(7)} iter | ${opsSec.toFixed(0).padStart(12)} ops/sec | ${elapsed.toFixed(1)}ms`);
    expect(ok).toBe(N);
  });
});
