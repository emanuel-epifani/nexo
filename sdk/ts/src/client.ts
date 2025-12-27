import * as net from 'net';

// --- PROTOCOL DEFINITIONS ---

enum FrameType {
  REQUEST = 0x01,
  RESPONSE = 0x02,
  PUSH = 0x03,
  ERROR = 0x04,
  PING = 0x05,
  PONG = 0x06,
}

enum ResponseStatus {
  OK = 0x00,
  ERR = 0x01,
  NULL = 0x02,
  DATA = 0x03,
}

enum Opcode {
  // KV commands (0x02-0x0F)
  KV_SET = 0x02,
  KV_GET = 0x03,
  KV_DEL = 0x04,

  // Queue commands (0x10-0x1F)
  Q_PUSH = 0x11,
  Q_POP = 0x12,

  // Topic commands (0x20-0x2F)
  PUB = 0x21,
  SUB = 0x22,

  // Stream commands (0x30-0x3F)
  S_ADD = 0x31,
  S_READ = 0x32,
}

// --- RING BUFFER DECODER (Zero-Copy, Pre-allocated) ---

class RingDecoder {
  private buf: Buffer;
  private head = 0;  // Read position
  private tail = 0;  // Write position

  constructor(size = 512 * 1024) {
    this.buf = Buffer.allocUnsafe(size);
  }

  push(chunk: Buffer): void {
    const needed = chunk.length;
    const available = this.buf.length - this.tail;

    // Compact if not enough space at end
    if (available < needed) {
      const used = this.tail - this.head;
      if (used > 0) {
        this.buf.copy(this.buf, 0, this.head, this.tail);
      }
      this.tail = used;
      this.head = 0;
    }

    chunk.copy(this.buf, this.tail);
    this.tail += needed;
  }

  nextFrame(): { type: number; id: number; payload: Buffer } | null {
    const available = this.tail - this.head;
    if (available < 9) return null;

    const type = this.buf[this.head];
    const id = this.buf.readUInt32BE(this.head + 1);
    const payloadLen = this.buf.readUInt32BE(this.head + 5);
    const totalLen = 9 + payloadLen;

    if (available < totalLen) return null;

    // subarray is zero-copy - shares memory with ring buffer
    const payload = this.buf.subarray(this.head + 9, this.head + totalLen);
    this.head += totalLen;

    // Reset positions when buffer is empty (keeps memory locality)
    if (this.head === this.tail) {
      this.head = 0;
      this.tail = 0;
    }

    return { type, id, payload };
  }
}

// --- CLIENT IMPLEMENTATION ---

export interface NexoOptions {
  host?: string;
  port?: number;
}

// Pending request handler type
type PendingHandler = { resolve: (v: { status: ResponseStatus; data: Buffer }) => void; reject: (e: Error) => void };

// ID space constants
const ID_MASK = 0xFFFF;      // 65535 max concurrent requests
const ID_SPACE = ID_MASK + 1; // 65536

export class NexoClient {
  private socket: net.Socket;
  private isConnected = false;
  private host: string;
  private port: number;

  // Optimized decoder
  private decoder = new RingDecoder();

  // ID management with wrap-around
  private nextId = 1;

  // Sparse array for pending requests (faster than Map for sequential IDs)
  private pending: (PendingHandler | undefined)[] = new Array(ID_SPACE);

  // Batch write buffer
  private writeBuf: Buffer;
  private writeOffset = 0;
  private flushScheduled = false;

  constructor(options: NexoOptions = {}) {
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 8080;
    this.socket = new net.Socket();
    this.socket.setNoDelay(true); // Disable Nagle for lower latency
    this.writeBuf = Buffer.allocUnsafe(64 * 1024);
    this.setupSocketListeners();
  }

  static async connect(options: NexoOptions = {}): Promise<NexoClient> {
    const client = new NexoClient(options);
    await client.connect();
    return client;
  }

  private setupSocketListeners() {
    this.socket.on('data', (chunk) => {
      this.decoder.push(chunk);
      let frame;
      while ((frame = this.decoder.nextFrame())) {
        if (frame.type === FrameType.RESPONSE) {
          const handler = this.pending[frame.id];
          if (handler) {
            const status = frame.payload[0] as ResponseStatus;
            // Copy data since payload references ring buffer (will be overwritten)
            const data = Buffer.from(frame.payload.subarray(1));
            handler.resolve({ status, data });
            this.pending[frame.id] = undefined;
          }
        }
      }
    });

    this.socket.on('error', (err) => {
      this.isConnected = false;
      // Reject all pending
      for (let i = 0; i < ID_SPACE; i++) {
        const handler = this.pending[i];
        if (handler) {
          handler.reject(err as Error);
          this.pending[i] = undefined;
        }
      }
    });

    this.socket.on('close', () => {
      this.isConnected = false;
    });
  }

  async connect(): Promise<void> {
    if (this.isConnected) return;
    return new Promise((resolve, reject) => {
      this.socket.connect(this.port, this.host, () => {
        this.isConnected = true;
        resolve();
      });
      this.socket.once('error', (err) => {
        if (!this.isConnected) reject(err);
      });
    });
  }

  private flushBatch = (): void => {
    this.flushScheduled = false;
    if (this.writeOffset === 0) return;

    // Write entire batch in one syscall
    this.socket.write(this.writeBuf.subarray(0, this.writeOffset));
    this.writeOffset = 0;
  };

  private send(opcode: number, payloadBody: Buffer): Promise<{ status: ResponseStatus; data: Buffer }> {
    if (!this.isConnected) return Promise.reject(new Error('Client not connected'));

    // Wrap ID to stay within array bounds
    const id = this.nextId;
    this.nextId = (this.nextId + 1) & ID_MASK;
    if (this.nextId === 0) this.nextId = 1; // Skip 0

    const payloadLen = 1 + payloadBody.length;
    const totalSize = 9 + payloadLen;

    // Check if batch buffer needs flush
    if (this.writeOffset + totalSize > this.writeBuf.length) {
      // Sync flush for large messages
      if (this.writeOffset > 0) {
        this.socket.write(this.writeBuf.subarray(0, this.writeOffset));
        this.writeOffset = 0;
      }
      // If single message is too large, write directly
      if (totalSize > this.writeBuf.length) {
        const msg = Buffer.allocUnsafe(totalSize);
        msg[0] = FrameType.REQUEST;
        msg.writeUInt32BE(id, 1);
        msg.writeUInt32BE(payloadLen, 5);
        msg[9] = opcode;
        payloadBody.copy(msg, 10);
        this.socket.write(msg);

        return new Promise((resolve, reject) => {
          this.pending[id] = { resolve, reject };
        });
      }
    }

    // Write to batch buffer (zero allocation path)
    const buf = this.writeBuf;
    let off = this.writeOffset;

    buf[off] = FrameType.REQUEST;
    buf.writeUInt32BE(id, off + 1);
    buf.writeUInt32BE(payloadLen, off + 5);
    buf[off + 9] = opcode;
    payloadBody.copy(buf, off + 10);
    this.writeOffset = off + totalSize;

    // Schedule single flush per event loop tick
    if (!this.flushScheduled) {
      this.flushScheduled = true;
      setImmediate(this.flushBatch);
    }

    return new Promise((resolve, reject) => {
      this.pending[id] = { resolve, reject };
    });
  }

  disconnect(): void {
    // Flush any pending writes
    if (this.writeOffset > 0) {
      this.socket.write(this.writeBuf.subarray(0, this.writeOffset));
      this.writeOffset = 0;
    }
    this.socket.end();
    this.socket.destroy();
    this.isConnected = false;
  }

  // --- BROKERS ---

  public readonly kv = {
    set: (key: string, value: string | Buffer, ttlSeconds = 0): Promise<void> => {
      const keyLen = Buffer.byteLength(key, 'utf8');
      const valBuf = typeof value === 'string' ? Buffer.from(value, 'utf8') : value;

      // KV_SET Payload: [TTL:8][KeyLen:4][Key][Value]
      const payloadSize = 8 + 4 + keyLen + valBuf.length;
      const payload = Buffer.allocUnsafe(payloadSize);

      // Write TTL + KeyLen in one go
      payload.writeBigUInt64BE(BigInt(ttlSeconds), 0);
      payload.writeUInt32BE(keyLen, 8);

      // Write key directly (avoids intermediate buffer)
      payload.write(key, 12, 'utf8');

      // Copy value
      valBuf.copy(payload, 12 + keyLen);

      return this.send(Opcode.KV_SET, payload).then((res) => {
        if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
      });
    },

    get: (key: string): Promise<Buffer | null> => {
      const keyLen = Buffer.byteLength(key, 'utf8');
      const payload = Buffer.allocUnsafe(4 + keyLen);
      payload.writeUInt32BE(keyLen, 0);
      payload.write(key, 4, 'utf8');

      return this.send(Opcode.KV_GET, payload).then((res) => {
        if (res.status === ResponseStatus.NULL) return null;
        if (res.status === ResponseStatus.DATA) return res.data;
        if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
        return null;
      });
    },

    del: (key: string): Promise<void> => {
      const keyLen = Buffer.byteLength(key, 'utf8');
      const payload = Buffer.allocUnsafe(4 + keyLen);
      payload.writeUInt32BE(keyLen, 0);
      payload.write(key, 4, 'utf8');

      return this.send(Opcode.KV_DEL, payload).then((res) => {
        if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
      });
    },
  };
}

/*
Ring Buffer: Evita Buffer.concat() che copiava tutti i dati ad ogni frame
Batch writes: Accumula messaggi in un buffer e li invia in una sola syscall per tick
Array vs Map: Lookup pending[id] è O(1) senza overhead di hashing
setImmediate vs nextTick: Migliore per I/O bound, non blocca altri callback
 */
