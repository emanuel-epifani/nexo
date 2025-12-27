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

interface ResponseFrame {
  id: number;
  status: ResponseStatus;
  data: Buffer;
}

// --- STREAM DECODER (Zero-Waste TCP Accumulator) ---

class StreamDecoder {
  private chunks: Buffer[] = [];
  private totalLength: number = 0;

  push(chunk: Buffer) {
    this.chunks.push(chunk);
    this.totalLength += chunk.length;
  }

  nextFrame(): { type: number, id: number, payload: Buffer } | null {
    if (this.totalLength < 9) return null;

    let type: number;
    let id: number;
    let payloadLen: number;

    const first = this.chunks[0];
    if (first.length >= 9) {
      type = first[0];
      id = first.readUInt32BE(1);
      payloadLen = first.readUInt32BE(5);
    } else {
      const head = Buffer.concat(this.chunks, 9);
      type = head[0];
      id = head.readUInt32BE(1);
      payloadLen = head.readUInt32BE(5);
    }

    const totalLen = 9 + payloadLen;
    if (this.totalLength < totalLen) return null;

    const fullBuffer = this.chunks.length === 1 ? this.chunks[0] : Buffer.concat(this.chunks);
    const frame = {
      type,
      id,
      payload: fullBuffer.subarray(9, totalLen)
    };

    const remainingLen = this.totalLength - totalLen;
    if (remainingLen > 0) {
      this.chunks = [fullBuffer.subarray(totalLen)];
      this.totalLength = remainingLen;
    } else {
      this.chunks = [];
      this.totalLength = 0;
    }

    return frame;
  }
}

// --- CLIENT IMPLEMENTATION ---

export interface NexoOptions {
  host?: string;
  port?: number;
}

export class NexoClient {
  private socket: net.Socket;
  private isConnected: boolean = false;
  private host: string;
  private port: number;

  private decoder = new StreamDecoder();
  private nextId = 1;
  private pendingRequests = new Map<number, { resolve: Function, reject: Function }>();
  private isCorked = false;

  constructor(options: NexoOptions = {}) {
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 8080;
    this.socket = new net.Socket();
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
          const req = this.pendingRequests.get(frame.id);
          if (req) {
            const status = frame.payload[0] as ResponseStatus;
            const data = frame.payload.subarray(1);
            req.resolve({ status, data });
            this.pendingRequests.delete(frame.id);
          }
        }
      }
    });

    this.socket.on('error', (err) => {
      this.isConnected = false;
      for (const [id, req] of this.pendingRequests) {
        req.reject(err);
        this.pendingRequests.delete(id);
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

  private async send(opcode: number, payloadBody: Buffer): Promise<{ status: ResponseStatus, data: Buffer }> {
    if (!this.isConnected) throw new Error('Client not connected');

    const id = this.nextId++;
    const payloadLen = 1 + payloadBody.length;
    const totalSize = 9 + payloadLen;

    const message = Buffer.allocUnsafe(totalSize);
    message.writeUInt8(FrameType.REQUEST, 0);
    message.writeUInt32BE(id, 1);
    message.writeUInt32BE(payloadLen, 5);
    message.writeUInt8(opcode, 9);
    payloadBody.copy(message, 10);

    return new Promise((resolve, reject) => {
      this.pendingRequests.set(id, { resolve, reject });

      if (!this.isCorked) {
        this.socket.cork();
        this.isCorked = true;
        process.nextTick(() => {
          this.socket.uncork();
          this.isCorked = false;
        });
      }

      this.socket.write(message);
    });
  }

  disconnect(): void {
    this.socket.end();
    this.socket.destroy();
    this.isConnected = false;
  }

  // --- BROKERS ---

  public readonly kv = {
    set: async (key: string, value: string | Buffer, ttlSeconds: number = 0): Promise<void> => {
      const keyBuf = Buffer.from(key, 'utf8');
      const valBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');

      // KV_SET Payload: [TTL:8][KeyLen:4][Key][Value]
      const kvPayload = Buffer.allocUnsafe(8 + 4 + keyBuf.length + valBuf.length);

      // Write TTL (uint64 Big Endian)
      kvPayload.writeBigUInt64BE(BigInt(ttlSeconds), 0);

      // Write Key Length (uint32 Big Endian)
      kvPayload.writeUInt32BE(keyBuf.length, 8);

      // Copy Key
      keyBuf.copy(kvPayload, 12);

      // Copy Value
      valBuf.copy(kvPayload, 12 + keyBuf.length);

      const res = await this.send(Opcode.KV_SET, kvPayload);
      if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
    },

    get: async (key: string): Promise<Buffer | null> => {
      const keyBuf = Buffer.from(key, 'utf8');
      const payload = Buffer.allocUnsafe(4 + keyBuf.length);
      payload.writeUInt32BE(keyBuf.length, 0);
      keyBuf.copy(payload, 4);

      const res = await this.send(Opcode.KV_GET, payload);

      if (res.status === ResponseStatus.NULL) return null;
      if (res.status === ResponseStatus.DATA) return res.data;
      if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
      return null;
    },

    del: async (key: string): Promise<void> => {
      const keyBuf = Buffer.from(key, 'utf8');
      const payload = Buffer.allocUnsafe(4 + keyBuf.length);
      payload.writeUInt32BE(keyBuf.length, 0);
      keyBuf.copy(payload, 4);

      const res = await this.send(Opcode.KV_DEL, payload);
      if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
    }
  };
}
