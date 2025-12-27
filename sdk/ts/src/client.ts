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

// --- STREAM DECODER (Optimized TCP Accumulator) ---

class StreamDecoder {
  private chunks: Buffer[] = [];
  private totalLength: number = 0;

  push(chunk: Buffer) {
    this.chunks.push(chunk);
    this.totalLength += chunk.length;
  }

  nextFrame(): { type: number, id: number, payload: Buffer } | null {
    // 1. Need at least 9 bytes for the header
    if (this.totalLength < 9) return null;

    // 2. Ensure contiguous buffer for header reading
    if (this.chunks.length > 1) {
      this.chunks = [Buffer.concat(this.chunks)];
    }

    const buffer = this.chunks[0];
    const payloadLen = buffer.readUInt32BE(5);
    const totalLen = 9 + payloadLen;

    // 3. Need the full packet
    if (this.totalLength < totalLen) return null;

    // 4. Extract frame data
    const frame = {
      type: buffer[0],
      id: buffer.readUInt32BE(1),
      payload: buffer.subarray(9, totalLen)
    };

    // 5. Efficiently update state without unnecessary copies
    const remaining = buffer.subarray(totalLen);
    if (remaining.length > 0) {
      this.chunks = [remaining];
      this.totalLength = remaining.length;
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
        // TODO: Handle PUSH frames (FrameType.PUSH)
      }
    });

    this.socket.on('error', (err) => {
      this.isConnected = false;
      // Reject all pending requests on connection error
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

  /**
   * Universal sender with single-allocation and multiplexing support.
   */
  private async send(opcode: number, payloadBody: Buffer): Promise<{ status: ResponseStatus, data: Buffer }> {
    if (!this.isConnected) throw new Error('Client not connected');

    const id = this.nextId++;
    const payloadLen = 1 + payloadBody.length; // Opcode (1) + Body
    const totalSize = 9 + payloadLen;          // Header (9) + Payload

    // SINGLE ALLOCATION: Much faster than multiple Buffer.concat calls
    const message = Buffer.allocUnsafe(totalSize);

    // 1. Write Header: [Type:1][ID:4][Len:4]
    message.writeUInt8(FrameType.REQUEST, 0);
    message.writeUInt32BE(id, 1);
    message.writeUInt32BE(payloadLen, 5);

    // 2. Write Application Payload: [Opcode:1][Body:N]
    message.writeUInt8(opcode, 9);
    payloadBody.copy(message, 10);

    return new Promise((resolve, reject) => {
      this.pendingRequests.set(id, { resolve, reject });
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
    set: async (key: string, value: string | Buffer): Promise<void> => {
      const keyBuf = Buffer.from(key, 'utf8');
      const valBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');

      // KV_SET Payload: [KeyLen:4][Key][Value]
      const kvPayload = Buffer.allocUnsafe(4 + keyBuf.length + valBuf.length);
      kvPayload.writeUInt32BE(keyBuf.length, 0);
      keyBuf.copy(kvPayload, 4);
      valBuf.copy(kvPayload, 4 + keyBuf.length);

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
