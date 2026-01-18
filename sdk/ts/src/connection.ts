import * as net from 'net';
import { logger } from './utils/logger';
import { FrameType, ResponseStatus, Opcode, NexoOptions } from './protocol';
import { Cursor, FrameCodec } from './codec';

export class NexoConnection {
  private socket: net.Socket;
  public isConnected = false;
  private nextId = 1;
  private pending = new Map<number, (res: { status: number, data: Buffer }) => void>();
  private requestTimeoutMs: number;
  
  public onPush?: (topic: string, data: any) => void;

  // Simple Buffering
  private buffer: Buffer = Buffer.alloc(0);

  constructor(private host: string, private port: number, options: NexoOptions = {}) {
    this.socket = new net.Socket();
    this.socket.setNoDelay(true); // Low latency
    this.requestTimeoutMs = options.requestTimeoutMs ?? 10000;
    this.setupListeners();
  }

  async connect(): Promise<void> {
    return new Promise((res, rej) => {
      this.socket.connect(this.port, this.host, () => {
        this.isConnected = true; res();
      });
      this.socket.once('error', rej);
    });
  }

  private setupListeners() {
    this.socket.on('data', (chunk) => {
      // 1. Easy Append
      this.buffer = Buffer.concat([this.buffer, chunk]);
      this.processBuffer();
    });

    const cleanup = (err: any) => {
      this.isConnected = false;
      this.pending.clear();
    };

    this.socket.on('error', (err) => logger.error("Socket error", err));
    this.socket.on('close', cleanup);
  }

  private processBuffer() {
    while (true) {
      // Need at least header (9 bytes)
      if (this.buffer.length < 9) break;

      const payloadLen = this.buffer.readUInt32BE(5);
      const totalFrameLen = 9 + payloadLen;

      if (this.buffer.length < totalFrameLen) break;

      // 2. Slice Frame
      const frame = this.buffer.subarray(0, totalFrameLen);
      this.buffer = this.buffer.subarray(totalFrameLen); // Advance buffer

      this.handleFrame(frame);
    }
  }

  private handleFrame(frame: Buffer) {
    const cursor = new Cursor(frame);
    const type = cursor.readU8();
    const id = cursor.readU32();
    cursor.readU32(); // Skip payloadLen

    const payload = cursor.buf.subarray(cursor.offset);

    switch (type) {
      case FrameType.RESPONSE: {
        const resolve = this.pending.get(id);
        if (resolve) {
          this.pending.delete(id);
          // Payload: [Status:1][Data...]
          resolve({ status: payload[0], data: payload.subarray(1) });
        }
        break;
      }
      case FrameType.PUSH: {
        if (this.onPush) {
          const pushCursor = new Cursor(payload);
          const topic = pushCursor.readString();
          const data = FrameCodec.decodeAny(pushCursor);
          this.onPush(topic, data);
        }
        break;
      }
      case FrameType.ERROR:
        logger.error('Received Protocol Error');
        break;
      case FrameType.PING:
        // Optional: Send PONG
        break;
    }
  }

  send(opcode: Opcode, ...args: Buffer[]): Promise<{ status: ResponseStatus, cursor: Cursor }> {
    const id = this.nextId++;
    if (this.nextId === 0) this.nextId = 1;

    const packet = FrameCodec.packRequest(id, opcode, ...args);

    return new Promise((resolve, reject) => {
      // 1. Setup Timeout
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Request timeout after ${this.requestTimeoutMs}ms`));
      }, this.requestTimeoutMs);

      // 2. Register Callback
      this.pending.set(id, (res) => {
        clearTimeout(timer);
        if (res.status === ResponseStatus.ERR) {
          const errCursor = new Cursor(res.data);
          const errMsg = errCursor.readString();
          // Silence common expected errors
          if (!errMsg.includes('FENCED') && !errMsg.includes('REBALANCE') && !errMsg.includes('not found')) {
            logger.error(`<- ERROR ${Opcode[opcode]} (${errMsg})`);
          }
          reject(new Error(errMsg));
        } else {
          resolve({ status: res.status, cursor: new Cursor(res.data) });
        }
      });

      // 3. Send
      if (!this.isConnected) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(new Error("Client not connected"));
        return;
      }
      this.socket.write(packet);
    });
  }

  disconnect() {
    this.socket.destroy();
    this.isConnected = false;
  }
}
