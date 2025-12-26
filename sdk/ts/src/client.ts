import * as net from 'net';

// --- PROTOCOL DEFINITIONS ---

enum Opcode {
  PING = 0x01,
  
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

enum ResponseStatus {
  OK = 0x00,
  ERR = 0x01,
  NULL = 0x02,
  DATA = 0x03,
}

interface ResponseFrame {
  status: ResponseStatus;
  data: Buffer;
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

  constructor(options: NexoOptions = {}) {
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 8080;
    this.socket = new net.Socket();
  }

  /**
   * Static factory to create and connect a client in one line.
   */
  static async connect(options: NexoOptions = {}): Promise<NexoClient> {
    const client = new NexoClient(options);
    await client.connect();
    return client;
  }

  /**
   * Connects to the Nexo broker.
   */
  async connect(): Promise<void> {
    if (this.isConnected) return;

    return new Promise((resolve, reject) => {
      this.socket.connect(this.port, this.host, () => {
        this.isConnected = true;
        resolve();
      });

      this.socket.on('error', (err) => {
        if (!this.isConnected) reject(err);
      });

      this.socket.on('close', () => {
        this.isConnected = false;
      });
    });
  }

  /**
   * Internal sender: handles binary framing [Opcode:1][Len:4][Payload:N]
   * Hot path optimized: no connection checks here.
   */
  private async send(opcode: number, payload: Buffer): Promise<ResponseFrame> {
    return new Promise((resolve, reject) => {
      const header = Buffer.alloc(5);
      header.writeUInt8(opcode, 0);
      header.writeUInt32BE(payload.length, 1);

      this.socket.write(Buffer.concat([header, payload]));

      const onData = (data: Buffer) => {
        this.socket.removeListener('data', onData);
        if (data.length < 5) {
          reject(new Error('Invalid response header'));
          return;
        }

        const status = data.readUInt8(0) as ResponseStatus;
        const bodyLen = data.readUInt32BE(1);
        const body = data.subarray(5, 5 + bodyLen);

        resolve({ status, data: body });
      };

      this.socket.once('data', onData);
      this.socket.once('error', (err) => {
        this.socket.removeListener('data', onData);
        reject(err);
      });
    });
  }

  /**
   * Disconnects from the Nexo broker.
   */
  disconnect(): void {
    this.socket.end();
    this.socket.destroy();
    this.isConnected = false;
  }

  // --- KV MODULE ---
  public readonly kv = {
    set: async (key: string, value: string | Buffer): Promise<void> => {
      const keyBuf = Buffer.from(key, 'utf8');
      const valBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');
      const payload = Buffer.alloc(4 + keyBuf.length + valBuf.length);
      payload.writeUInt32BE(keyBuf.length, 0);
      keyBuf.copy(payload, 4);
      valBuf.copy(payload, 4 + keyBuf.length);

      const res = await this.send(Opcode.KV_SET, payload);
      if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
    },

    get: async (key: string): Promise<Buffer | null> => {
      const res = await this.send(Opcode.KV_GET, Buffer.from(key, 'utf8'));
      if (res.status === ResponseStatus.NULL) return null;
      if (res.status === ResponseStatus.DATA) return res.data;
      if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
      return null;
    },

    del: async (key: string): Promise<void> => {
      const res = await this.send(Opcode.KV_DEL, Buffer.from(key, 'utf8'));
      if (res.status === ResponseStatus.ERR) throw new Error(res.data.toString());
    }
  };

}
