import { DataType, FrameType, Opcode } from './protocol';

/** @internal */
export class Cursor {
  constructor(public buf: Buffer, public offset = 0) { }

  readU8(): number { return this.buf.readUInt8(this.offset++); }
  readU32(): number { const v = this.buf.readUInt32BE(this.offset); this.offset += 4; return v; }
  readU64(): bigint { const v = this.buf.readBigUInt64BE(this.offset); this.offset += 8; return v; }
  
  readBuffer(len: number): Buffer {
    const v = this.buf.subarray(this.offset, this.offset + len);
    this.offset += len;
    return v;
  }

  readString(): string {
    const len = this.readU32();
    return this.readBuffer(len).toString('utf8');
  }

  readUUID(): string {
    return this.readBuffer(16).toString('hex');
  }
}

/** @internal */
export class FrameCodec {
  // --- ENCODERS (Data -> Buffer) ---

  static u8(v: number): Buffer {
    const b = Buffer.allocUnsafe(1);
    b.writeUInt8(v, 0);
    return b;
  }

  static u32(v: number): Buffer {
    const b = Buffer.allocUnsafe(4);
    b.writeUInt32BE(v, 0);
    return b;
  }

  static u64(v: number | bigint): Buffer {
    const b = Buffer.allocUnsafe(8);
    b.writeBigUInt64BE(BigInt(v), 0);
    return b;
  }

  static string(s: string): Buffer {
    const strBuf = Buffer.from(s, 'utf8');
    // [Len:4][Bytes...]
    return Buffer.concat([this.u32(strBuf.length), strBuf]);
  }

  static uuid(hex: string): Buffer {
    return Buffer.from(hex, 'hex');
  }

  static any(data: any): Buffer {
    let type = DataType.RAW;
    let payload: Buffer;

    if (Buffer.isBuffer(data)) {
      type = DataType.RAW;
      payload = data;
    } else if (typeof data === 'string') {
      type = DataType.STRING;
      payload = Buffer.from(data, 'utf8');
    } else {
      type = DataType.JSON;
      payload = Buffer.from(JSON.stringify(data ?? null), 'utf8');
    }

    // [Type:1][Payload...]
    return Buffer.concat([this.u8(type), payload]);
  }

  static packRequest(id: number, opcode: Opcode, ...parts: Buffer[]): Buffer {
    const payload = Buffer.concat([this.u8(opcode), ...parts]);
    const header = Buffer.allocUnsafe(9);
    
    // Header: [Type:1][ID:4][PayloadLen:4]
    header.writeUInt8(FrameType.REQUEST, 0);
    header.writeUInt32BE(id, 1);
    header.writeUInt32BE(payload.length, 5);

    return Buffer.concat([header, payload]);
  }

  // --- DECODERS (Buffer -> Data) ---

  static decodeAny(cursor: Cursor): any {
    const type = cursor.readU8();
    const content = cursor.buf.subarray(cursor.offset); // Read until end
    // Note: In strict framing, we might want to pass length, but here 'Any' is usually trailing

    switch (type) {
      case DataType.JSON: return content.length ? JSON.parse(content.toString('utf8')) : null;
      case DataType.STRING: return content.toString('utf8');
      case DataType.RAW: default: return content;
    }
  }
}
