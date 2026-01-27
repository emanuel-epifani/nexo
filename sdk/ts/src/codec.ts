import { DataType, FrameType } from './protocol';

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
    const result = Buffer.allocUnsafe(4 + strBuf.length);
    result.writeUInt32BE(strBuf.length, 0);
    strBuf.copy(result, 4);
    return result;
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

    const result = Buffer.allocUnsafe(1 + payload.length);
    result.writeUInt8(type, 0);
    payload.copy(result, 1);
    return result;
  }

  static packRequest(id: number, opcode: number, ...parts: Buffer[]): Buffer {
    // 1. Calcolo dimensione PAYLOAD (Opcode [1 byte] + Argomenti)
    let payloadSize = 1;
    for (const part of parts) payloadSize += part.length;

    // 2. Allocazione buffer TOTALE (Header [9 bytes] + Payload)
    // Header è sempre fisso a 9 bytes: [Type:1][ID:4][Len:4]
    const result = Buffer.allocUnsafe(9 + payloadSize);

    // --- SCRITTURA HEADER (Bytes 0-8) ---
    // Byte 0: Tipo di Frame (REQUEST = 0x01)
    result.writeUInt8(FrameType.REQUEST, 0);
    // Bytes 1-4: ID della richiesta (UInt32 Big Endian)
    result.writeUInt32BE(id, 1);
    // Bytes 5-8: Lunghezza del Payload (UInt32 Big Endian)
    // Questo dice al server quanti byte leggere dopo l'header
    result.writeUInt32BE(payloadSize, 5);

    // --- SCRITTURA PAYLOAD (Bytes 9+) ---
    // Byte 9: Opcode (Il primo byte del payload è sempre l'operazione)
    result.writeUInt8(opcode, 9);
    // Bytes 10+: Copia degli argomenti (dati effettivi)
    let offset = 10;
    for (const part of parts) {
      part.copy(result, offset);
      offset += part.length;
    }

    return result;
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
