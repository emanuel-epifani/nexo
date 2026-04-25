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
    const s = this.buf.toString('utf8', this.offset, this.offset + len);
    this.offset += len;
    return s;
  }

  readUUID(): string {
    const s = this.buf.toString('hex', this.offset, this.offset + 16);
    this.offset += 16;
    return s;
  }

  /**
   * Decode a trailing `any` value (DataType-prefixed) from the current position
   * to the end of the buffer.
   */
  decodeAny(): any {
    const type = this.buf.readUInt8(this.offset++);
    const start = this.offset;
    const end = this.buf.length;
    this.offset = end;
    switch (type) {
      case DataType.JSON:
        return start === end ? null : JSON.parse(this.buf.toString('utf8', start, end));
      case DataType.STRING:
        return this.buf.toString('utf8', start, end);
      case DataType.RAW:
      default:
        return this.buf.subarray(start, end);
    }
  }
}

/**
 * Single-pass frame writer. Stages no per-arg `Buffer`s: each `u8/u32/u64/string/
 * uuid/any` writes inline into a growable internal buffer that is then returned
 * (zero-copy `subarray`) by `finish()`.
 *
 * One instance per connection is reused via `begin()` (which allocates a fresh
 * internal buffer each call — required because `socket.write` retains a
 * reference to the previously returned buffer until drained).
 *
 * @internal
 */
export class FrameWriter {
  private buf!: Buffer;
  private offset = 10;

  /**
   * Start a new frame. Allocates a fresh internal buffer; default initial size
   * covers ~99% of broker commands without a grow.
   */
  begin(initialSize = 256): this {
    this.buf = Buffer.allocUnsafe(initialSize);
    this.offset = 10;
    return this;
  }

  private ensure(extra: number): void {
    const need = this.offset + extra;
    if (need <= this.buf.length) return;
    let next = this.buf.length * 2;
    while (next < need) next *= 2;
    const grown = Buffer.allocUnsafe(next);
    this.buf.copy(grown, 0, 0, this.offset);
    this.buf = grown;
  }

  u8(v: number): this {
    this.ensure(1);
    this.buf.writeUInt8(v, this.offset);
    this.offset += 1;
    return this;
  }

  u32(v: number): this {
    this.ensure(4);
    this.buf.writeUInt32BE(v, this.offset);
    this.offset += 4;
    return this;
  }

  u64(v: bigint | number): this {
    this.ensure(8);
    this.buf.writeBigUInt64BE(typeof v === 'bigint' ? v : BigInt(v), this.offset);
    this.offset += 8;
    return this;
  }

  string(s: string): this {
    const len = Buffer.byteLength(s, 'utf8');
    this.ensure(4 + len);
    this.buf.writeUInt32BE(len, this.offset);
    this.offset += 4;
    if (len > 0) this.buf.write(s, this.offset, len, 'utf8');
    this.offset += len;
    return this;
  }

  /**
   * Write a 16-byte UUID parsing hex inline. Accepts both canonical (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)
   * and dashless 32-char hex. No regex, no intermediate Buffer.
   */
  uuid(hex: string): this {
    this.ensure(16);
    const buf = this.buf;
    let off = this.offset;
    let high = 0;
    let highSet = false;
    for (let i = 0; i < hex.length; i++) {
      const c = hex.charCodeAt(i);
      if (c === 0x2D /* '-' */) continue;
      let nib: number;
      if (c >= 0x30 && c <= 0x39) nib = c - 0x30;
      else if (c >= 0x61 && c <= 0x66) nib = c - 0x57;
      else if (c >= 0x41 && c <= 0x46) nib = c - 0x37;
      else throw new Error(`Invalid UUID hex char at ${i}: ${hex[i]}`);
      if (!highSet) { high = nib << 4; highSet = true; }
      else { buf[off++] = high | nib; highSet = false; }
    }
    this.offset = off;
    return this;
  }

  any(data: unknown): this {
    if (Buffer.isBuffer(data)) {
      this.ensure(1 + data.length);
      this.buf.writeUInt8(DataType.RAW, this.offset++);
      data.copy(this.buf, this.offset);
      this.offset += data.length;
    } else if (typeof data === 'string') {
      const len = Buffer.byteLength(data, 'utf8');
      this.ensure(1 + len);
      this.buf.writeUInt8(DataType.STRING, this.offset++);
      if (len > 0) this.buf.write(data, this.offset, len, 'utf8');
      this.offset += len;
    } else {
      const json = JSON.stringify(data ?? null);
      const len = Buffer.byteLength(json, 'utf8');
      this.ensure(1 + len);
      this.buf.writeUInt8(DataType.JSON, this.offset++);
      this.buf.write(json, this.offset, len, 'utf8');
      this.offset += len;
    }
    return this;
  }

  /**
   * Finalize the frame: writes the 10-byte header in-place and returns a
   * zero-copy view of the populated bytes. After calling this, the writer must
   * be re-`begin()`ed before reuse.
   */
  finish(id: number, opcode: number): Buffer {
    const total = this.offset;
    this.buf.writeUInt8(FrameType.REQUEST, 0);
    this.buf.writeUInt8(opcode, 1);
    this.buf.writeUInt32BE(id, 2);
    this.buf.writeUInt32BE(total - 10, 6);
    return this.buf.subarray(0, total);
  }
}
