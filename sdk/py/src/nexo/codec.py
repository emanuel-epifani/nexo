from __future__ import annotations

import json
import struct
from typing import Any, Callable, Union

from .protocol import (
    DataType,
    FrameType,
    PROTOCOL_VERSION,
    HEADER_SIZE,
    HEADER_OFFSET_VERSION,
    HEADER_OFFSET_TYPE,
    HEADER_OFFSET_META,
    HEADER_OFFSET_ID,
    HEADER_OFFSET_PAYLOAD_LEN,
)

# ─── Cursor ──────────────────────────────────────────────────────────────────

class Cursor:
    __slots__ = ("buf", "offset")

    def __init__(self, buf: bytes, offset: int = 0) -> None:
        self.buf = buf
        self.offset = offset

    def read_u8(self) -> int:
        v = self.buf[self.offset]
        self.offset += 1
        return v

    def read_u16(self) -> int:
        v = struct.unpack_from(">H", self.buf, self.offset)[0]
        self.offset += 2
        return v

    def read_u32(self) -> int:
        v = struct.unpack_from(">I", self.buf, self.offset)[0]
        self.offset += 4
        return v

    def read_u64(self) -> int:
        v = struct.unpack_from(">Q", self.buf, self.offset)[0]
        self.offset += 8
        return v

    def read_i64(self) -> int:
        v = struct.unpack_from(">q", self.buf, self.offset)[0]
        self.offset += 8
        return v

    def read_buffer(self, length: int) -> bytes:
        v = self.buf[self.offset : self.offset + length]
        self.offset += length
        return v

    def read_string(self) -> str:
        length = self.read_u32()
        s = self.buf[self.offset : self.offset + length].decode("utf-8")
        self.offset += length
        return s

    def read_uuid(self) -> str:
        s = self.buf[self.offset : self.offset + 16].hex()
        self.offset += 16
        return s

    def decode_any(self) -> Any:
        dtype = self.buf[self.offset]
        self.offset += 1
        start = self.offset
        end = len(self.buf)
        self.offset = end
        if dtype == DataType.INT:
            return struct.unpack_from(">q", self.buf, start)[0]
        elif dtype == DataType.JSON:
            if start == end:
                return None
            return json.loads(self.buf[start:end].decode("utf-8"))
        elif dtype == DataType.STRING:
            return self.buf[start:end].decode("utf-8")
        else:
            return self.buf[start:end]

    def decode_any_from_buffer(self, length: int) -> Any:
        dtype = self.buf[self.offset]
        self.offset += 1
        start = self.offset
        end = self.offset + (length - 1)
        self.offset = end
        if dtype == DataType.INT:
            return struct.unpack_from(">q", self.buf, start)[0]
        elif dtype == DataType.JSON:
            if start == end:
                return None
            return json.loads(self.buf[start:end].decode("utf-8"))
        elif dtype == DataType.STRING:
            return self.buf[start:end].decode("utf-8")
        else:
            return self.buf[start:end]


# ─── FrameWriter ─────────────────────────────────────────────────────────────

_BinaryLike = Union[bytes, bytearray, memoryview]


class FrameWriter:
    __slots__ = ("_buf", "_offset")

    def __init__(self) -> None:
        self._buf: bytearray = bytearray(256)
        self._offset: int = HEADER_SIZE

    def begin(self, initial_size: int = 256) -> "FrameWriter":
        self._buf = bytearray(initial_size)
        self._offset = HEADER_SIZE
        return self

    def _ensure(self, extra: int) -> None:
        need = self._offset + extra
        if need <= len(self._buf):
            return
        next_size = len(self._buf) * 2
        while next_size < need:
            next_size *= 2
        self._buf.extend(bytearray(next_size - len(self._buf)))

    def u8(self, v: int) -> "FrameWriter":
        self._ensure(1)
        struct.pack_into(">B", self._buf, self._offset, v)
        self._offset += 1
        return self

    def u16(self, v: int) -> "FrameWriter":
        self._ensure(2)
        struct.pack_into(">H", self._buf, self._offset, v)
        self._offset += 2
        return self

    def u32(self, v: int) -> "FrameWriter":
        if v < 0 or v > 0xFFFFFFFF or not isinstance(v, int):
            raise ValueError(f"u32 value out of range: {v}")
        self._ensure(4)
        struct.pack_into(">I", self._buf, self._offset, v)
        self._offset += 4
        return self

    def u64(self, v: int) -> "FrameWriter":
        if v < 0 or v > 0xFFFFFFFFFFFFFFFF:
            raise ValueError(f"u64 value out of range: {v}")
        self._ensure(8)
        struct.pack_into(">Q", self._buf, self._offset, v)
        self._offset += 8
        return self

    def i64(self, v: int) -> "FrameWriter":
        if v < -0x8000000000000000 or v > 0x7FFFFFFFFFFFFFFF:
            raise ValueError(f"i64 value out of range: {v}")
        self._ensure(8)
        struct.pack_into(">q", self._buf, self._offset, v)
        self._offset += 8
        return self

    def raw_bytes(self, v: _BinaryLike) -> "FrameWriter":
        if isinstance(v, memoryview):
            v = bytes(v)
        length = len(v)
        self._ensure(length)
        self._buf[self._offset : self._offset + length] = v
        self._offset += length
        return self

    def string(self, s: str) -> "FrameWriter":
        encoded = s.encode("utf-8")
        length = len(encoded)
        self._ensure(4 + length)
        struct.pack_into(">I", self._buf, self._offset, length)
        self._offset += 4
        if length > 0:
            self._buf[self._offset : self._offset + length] = encoded
        self._offset += length
        return self

    def uuid(self, hex_str: str) -> "FrameWriter":
        self._ensure(16)
        clean = hex_str.replace("-", "")
        if len(clean) != 32:
            raise ValueError(f"Invalid UUID hex length: {len(clean)}")
        self._buf[self._offset : self._offset + 16] = bytes.fromhex(clean)
        self._offset += 16
        return self

    def any(self, data: Any) -> "FrameWriter":
        if isinstance(data, (bytes, bytearray, memoryview)):
            if isinstance(data, memoryview):
                data = bytes(data)
            length = len(data)
            self._ensure(1 + length)
            self._buf[self._offset] = DataType.RAW
            self._offset += 1
            self._buf[self._offset : self._offset + length] = data
            self._offset += length
        elif isinstance(data, bool):
            json_bytes = json.dumps(data, separators=(',', ':')).encode("utf-8")
            length = len(json_bytes)
            self._ensure(1 + length)
            self._buf[self._offset] = DataType.JSON
            self._offset += 1
            self._buf[self._offset : self._offset + length] = json_bytes
            self._offset += length
        elif isinstance(data, int) and -2**63 <= data <= 2**63 - 1:
            self._ensure(9)
            self._buf[self._offset] = DataType.INT
            self._offset += 1
            struct.pack_into(">q", self._buf, self._offset, data)
            self._offset += 8
        elif isinstance(data, str):
            encoded = data.encode("utf-8")
            length = len(encoded)
            self._ensure(1 + length)
            self._buf[self._offset] = DataType.STRING
            self._offset += 1
            if length > 0:
                self._buf[self._offset : self._offset + length] = encoded
            self._offset += length
        else:
            json_bytes = json.dumps(data if data is not None else None, separators=(',', ':')).encode("utf-8")
            length = len(json_bytes)
            self._ensure(1 + length)
            self._buf[self._offset] = DataType.JSON
            self._offset += 1
            self._buf[self._offset : self._offset + length] = json_bytes
            self._offset += length
        return self

    def any_with_len(self, data: Any) -> "FrameWriter":
        if isinstance(data, (bytes, bytearray, memoryview)):
            if isinstance(data, memoryview):
                data = bytes(data)
            length = len(data)
            self._ensure(4 + 1 + length)
            struct.pack_into(">I", self._buf, self._offset, 1 + length)
            self._offset += 4
            self._buf[self._offset] = DataType.RAW
            self._offset += 1
            self._buf[self._offset : self._offset + length] = data
            self._offset += length
        elif isinstance(data, bool):
            json_bytes = json.dumps(data, separators=(',', ':')).encode("utf-8")
            length = len(json_bytes)
            self._ensure(4 + 1 + length)
            struct.pack_into(">I", self._buf, self._offset, 1 + length)
            self._offset += 4
            self._buf[self._offset] = DataType.JSON
            self._offset += 1
            self._buf[self._offset : self._offset + length] = json_bytes
            self._offset += length
        elif isinstance(data, int) and -2**63 <= data <= 2**63 - 1:
            self._ensure(4 + 9)
            struct.pack_into(">I", self._buf, self._offset, 9)
            self._offset += 4
            self._buf[self._offset] = DataType.INT
            self._offset += 1
            struct.pack_into(">q", self._buf, self._offset, data)
            self._offset += 8
        elif isinstance(data, str):
            encoded = data.encode("utf-8")
            length = len(encoded)
            self._ensure(4 + 1 + length)
            struct.pack_into(">I", self._buf, self._offset, 1 + length)
            self._offset += 4
            self._buf[self._offset] = DataType.STRING
            self._offset += 1
            if length > 0:
                self._buf[self._offset : self._offset + length] = encoded
            self._offset += length
        else:
            json_bytes = json.dumps(data if data is not None else None, separators=(',', ':')).encode("utf-8")
            length = len(json_bytes)
            self._ensure(4 + 1 + length)
            struct.pack_into(">I", self._buf, self._offset, 1 + length)
            self._offset += 4
            self._buf[self._offset] = DataType.JSON
            self._offset += 1
            self._buf[self._offset : self._offset + length] = json_bytes
            self._offset += length
        return self

    def finish(
        self, corr_id: int, opcode: int, frame_type: int = FrameType.REQUEST
    ) -> bytes:
        total = self._offset
        self._buf[HEADER_OFFSET_VERSION] = PROTOCOL_VERSION
        self._buf[HEADER_OFFSET_TYPE] = frame_type
        self._buf[HEADER_OFFSET_META] = opcode
        struct.pack_into(">I", self._buf, HEADER_OFFSET_ID, corr_id)
        struct.pack_into(">I", self._buf, HEADER_OFFSET_PAYLOAD_LEN, total - HEADER_SIZE)
        return bytes(self._buf[:total])


# Type alias for build functions passed to connection.send
BuildFn = Callable[["FrameWriter"], Any]
