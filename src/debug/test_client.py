#!/usr/bin/env python3
"""
Binary Protocol Test Client for Nexo.
Header: [Opcode: 1B] [BodyLen: 4B BE]
Payload: Body...
"""

import socket
import struct
import sys

# Opcodes
OP_PING   = 0x01
OP_KV_SET = 0x02
OP_KV_GET = 0x03
OP_KV_DEL = 0x04

# Status Codes
STATUS_OK   = 0x00
STATUS_ERR  = 0x01
STATUS_NULL = 0x02
STATUS_DATA = 0x03

def print_separator(title):
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)

def pack_cmd(opcode, payload=b""):
    """Pack opcode and payload into a binary frame"""
    header = struct.pack(">BI", opcode, len(payload))
    return header + payload

def recv_response(sock):
    """Receive and parse binary response"""
    try:
        # 1. Read Header (5 bytes)
        header = sock.recv(5)
        if len(header) < 5:
            print("[ERR ] Connection closed or incomplete header")
            return None
            
        status, length = struct.unpack(">BI", header)
        
        # 2. Read Body
        data = b""
        if length > 0:
            data = sock.recv(length)
            while len(data) < length:
                chunk = sock.recv(length - len(data))
                if not chunk: break
                data += chunk
        
        # 3. Decode
        status_str = "UNKNOWN"
        decoded = ""
        
        if status == STATUS_OK:
            status_str = "OK"
        elif status == STATUS_ERR:
            status_str = "ERROR"
            decoded = data.decode('utf-8', errors='replace')
        elif status == STATUS_NULL:
            status_str = "NULL"
        elif status == STATUS_DATA:
            status_str = "DATA"
            decoded = data.decode('utf-8', errors='replace') # Assuming text for now, could be bytes
            
        print(f"[RECV] Status: {status_str}, Len: {length}")
        if decoded:
            print(f"       Body: {decoded}")
        elif data:
            print(f"       Body (hex): {data.hex()}")
            
        return status, data
        
    except Exception as e:
        print(f"[ERR ] {e}")
        return None

def send_ping(sock):
    print("\n[TEST] PING")
    packet = pack_cmd(OP_PING)
    sock.sendall(packet)
    recv_response(sock)

def send_kv_set(sock, key, value):
    print(f"\n[TEST] KV.SET {key} = {value}")
    k_bytes = key.encode()
    v_bytes = value.encode()
    # Payload: [KeyLen 4B] [Key] [Value]
    payload = struct.pack(">I", len(k_bytes)) + k_bytes + v_bytes
    packet = pack_cmd(OP_KV_SET, payload)
    sock.sendall(packet)
    recv_response(sock)

def send_kv_get(sock, key):
    print(f"\n[TEST] KV.GET {key}")
    k_bytes = key.encode()
    # Payload: [Key]
    packet = pack_cmd(OP_KV_GET, k_bytes)
    sock.sendall(packet)
    recv_response(sock)

def send_kv_del(sock, key):
    print(f"\n[TEST] KV.DEL {key}")
    k_bytes = key.encode()
    # Payload: [Key]
    packet = pack_cmd(OP_KV_DEL, k_bytes)
    sock.sendall(packet)
    recv_response(sock)

def main():
    print_separator("Nexo Binary Protocol Tester")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 8080))
        print("✓ Connected to localhost:8080")
    except ConnectionRefusedError:
        print("✗ Could not connect. Is server running?")
        sys.exit(1)
    
    try:
        send_ping(sock)
        
        send_kv_set(sock, "user:1", "Alice")
        send_kv_get(sock, "user:1")
        
        send_kv_set(sock, "user:1", "Bob") # Update
        send_kv_get(sock, "user:1")
        
        send_kv_del(sock, "user:1")
        send_kv_get(sock, "user:1") # Should be NULL

        # Test Binary Data
        send_kv_set(sock, "bin:img", "\x00\xFF\x00\xAA") 
        send_kv_get(sock, "bin:img")

    finally:
        sock.close()
        print_separator("Done")

if __name__ == "__main__":
    main()



