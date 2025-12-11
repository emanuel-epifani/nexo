#!/usr/bin/env python3
"""
Test script to demonstrate new logging with Command enum
"""

import socket
import time

def send_command(sock, *args):
    """Send RESP command"""
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_bytes = str(arg).encode('utf-8')
        cmd += f"${len(arg_bytes)}\r\n{arg_bytes.decode()}\r\n"
    
    print(f"→ Sending: {args}")
    sock.sendall(cmd.encode())
    
    response = sock.recv(4096)
    print(f"← Response: {response.decode('utf-8', errors='ignore')}")
    print()
    time.sleep(0.1)

def main():
    print("=" * 70)
    print("Testing New Command Enum Implementation")
    print("=" * 70)
    print()
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 8080))
    print("✓ Connected to localhost:8080")
    print()
    
    try:
        print("Test 1: PING")
        send_command(sock, "PING")
        
        print("Test 2: KV.SET with TTL")
        send_command(sock, "KV.SET", "session:abc", "token123", "60")
        
        print("Test 3: KV.GET")
        send_command(sock, "KV.GET", "session:abc")
        
        print("Test 4: KV.SET without TTL")
        send_command(sock, "KV.SET", "user:123", "Alice")
        
        print("Test 5: KV.DEL")
        send_command(sock, "KV.DEL", "user:123")
        
        print("Test 6: TOPIC.SUBSCRIBE (not implemented)")
        send_command(sock, "TOPIC.SUBSCRIBE", "news/tech")
        
        print("Test 7: Invalid command")
        send_command(sock, "INVALID.COMMAND")
        
        print("=" * 70)
        print("Tests complete! Check server logs for new format.")
        print("=" * 70)
        
    finally:
        sock.close()

if __name__ == "__main__":
    main()
