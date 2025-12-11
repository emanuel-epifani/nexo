#!/usr/bin/env python3
"""
Simple Python client to test Nexo server
Usage: python3 test_client.py
"""

import socket

def send_command(sock, *args):
    """Send RESP command and receive response"""
    # Encode command as RESP array
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_bytes = str(arg).encode('utf-8')
        cmd += f"${len(arg_bytes)}\r\n{arg_bytes.decode()}\r\n"
    
    print(f"→ Sending: {args}")
    print(f"  Raw RESP: {repr(cmd)}")
    
    sock.sendall(cmd.encode())
    
    # Receive response
    response = sock.recv(4096)
    print(f"← Response: {repr(response)}")
    print(f"  Decoded: {response.decode('utf-8', errors='ignore')}")
    print()
    
    return response

def main():
    print("=" * 60)
    print("Nexo Test Client")
    print("=" * 60)
    print()
    
    # Connect to server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 8080))
    print("✓ Connected to localhost:8080")
    print()
    
    try:
        # Test 1: PING
        print("Test 1: PING")
        send_command(sock, "PING")
        
        # Test 2: KV.SET
        print("Test 2: KV.SET user:123 Alice")
        send_command(sock, "KV.SET", "user:123", "Alice")
        
        # Test 3: KV.GET
        print("Test 3: KV.GET user:123")
        send_command(sock, "KV.GET", "user:123")
        
        # Test 4: KV.SET with TTL
        print("Test 4: KV.SET session:abc token123 60")
        send_command(sock, "KV.SET", "session:abc", "token123", "60")
        
        # Test 5: KV.GET non-existent
        print("Test 5: KV.GET nonexistent")
        send_command(sock, "KV.GET", "nonexistent")
        
        # Test 6: KV.DEL existing
        print("Test 6: KV.DEL user:123")
        send_command(sock, "KV.DEL", "user:123")
        
        # Test 7: KV.DEL non-existent
        print("Test 7: KV.DEL nonexistent")
        send_command(sock, "KV.DEL", "nonexistent")
        
        # Test 8: Invalid command
        print("Test 8: INVALID.COMMAND")
        send_command(sock, "INVALID.COMMAND")
        
        print("=" * 60)
        print("All tests completed!")
        print("=" * 60)
        
    finally:
        sock.close()

if __name__ == "__main__":
    main()
