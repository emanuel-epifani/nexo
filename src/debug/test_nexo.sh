#!/bin/bash

# Test script for Nexo server
# Usage: ./test_nexo.sh

echo "Testing Nexo Server on localhost:8080"
echo "========================================="
echo ""

# Function to send RESP command
send_resp() {
    local cmd="$1"
    echo "Sending: $cmd"
    echo -ne "$cmd" | nc localhost 8080
    echo ""
}

# Test PING
echo "1. Testing PING"
send_resp "*1\r\n\$4\r\nPING\r\n"
echo ""

# Test KV.SET
echo "2. Testing KV.SET user:123 John"
send_resp "*3\r\n\$6\r\nKV.SET\r\n\$8\r\nuser:123\r\n\$4\r\nJohn\r\n"
echo ""

# Test KV.GET
echo "3. Testing KV.GET user:123"
send_resp "*2\r\n\$6\r\nKV.GET\r\n\$8\r\nuser:123\r\n"
echo ""

# Test KV.SET with TTL
echo "4. Testing KV.SET temp:key value 5 (5 second TTL)"
send_resp "*4\r\n\$6\r\nKV.SET\r\n\$8\r\ntemp:key\r\n\$5\r\nvalue\r\n\$1\r\n5\r\n"
echo ""

# Test KV.GET on key that doesn't exist
echo "5. Testing KV.GET nonexistent"
send_resp "*2\r\n\$6\r\nKV.GET\r\n\$11\r\nnonexistent\r\n"
echo ""

# Test KV.DEL
echo "6. Testing KV.DEL user:123"
send_resp "*2\r\n\$6\r\nKV.DEL\r\n\$8\r\nuser:123\r\n"
echo ""

# Test KV.DEL on non-existent key
echo "7. Testing KV.DEL nonexistent"
send_resp "*2\r\n\$6\r\nKV.DEL\r\n\$11\r\nnonexistent\r\n"
echo ""

echo "========================================="
echo "Tests complete!"



