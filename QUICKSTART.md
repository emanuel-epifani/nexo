# Nexo MVP - Quick Start Guide

## What's Implemented

- **RESP Protocol**: Full Redis-compatible protocol parser and encoder
- **KV Manager**: In-memory key-value store with TTL support
- **Dispatcher**: Namespace-based command routing
- **Commands**:
  - `PING` - Health check
  - `KV.SET key value [ttl]` - Set key with optional TTL (seconds)
  - `KV.GET key` - Get value
  - `KV.DEL key` - Delete key

## Running the Server

```bash
cargo run
```

Server will start on `localhost:8080`

## Testing

### Using the Test Script

```bash
./test_nexo.sh
```

### Manual Testing with netcat

```bash
# Open connection
nc localhost 8080

# Send PING (type these lines, press Enter after each)
*1
$4
PING

# Send KV.SET
*3
$6
KV.SET
$4
name
$4
John

# Send KV.GET
*2
$6
KV.GET
$4
name
```

## Logging

The server logs at 3 levels:

1. **[Socket]** - Raw bytes received from client (shows `\r\n`)
2. **[Parser]** - Clean parsed command array
3. **[Dispatcher]** - Command routing
4. **[KV Manager]** - Manager operations (SET/GET/DEL)

Example output:
```
[Server] Nexo listening on :8080
[Server] New connection from 127.0.0.1:54321
[Socket] Raw bytes received: [42, 49, 13, 10, 36, 52, 13, 10, 80, 73, 78, 71, 13, 10]
[Parser] Parsed args (clean): ["PING"]
[Dispatcher] Handling PING command
```

## Commands Reference

### PING
```
Request:  *1\r\n$4\r\nPING\r\n
Response: $4\r\nPONG\r\n
```

### KV.SET (without TTL)
```
Request:  *3\r\n$6\r\nKV.SET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
Response: +OK\r\n
```

### KV.SET (with TTL)
```
Request:  *4\r\n$6\r\nKV.SET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\n60\r\n
Response: +OK\r\n
```

### KV.GET
```
Request:  *2\r\n$6\r\nKV.GET\r\n$3\r\nkey\r\n
Response: $5\r\nvalue\r\n (if found)
          $-1\r\n          (if not found)
```

### KV.DEL
```
Request:  *2\r\n$6\r\nKV.DEL\r\n$3\r\nkey\r\n
Response: :1\r\n (deleted)
          :0\r\n (not found)
```

## Error Handling

All errors return RESP error format:
```
-ERR error message\r\n
```

Examples:
- Unknown command: `-ERR Unknown namespace: INVALID\r\n`
- Invalid args: `-ERR KV.SET requires at least 2 arguments\r\n`
- Protocol error: `-ERR Protocol error: CRLF not found\r\n`

## Architecture

```
Client
  ↓
TCP :8080
  ↓
RESP Parser (bytes → RespValue → string array)
  ↓
Dispatcher (namespace routing)
  ├─→ PING (direct response)
  └─→ KV.* → KV Manager
```

## Next Steps

- [ ] Add TOPIC (Pub/Sub) manager
- [ ] Add persistence (WAL + snapshots)
- [ ] Add INFO command
- [ ] Write client SDKs
