# Server Flow

## Connection Lifecycle

```
Client connects
    ↓
network.rs → TCP listener + accept loop + connection handler
    ↓
protocol.rs → parse RESP bytes
    ↓
routing.rs → parse Command enum + validate + dispatch to manager
    ↓
features/* → execute business logic
    ↓
protocol.rs → encode RESP response
    ↓
network.rs → write to socket
```

## Components (3 Files)

- **network.rs** - TCP listener + connection handling (Layer 1)
- **protocol.rs** - RESP parser/encoder (Layer 2)
- **routing.rs** - Command parsing + dispatcher (Layer 3)

## Example Flow

```
Raw bytes: *3\r\n$6\r\nKV.SET\r\n$3\r\nkey\r\n$3\r\nval\r\n
    ↓ network.rs (read socket)
    ↓ protocol.rs (parse_resp)
RespValue::Array(["KV.SET", "key", "val"])
    ↓ routing.rs (Command::from_args)
Command::KvSet { key: "key", value: b"val", ttl: None }
    ↓ routing.rs (route + dispatch)
kv_manager.set("key", b"val", None)
    ↓ protocol.rs (encode_resp)
Response::Ok → b"+OK\r\n"
    ↓ network.rs (write socket)
```
