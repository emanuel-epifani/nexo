# Server Flow

## Connection Lifecycle

```
Client connects
    ↓
network.rs → TCP listener + accept loop + connection handler
    ↓
protocol.rs → parse Binary Frame (Opcode + Length + Payload)
    ↓
routing.rs → parse Command enum + validate + dispatch to manager
    ↓
features/* → execute business logic
    ↓
protocol.rs → encode Binary response
    ↓
network.rs → write to socket
```

## Components (3 Files)

- **network.rs** - TCP listener + connection handling (Layer 1)
- **protocol.rs** - Binary Protocol parser/encoder (Layer 2)
- **routing.rs** - Command parsing + dispatcher (Layer 3)

## Example Flow (KV.SET "key" "val")

```
Raw bytes (Binary): 
[0x02]          (Opcode: KV_SET)
[0x00,0x00,0x00,0x0A] (Body Len: 4 + 3 + 3 = 10 bytes)
[0x00,0x00,0x00,0x03] (Key Len: 3)
[0x6B,0x65,0x79]      ("key")
[0x76,0x61,0x6C]      ("val")

    ↓ network.rs (read socket)
    ↓ protocol.rs (parse_request)

Request { opcode: 0x02, payload: &[...] }

    ↓ routing.rs (Command::from_request)

Command::KvSet { key: "key", value: Vec<u8> }

    ↓ routing.rs (route + dispatch)

kv_manager.set("key", b"val", None)

    ↓ protocol.rs (encode_response)

Response::Ok → [0x00] [0x00,0x00,0x00,0x00] (Status OK, Len 0)

    ↓ network.rs (write socket)
```
