# Server Flow

## Architecture

```
TCP Socket → network.rs → commands.rs → manager.rs
```

Three layers, no hidden indirection.

---

## File Structure

```
src/
├── server/
│   ├── network.rs      TCP listener + routing
│   ├── commands.rs     All command parsing + execution
│   └── protocol.rs     Frame format (opcode + length + payload)
│
└── features/
    ├── kv/manager.rs       Key-value storage
    ├── queue/manager.rs    FIFO queues
    ├── topic/manager.rs    Pub/Sub
    └── stream/manager.rs   Append-only logs
```

---

## Request Flow

```
1. Client sends binary frame
   [opcode: 1 byte][length: 4 bytes][payload: N bytes]

2. network.rs
   - Parse frame (via protocol.rs)
   - Match opcode to feature range
   - Call Command::parse().execute()

3. commands.rs
   - Parse payload bytes → typed Command enum
   - Execute command → call manager method
   - Return Response

4. manager.rs
   - Execute business logic
   - Return result

5. network.rs
   - Encode response
   - Write to TCP socket
```

---

## Routing Table

```rust
// network.rs - handle_connection()

match opcode {
    0x01         → PING
    0x02..=0x0F  → KV (Set, Get, Del)
    0x10..=0x1F  → Queue (Push, Pop)
    0x20..=0x2F  → Topic (Publish, Subscribe)
    0x30..=0x3F  → Stream (Add, Read)
}
```

All routing visible in one place.

---

## Example: KV SET

```
Client sends:
[0x02][0x00 0x00 0x00 0x0F][0x00 0x00 0x00 0x03]['f''o''o']['b''a''r']
 opcode  payload length=15    key_len=3      key    value

Flow:
network.rs    → Receives bytes, parses frame
              → Matches 0x02 (KV range)
              → Calls KvCommand::parse(0x02, payload)

commands.rs   → Parses payload into KvCommand::Set { key: "foo", value: b"bar" }
              → Calls .execute(&engine.kv)
              → Calls manager.set("foo", b"bar", None)

kv_manager.rs → Stores in DashMap
              → Returns Ok(())

commands.rs   → Returns Response::Ok

network.rs    → Encodes [0x00][0x00 0x00 0x00 0x00]
              → Writes to socket
```

---

## Adding a New Feature

1. Create `features/new_feature/manager.rs` with business logic
2. Add enum + parse + execute in `commands.rs`
3. Add opcode range in `network.rs` routing match

Example:
```rust
// commands.rs
pub enum NewFeatureCommand { ... }
impl NewFeatureCommand {
    pub fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> { ... }
    pub fn execute(self, manager: &Manager) -> Result<Response, String> { ... }
}

// network.rs
0x40..=0x4F => {
    NewFeatureCommand::parse(req.opcode, req.payload)?
        .execute(&engine.new_feature)
}
```

---

## Key Design Principles

- **Routing is visible** - All dispatch logic in `network.rs` match statement
- **Commands centralized** - All parsing in one file, shared helpers
- **Managers isolated** - No protocol knowledge, pure business logic
- **No hidden layers** - What you see is what you get

---

## Where to Look

| Task | File |
|------|------|
| Understand request flow | `network.rs` handle_connection() |
| See all commands | `commands.rs` |
| Add new command | `commands.rs` + 1 line in `network.rs` |
| Change business logic | `features/*/manager.rs` |
| Change wire format | `protocol.rs` |

