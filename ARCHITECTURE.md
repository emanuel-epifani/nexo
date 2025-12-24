# Nexo Architecture - Centralized Commands Design

## Overview

Nexo usa un'architettura **minimalista** con **routing visibile** e **comandi centralizzati**.

```
TCP Socket
    ↓
network.rs (routing inline visibile)
    ↓
commands.rs (tutti i Command enum + parse + execute)
    ↓
features/*/manager.rs (business logic)
```

---

## File Structure

```
src/
├── server/
│   ├── mod.rs          (9 righe)
│   ├── network.rs      (124 righe) ← Routing inline VISIBILE
│   ├── protocol.rs     (108 righe) ← Frame parsing
│   └── commands.rs     (272 righe) ← TUTTI i comandi centralizzati
│
└── features/
    ├── kv/
    │   ├── mod.rs
    │   └── manager.rs  ← Solo business logic
    ├── queue/
    │   ├── mod.rs
    │   └── manager.rs
    ├── topic/
    │   ├── mod.rs
    │   └── manager.rs
    └── stream/
        ├── mod.rs
        └── manager.rs
```

**Totale server: 513 righe** (4 file invece di 7)

---

## Layer 1: Network - Routing Visibile

**`network.rs` (124 righe)**

Il routing è **inline** nel connection handler - vedi subito dove va ogni opcode:

```rust
let response = match req.opcode {
    0x01 => Ok(Response::Ok), // PING
    
    // KV commands (0x02-0x0F)
    0x02..=0x0F => {
        KvCommand::parse(req.opcode, req.payload)?
            .execute(&engine.kv)
    },
    
    // Queue commands (0x10-0x1F)
    0x10..=0x1F => {
        QueueCommand::parse(req.opcode, req.payload)?
            .execute(&engine.queue)
    },
    
    // Topic commands (0x20-0x2F)
    0x20..=0x2F => {
        TopicCommand::parse(req.opcode, req.payload)?
            .execute(&engine.topic)
    },
    
    // Stream commands (0x30-0x3F)
    0x30..=0x3F => {
        StreamCommand::parse(req.opcode, req.payload)?
            .execute(&engine.stream)
    },
    
    _ => Err(format!("Unknown opcode: 0x{:02X}", req.opcode)),
}.unwrap_or_else(Response::Error);
```

**Vantaggi:**
- ✅ **Routing visibile** a colpo d'occhio
- ✅ **Range-based** (leggibile, scalabile)
- ✅ **Chiamata diretta**: `parse()?.execute()`

---

## Layer 2: Commands - Centralizzato

**`server/commands.rs` (272 righe)**

Tutti i comandi in un unico file:

```rust
// Opcodes
pub const OP_KV_SET: u8 = 0x02;
pub const OP_KV_GET: u8 = 0x03;
// ... tutti gli opcode

// Command Enums
pub enum KvCommand { Set {...}, Get {...}, Del {...} }
pub enum QueueCommand { Push {...}, Pop {...} }
pub enum TopicCommand { Publish {...}, Subscribe {...} }
pub enum StreamCommand { Add {...}, Read {...} }

// Parsing Helpers (condivisi!)
fn parse_length_prefixed_string(payload: &[u8]) -> Result<(&str, &[u8]), String>
fn parse_string(payload: &[u8]) -> Result<String, String>
fn parse_string_and_u64(payload: &[u8]) -> Result<(&str, u64), String>

// Implementations
impl KvCommand {
    pub fn parse(opcode: u8, payload: &[u8]) -> Result<Self, String> { ... }
    pub fn execute(self, manager: &KvManager) -> Result<Response, String> { ... }
}

impl QueueCommand { ... }
impl TopicCommand { ... }
impl StreamCommand { ... }
```

**Vantaggi:**
- ✅ **Helper condivisi** (no duplicazione ~80 righe)
- ✅ **Tutto visibile** in un file
- ✅ **Type safety** (enum tipizzati)
- ✅ **Testabile** (ogni Command testabile separatamente)

---

## Layer 3: Managers - Business Logic

**`features/*/manager.rs`**

Solo business logic, nessuna conoscenza del protocollo wire:

```rust
// kv_manager.rs
pub struct KvManager {
    store: DashMap<String, Entry>,
}

impl KvManager {
    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<u64>) -> Result<(), String>
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>
    pub fn del(&self, key: &str) -> Result<bool, String>
}
```

**Vantaggi:**
- ✅ **Nessuna dipendenza** dal protocollo
- ✅ **Testabile** in isolamento
- ✅ **Riutilizzabile** (CLI, admin API, etc.)

---

## Data Flow Example: KV SET

```
1. Client: [0x02][len][payload]
   ↓
2. network.rs:74 → parse_request()
   → Request { opcode: 0x02, payload: [...] }
   ↓
3. network.rs:85 → match 0x02 (in range 0x02..=0x0F)
   ↓
4. network.rs:86 → KvCommand::parse(0x02, payload)?
   ↓
5. commands.rs:110 → match OP_KV_SET
   → parse_length_prefixed_string()
   → KvCommand::Set { key, value, ttl }
   ↓
6. network.rs:87 → .execute(&engine.kv)
   ↓
7. commands.rs:131 → match KvCommand::Set
   ↓
8. commands.rs:133 → manager.set(key, value, ttl)?
   ↓
9. kv_manager.rs:25 → store.insert(...)
   ↓
10. Response::Ok
```

**Solo 3 salti logici:**
1. Network → routing
2. Commands → parse + execute
3. Manager → business logic

---

## Key Design Decisions

### ✅ Centralized Commands
**Perché:** Elimina duplicazione helper, tutto visibile in un posto  
**Trade-off:** File più grande (272 righe, ma gestibile)

### ✅ Inline Routing
**Perché:** Flusso chiaro, vedi subito dove va ogni opcode  
**Trade-off:** `network.rs` leggermente più lungo (ma lineare)

### ✅ Enum-based Commands
**Perché:** Type safety, testabilità, pattern matching  
**Trade-off:** Più verboso di funzioni dirette (ma vale la pena)

### ✅ Managers Puliti
**Perché:** Business logic isolata, riutilizzabile  
**Trade-off:** Nessuno!

---

## Adding a New Feature

1. Implementa `features/new_feature/manager.rs`
2. Aggiungi enum + parse + execute in `server/commands.rs`
3. Aggiungi range in `network.rs` routing (1 riga)

**Esempio:**
```rust
// commands.rs
pub enum NewFeatureCommand { ... }
impl NewFeatureCommand {
    pub fn parse(...) -> Result<Self, String> { ... }
    pub fn execute(...) -> Result<Response, String> { ... }
}

// network.rs
0x40..=0x4F => {
    NewFeatureCommand::parse(req.opcode, req.payload)?
        .execute(&engine.new_feature)
},
```

---

## Testing Strategy

```rust
// Test parsing
#[test]
fn test_kv_parse() {
    let payload = [0x00, 0x00, 0x00, 0x03, b'f', b'o', b'o'];
    let cmd = KvCommand::parse(OP_KV_GET, &payload).unwrap();
    assert!(matches!(cmd, KvCommand::Get { key } if key == "foo"));
}

// Test execution
#[test]
fn test_kv_execute() {
    let manager = KvManager::new();
    let cmd = KvCommand::Set { 
        key: "test".to_string(), 
        value: b"value".to_vec(), 
        ttl: None 
    };
    let resp = cmd.execute(&manager).unwrap();
    assert!(matches!(resp, Response::Ok));
}
```

---

## Metrics

- **Files:** 13 Rust files total (server + features)
- **Server code:** 513 lines (4 files)
- **Commands:** 272 lines (1 file centralizzato)
- **Layers:** 2 (network → commands → manager)
- **Helper duplication:** 0 (tutti condivisi in commands.rs)
- **Routing visibility:** ✅ Inline in network.rs

---

## Comparison with Previous Architectures

### Before (feature-centric, 4 commands files)
```
network.rs → kv/commands.rs → kv_manager.rs
          → queue/commands.rs → queue_manager.rs
          → topic/commands.rs → topic_manager.rs
          → stream/commands.rs → stream_manager.rs
```
- 4 commands files × ~90 righe = ~360 righe
- Helper duplicati in ogni file (~80 righe duplicate)
- Routing visibile ✅

### After (centralized commands)
```
network.rs → commands.rs → kv_manager.rs
                        → queue_manager.rs
                        → topic_manager.rs
                        → stream_manager.rs
```
- 1 commands file = 272 righe
- Helper condivisi (no duplicazione)
- Routing visibile ✅

**Net saving:** ~170 righe + 3 file eliminati

---

## Advantages Summary

✅ **Routing visibile** - Match inline in `network.rs`  
✅ **Helper condivisi** - No duplicazione (~80 righe risparmiate)  
✅ **Type safety** - Enum tipizzati per ogni feature  
✅ **Testabile** - Parse + execute separabili  
✅ **Tutto visibile** - 1 file per tutti i comandi  
✅ **Pragmatico** - Meno file, più chiaro  
✅ **Scalabile** - Aggiungi feature = 1 enum + 1 riga routing

---

## Philosophy

**"Massima chiarezza con minimo overhead"**

- Routing dove lo vedi subito (`network.rs`)
- Comandi dove li trovi tutti (`commands.rs`)
- Business logic dove è isolata (`manager.rs`)

Niente layer nascosti, niente indirezioni inutili. 🎯
