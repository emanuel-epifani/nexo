# Stream — Ottimizzazioni bitwise per throughput

> **Stato:** analisi, nessuna modifica al codice.  
> **Scope:** `src/brokers/stream`, `src/transport/tcp/protocol/wire.rs`, `sdk/ts/src/brokers/stream.ts`, `sdk/py/src/nexo/brokers/stream.py`.

## Baseline attuale

Dai benchmark in `tests/stress_tests.rs` e dai test di stress degli SDK:

- Rust `bench_stream_publish`: ~78.000 scritture confermate/sec.
- TS SDK `STREAM PUBLISH`: ~49.000 concorrenti / ~24.000 sequenziali.
- TS SDK `STREAM PUB BATCH`: ~1.860.000 eventi/sec.
- TS SDK `STREAM SUB+ACK`: ~34.000 msg/sec.
- Python SDK `STREAM SUB+ACK`: ~26.000 msg/sec.

Il percorso `fetch → process → ack` è il collo di bottiglia principale: è dominato da round-trip per singolo messaggio, parsing di `BigInt`/`u64` per ogni sequenza, e mutazioni dello stato in `ConsumerGroup`. Le ottimizzazioni bitwise più potenziali sono tutte lì.

---

## 1. Rust core — `ConsumerGroup` (`src/brokers/stream/domain/group.rs`)

`ConsumerGroup` è il percorso critico per ogni `fetch`, `ack`, redelivery e `check_redelivery`.

### 1.1 Sostituire `BTreeMap<u64, MsgState>` con uno stato bit-packed

- Oggi `msgs` è `BTreeMap<u64, MsgState>` e `redeliver_idx` è `BTreeSet<u64>`.
- Le sequenze di uno stream sono dense e monotone. Si può rappresentare lo stato in un `Vec<u64>` di 2 bit per messaggio:
  - `00` — acked / assente
  - `01` — pending
  - `10` — redeliver
  - `11` — DLS
- 64 bit contengono 32 stati. I metadati aggiuntivi (`consumer_id`, `delivery_count`, `key`) vivono in array paralleli o in uno `Slab`, allocati solo per i messaggi non-acked.
- `try_advance_floor()` diventa una scansione di parole con `trailing_ones` / `ctz` / `ctlz` invece di `O(gap)` lookup su `BTreeMap`.
- `redeliver_idx` scompare: si scansiona il bitmap degli stati `10`, molto più cache-friendly di un BTree.

### 1.2 Avanzamento dell'`ack_floor` con bitmap

Codice attuale in `ConsumerGroup::try_advance_floor`:

```rust
while self.ack_floor + 1 < self.next_deliver_seq {
    let next = self.ack_floor + 1;
    match self.msgs.get(&next) { ... }
}
```

Con uno stato bit-packed, un bit a `1` indica pending/redeliver, un bit a `0` indica acked/DLS/assente. L'avanzamento del floor diventa:

```rust
let word = state_bits[word_idx] >> (bit_idx * 2);
let leading_acked = word.trailing_ones() / 2;
self.ack_floor += leading_acked as u64;
```

Il percorso ack più comune — una sequenza contigua di ack — diventa un'operazione su poche parole `u64` invece di tanti lookup alberati.

### 1.3 Bitmap per i `blocked` key-set

`KeyState.blocked` è `BTreeSet<u64>`. Per chiavi con sequenze bloccate dense, si può usare un piccolo bitmap o una rappresentazione run-length `(start_seq, count)`. Per backlog enormi, fallback a `BTreeSet` o `RoaringBitmap`.

### 1.4 Flags bit-packed in `KeyState`

`KeyState` contiene `poisoned: bool`, `in_flight: Option<u64>` e `blocked: BTreeSet<u64>`. Si possono impacchettare in un singolo `u128`:
- bit 0: `poisoned`
- bit 1: `has_in_flight`
- bit 2-63: `in_flight_seq`
- i rimanenti bit o un puntatore al set bloccato.

---

## 2. Rust core — persistenza e formato on-disk

`serialize_message` in `src/brokers/stream/domain/persistence.rs` scrive:

```rust
[len u32][crc u32][seq u64][timestamp u64][key_len u16][key][payload]
```

### 2.1 Varint per i campi di lunghezza

La maggior parte dei payload e delle chiavi è piccola. Sostituire `u32`/`u16` fissi con varint/LEB128 per `len`, `key_len`, e `payload_len` riduce byte su disco e sul wire, e diminuisce il lavoro di parsing degli SDK.

### 2.2 Risposta `fetch` compatta con delta encoding

`encode_fetch` in `src/brokers/stream/tcp.rs` emette `count` seguito da metadati completi per messaggio. Nel caso comune i messaggi fetchati sono contigui. Un formato compatto:

```text
[count: u32]
[flags: u8]                 // bit 0: seq contigui
                            // bit 1: timestamp delta-encoded
                            // bit 2: presente una key-bitmap
[base_seq: varint]
[timestamp_base: varint]
per ogni messaggio:
  [timestamp_delta: varint]
  [key_len: varint] (0 = nessuna chiave)
  [payload_len: varint]
  [key bytes]
  [payload bytes]
```

Quando il flag `contiguo` è attivo, `seq = base_seq + i`. Si eliminano 8 byte di `u64` per messaggio e il parsing `BigInt` in JS/Python.

### 2.3 Lettura contigua da disco

`try_fetch_once` costruisce un `Vec<(u64, u64)>` di offset e invia `ReadRange`. Se il piano è un singolo range contiguo, `read_range` può fare una sola `read_exact` invece di un `seek` per record. Aggiungere `StorageCommand::ReadContiguous` per leggere `N` record a partire da un offset base e parse in memoria.

---

## 3. TCP / wire protocol (`src/transport/tcp/protocol/wire.rs`)

### 3.1 Reader/writer varint

Aggiungere `put_varint` / `read_varint` a `PayloadWriter` e `PayloadCursor`. Usarli per:
- `count` di `publish`/`fetch`
- `key_len` e `payload_len`
- `seq` delta
- `ack_floor`, `generation`, `wait_ms` quando piccoli

### 3.2 Header `Meta` come bitmap di flag

L'header attuale è `[Version:1][FrameType:1][Meta:1][CorrelationID:4][PayloadLen:4]`. Il byte `Meta` può essere diviso in flag:
- bit 5-0: opcode/status/push-type
- bit 6: richiesta risposta compatta
- bit 7: frame parte di un batch multi-frame

Se i bit alti vengono ignorati dai peer vecchi, la modifica può essere retrocompatibile; in alternativa richiede un bump di `PROTOCOL_VERSION`.

### 3.3 Opcode `ACK_BATCH`

Il più grande "game changer" lato SDK è un ack aggregato. Aggiungere `OP_S_ACK_BATCH`:

```text
OP_S_ACK_BATCH
  topic: string
  group: string
  consumer_id: string
  generation: u64
  base_seq: u64
  count: u32
  ack_mask: u64[]      // bit i => ack(base_seq + i)
```

Per `batchSize=100` contiguo bastano due `u64` di maschera. Un solo frame sostituisce 100 `S_ACK` individuali.

### 3.4 Tipo payload condiviso nel batch publish

`anyWithLen` aggiunge `1 byte DataType + 4 byte length` per ogni payload. Se tutti gli item di `publishBatch` hanno lo stesso tipo (RAW o JSON), si può inviare:

```text
OP_S_PUB
  topic
  count
  batch_type: u8       // DataType condiviso
  per ogni item:
    key_len: varint
    key (se > 0)
    payload_len: varint
    payload
```

Risparmia 5 byte per messaggio e un `decodeAnyFromBuffer` per item.

---

## 4. TypeScript SDK (`sdk/ts/src/brokers/stream.ts`)

### 4.1 Accumulo ack con bitmask

In `StreamSubscription.pollOnce`, `runConcurrent` manda un `S_ACK` per ogni messaggio. Si può accumulare in una `bigint` bitmask o in un array di `u64` e flushare con `OP_S_ACK_BATCH` alla fine del batch o dopo un breve timeout. Questo da solo può portare `STREAM SUB+ACK` da 34k a oltre 100k msg/s.

### 4.2 Evitare `BigInt` per ogni seq

Con la risposta compatta, `seq = baseSeq + BigInt(i)`. `i` è un `Number` piccolo, quindi si fa una sola conversione `BigInt` per batch invece di una per messaggio.

### 4.3 `FrameWriter` e varint

`codec.ts` allarga il buffer con potenze di due. Per scrivere varint, pre-stimare la dimensione e usare un ciclo inline che scrive 1-10 byte, riducendo le chiamate a `ensure()`.

---

## 5. Python SDK (`sdk/py/src/nexo/brokers/stream.py`)

### 5.1 Stessa strategia di ack batch

Accumulare gli ack in un `bytearray` o in una `int` bitmask e flushare con `OP_S_ACK_BATCH`. Il GIL di Python rende il `await conn.send` per messaggio particolarmente costoso; il batching è ancora più impattante che in TS.

### 5.2 Parsing con `memoryview` per risposte compatte

Una risposta compatta con array di `payload_len` consente di fare un unico `struct.unpack_from` di un prefisso conosciuto o di usare `int.from_bytes` su slice di `memoryview`, riducendo le chiamate Python per campo.

### 5.3 Fast path `decode_any_from_buffer`

Quando `batch_type` è `RAW`, restituire direttamente slice `bytes`. Quando è `JSON`, considerare un formato JSON-lines o un unico array di payload per evitare `json.loads` per ogni messaggio.

---

## 6. Ottimizzazioni trasversali a basso impatto ma facili

- **`MsgState` bit-packed** — 2 bit per stato invece di un enum con `String` allocata.
- **Indice numerico consumer** — `consumer_id` è una UUID stringa. Sostituirla con un `u32` locale, inviando l'indice sul wire, elimina 36 byte per messaggio pending.
- **Roaring bitmap per DLS** — `peek_dls`, `purge_dls`, `dls_snapshot` lavorano su insiemi di seq.
- **Bitmap key-type per batch** — un byte ogni 8 item indica JSON vs RAW invece di un byte per item.
- **Trim indice con bitmap** — quando `head_seq` avanza, azzerare 64 entry di `index` per volta con una word operation.

---

## 7. Compatibilità e rollout

- Ogni modifica al wire richiede un bump di `PROTOCOL_VERSION` e aggiornamenti simmetrici in `wire.rs`, `sdk/ts/src/codec.ts`, `sdk/py/src/nexo/codec.py`, test e docs.
- Modifiche on-disk richiedono una nuova estensione segment (es. `.log2`) o una migrazione esplicita in `recover_stream`.
- Il punto di partenza con il miglior rapporto impatto/rischio è **l'ack batch lato SDK** con un nuovo opcode: non tocca la persistenza e può essere negoziato tra client e server.
- La modifica Rust con il maggior impatto è lo **stato bit-packed di `ConsumerGroup`**: è localizzata in `group.rs` e validabile con i bench già presenti.

---

## 8. Raccomandazioni prioritarie

| Priorità | Ottimizzazione | Guadagno stimato | File principali | Rischio |
|---|---|---|---|---|
| 1 | `OP_S_ACK_BATCH` + flush SDK | 2-5x `SUB+ACK` | `tcp.rs`, `stream.ts`, `stream.py` | Basso |
| 2 | Stato bit-packed in `ConsumerGroup` | 2-3x fetch/ack | `group.rs` | Medio |
| 3 | Risposta `fetch` compatta/delta | 2-4x throughput fetch | `tcp.rs`, `persistence.rs`, SDK | Medio |
| 4 | Varint su wire/disk | 10-30% meno I/O | `wire.rs`, `persistence.rs`, SDK | Medio |
| 5 | Lettura contigua per piani contigui | 1.5-2x cold fetch | `persistence.rs`, `manager.rs` | Basso |
| 6 | Tipo payload condiviso nel batch | 10-20% meno byte batch | `tcp.rs`, SDK | Basso |
