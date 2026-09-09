# Ottimizzazioni bitwise sul broker Store: analisi di throughput

> Questo documento è puramente analitico: nessuna modifica al codice è stata apportata. Il broker preso in esame è `store`; le considerazioni si estendono a `src/`, `sdk/ts/` e `sdk/py/`.

## TL;DR

Sì, esistono spazi in cui il **bit-packing** e la **logica bit-a-bit** possono migliorare sensibilmente il throughput dello Store, soprattutto riducendo allocazioni, cache misses e branch misprediction. Tuttavia il vero “game changer” non è un singolo trucco bitwise, ma la combinazione di:

1. **Rappresentazione in-memory compatta degli oggetti dello Store** (`Entry`, TTL, valori piccoli).
2. **Bit-packing del frame `MAP_SET`**: un unico byte di `flags` che codifica TTL, tipo valore e, opzionalmente, dimensioni ridotte di chiave/valore.
3. **Ottimizzazione degli SDK**: evitare il percorso JSON per booleani e numeri, ridurre le chiamate per campo durante la costruzione del frame.

Micro-ottimizzazioni isolate (es. `is_inline_opcode` con maschera a bit) offrono guadagni più modesti, ma valgono la pena se il percorso critico è già molto ottimizzato.

---

## Stato attuale del percorso critico

### Rust

- `src/brokers/store/domain/map.rs`: ogni entry è `DashMap<String, Entry>` con `Entry { value: Bytes, expires_at: Option<Instant> }`.
- `src/brokers/store/tcp.rs:46-50`: il comando `MAP_SET` legge `key`, un byte `flags` (solo bit 0 usato per TTL), `ttl` opzionale e il resto del payload come `Bytes` grezzo.
- `src/brokers/store/domain/map.rs:66-76`: `get` clona `Bytes` (Arc) e controlla `Option<Instant>` con due branch annidati.
- `src/brokers/store/domain/map.rs:101-149`: `incr` alloca ogni volta un nuovo `Vec<u8>` di 9 byte per scrivere `INT_PREFIX + i64`.
- `src/transport/tcp/dispatcher.rs:15-29`: `is_inline_opcode` usa `matches!` su una lista di opcode; `dispatch` fa range check e match a catena.
- `src/transport/tcp/protocol/frame.rs:64-85`: l'header è già `repr(C)`/`bytemuck` Pod, quindi il parsing al livello TCP è già un cast bit-level.

### SDK TypeScript

- `sdk/ts/src/brokers/store.ts:14-22`: `MAP_SET` scrive `string(key) . u8(flags) . [u64(ttl)] . any(value)`.
- `sdk/ts/src/codec.ts:200-232`: `any(value)` fa una catena di `if/else` e, se il valore non è `Uint8Array | ArrayBuffer | number (safe integer) | string`, cade in `JSON.stringify`, quindi alloca stringa JSON.
- `sdk/ts/src/connection.ts:194-199`: ogni richiesta alloca un nuovo buffer e scrive l'header campo per campo.

### SDK Python

- `sdk/py/src/nexo/brokers/store.py:29-43`: analogo a TS, `any(value)` delegato a `FrameWriter.any`.
- `sdk/py/src/nexo/codec.py:195-236`: `any()` è ancora più costosa perché `isinstance(data, bool)` viene catturato prima di `isinstance(data, int)` e codifica `True/False` come JSON (`json.dumps(True)` → 5 byte totali: `0x02` + `"true"`).
- `sdk/py/src/nexo/codec.py:291-300`: `finish()` costruisce l'header con assegnazioni singole e due `struct.pack_into` separati.

---

## Opportunità bitwise con alto impatto

### 1. Packing di TTL e tag in un singolo `u64` (`Entry`)

**Problema**: `Option<Instant>` allarga `Entry` e introduce due branch su ogni `get`:

```rust
if let Some(expiry) = entry.expires_at {   // branch A
    if Instant::now() >= expiry {           // branch B
        return None;
    }
}
```

**Proposta**: sostituire `expires_at: Option<Instant>` con un unico `u64` in cui il bit più significativo o il valore `u64::MAX` indica “nessuna scadenza” e i bit rimanenti contengono i nanosecondi (o tick) di scadenza. La struttura diventa:

```rust
struct Entry {
    value: Bytes,
    expires_at: u64, // u64::MAX == no expiry
}
```

**Vantaggi bitwise**:

- Un solo `cmp` unsigned `entry.expires_at > now_ns` (con `now_ns` derivato da `Instant`) diventa quasi branchless.
- `Entry` si restringe da ~48/56 byte a ~40 byte (dipende da `Bytes`), migliorando la densità in cache di `DashMap`.
- La `retain` di pulizia diventa `|_, e| e.expires_at > now_ns`, lineare e prevedibile per il branch predictor.

**Impatto**: principale su workload con TTL o con molte chiavi. Può spostare il throughput del 10-25% su cache-bound dataset e ridurre la latenza di `get`.

**Rischio**: richiede una funzione stabile per convertire `Instant` in un dominio numerico monotonico e persistente al reboot (se si vuole salvare su disco in futuro). Oggi lo Store è in-memory, quindi è fattibile.

---

### 2. Bit-packed `MAP_SET` flags: un byte che decide tutto

**Problema**: il byte `flags` in `MAP_SET` (`src/brokers/store/tcp.rs:47-48`) usa solo il bit 0; il tipo del valore è invece gestito dal **byte `DataType` dentro il valore** (`DataType.RAW/STRING/JSON/INT`), che:

- costringe `any()` a scrivere un byte di prefisso;
- costringe il server a non sapere il tipo del valore che sta per scrivere;
- costringe `decodeAny`/`decode_any` a fare uno `switch`/`elif` sul primo byte.

**Proposta**: ridefinire il byte `flags` di `MAP_SET` come maschera multi-bit:

| bit | significato |
|-----|-------------|
| 0   | `HAS_TTL`   |
| 1-2 | `VALUE_TYPE` (00=RAW, 01=STRING, 10=INT, 11=JSON/BOOL/special) |
| 3   | `SMALL_KEY` (lunghezza chiave ≤ 255, si usa `u8` invece di `u32`) |
| 4   | `SMALL_VALUE` (valore ≤ 65535 byte, si usa `u16` invece di `u32` per la lunghezza stringa) |
| 5-7 | riservati |

**Vantaggi**:

- Il server sa immediatamente, con una sola `&` e `>>`, come interpretare il payload seguente, senza leggere il valore “in seconda battuta”.
- Per numeri e booleani si può evitare il prefisso `DataType` e usare layout diretti: `i64` (8 byte), `u8` (1 bit/byte), ecc.
- Per chiavi/valori piccoli si risparmiano 3 byte per la lunghezza della chiave e 2 byte per la lunghezza del valore stringa.
- L'intero frame è più piccolo; riduce byte spediti, specialmente importante in SDK Python/TS.

**Impatto SDK**:

- `sdk/ts/src/brokers/store.ts` e `sdk/py/src/nexo/brokers/store.py` possono esporre metodi tipizzati (`setInt`, `setBool`, `setRaw`) che costruiscono il frame senza passare per `any(value)`.
- `any()` può comunque esistere, ma calcola una sola volta i flag e poi chiama il percorso rapido corrispondente, evitando la catena `if/else`.

**Rischio**: cambio del wire protocol → bump di `PROTOCOL_VERSION` e allineamento di tutti gli SDK, test, `codec-fixtures.json`. È un breaking change.

---

### 3. Valori piccoli “inline” nel puntatore (tagged value)

**Problema**: `Bytes` (`bytes::Bytes`) è 32 byte di metadati. Per valori piccoli (un intero, un booleano, poche decine di byte) l'overhead della heap allocation e dell'indirezione domina la memoria e la cache.

**Proposta bitwise**: usare un tagged pointer/pointer-size word per il valore, sfruttando il fatto che i puntatori heap sono allineati a 8 byte, quindi i 3 bit bassi sono liberi.

```rust
// Concettuale
struct TaggedValue(usize);
// bit 0..2: tag (inline/raw/arc/cow)
// se INLINE: i bit alti contengono fino a 7 byte di dati + un nibble di tipo
// se HEAP: il puntatore punta al buffer
```

Per valori interi, ad esempio, si può memorizzare direttamente l'`i64` in un `u64` (nessun heap). Per booleani, un bit basta. Per stringhe corte fino a 7 byte, si può embeddare senza allocare.

**Vantaggi**:

- Elimina `Arc`/heap allocation per la maggior parte dei valori numerici e delle flag.
- `Entry` può diventare una struttura di 16-24 byte (`key: Arc<str>` + `value: u64` + `expires: u64`), densissima in cache.
- `get` e `incr` diventano operazioni su registri e `DashMap` invece che dereferenziazioni heap.

**Impatto**: il vero “game changer” per workload numerici/contatori. Il throughput di `incr` e `get` potrebbe raddoppiare o triplicare su valori piccoli.

**Rischio**: alta complessità; va gestito il drop corretto, la clonazione e l'interazione con `Bytes`. Da prototipare con un `enum` Rust (`Small(u64)` vs `Heap(Bytes)`) prima di spingersi sui tagged pointer veri.

---

### 4. `MAP_INCR` senza allocazione e con valore inline

**Problema attuale** (`src/brokers/store/domain/map.rs:106-109`):

```rust
fn encode_int(val: i64) -> Bytes {
    let mut buf = vec![INT_PREFIX];       // alloca Vec
    buf.extend_from_slice(&val.to_be_bytes());
    Bytes::from(buf)
}
```

Ogni `incr` alloca un `Vec<u8>` da 9 byte e lo converte in `Bytes`.

**Proposta**:

- Se il valore è inline (tagged `u64` di tipo `INT`), `incr` diventa `fetch_add`/`checked_add` sullo stesso `u64` della `DashMap`, **zero allocazioni**.
- Se il valore è `Bytes` di 9 byte, il parsing (`parse_i64`) può usare `i64::from_be_bytes` direttamente su `raw[1..9]` senza copiare in un `[u8; 8]` intermedio.

**Impatto**: rimuove un'allocazione per ogni `incr`, che su 8M ops/sec diventa un notevole risparmio di tempo CPU e pressione GC.

---

### 5. Classificazione opcode con maschera a bit

**Problema** (`src/transport/tcp/dispatcher.rs:15-29`):

```rust
pub fn is_inline_opcode(opcode: u8) -> bool {
    matches!(opcode, OP_S_ACK | ... | OP_MAP_INCR | ...)
}
```

A runtime questo è un branch/match; il `dispatch` poi fa altri range check.

**Proposta**: pre-computare una `u128` o un array `[u64;4]` in cui ogni bit rappresenta un opcode.

```rust
const INLINE: [u64; 4] = [
    0b... , // opcode 0-63
    0b... , // 64-127
    0b... , // 128-191
    0b... , // 192-255
];

fn is_inline_opcode(opcode: u8) -> bool {
    (INLINE[(opcode >> 6) as usize] >> (opcode & 0x3F)) & 1 == 1
}
```

Anche il dispatch verso il broker si può fare con una tabella di funzioni indicizzata da `opcode`, eliminando la catena di `if range.contains`.

**Impatto**: riduce branch misprediction su ogni frame. È un micro-guadagno, ma a milioni di frame/sec si somma.

---

### 6. SDK TypeScript: `any()` con lookup table e `DataView` per header

**Problema**:

- `sdk/ts/src/codec.ts:36-52`: `decodeAny()` usa `switch` sul tipo.
- `sdk/ts/src/codec.ts:200-232`: `any()` usa una cascata `if/else` e, per oggetti generici, `JSON.stringify`.
- `sdk/ts/src/codec.ts:288-296`: `finish()` scrive 5 campi dell'header con 5 chiamate separate.

**Proposte bitwise**:

1. **Lookup table per `decodeAny`**: un array di 4-8 funzioni decodificatrici indicizzato direttamente dal byte `DataType` (mascherato con `type & 0x07`). Elimina lo `switch`.
2. **`any()` con flag bits**: il chiamante passa o calcola un tipo numerico (2 bit) e il writer lo usa in `set` senza `if/else`. Per `Map.set` si può aggiungere un overload `set<T>(key, value, { type: 'int' | 'bool' | 'raw' | 'json' })`.
3. **Header in una sola scrittura**: usare `DataView`/`Uint32Array` per scrivere i due `u32` dell'header in un'unica operazione bit-level invece di due `writeUInt32BE`.

**Impatto**: migliore latenza per richieste piccole, soprattutto con booleani e numeri. L'eliminazione di `JSON.stringify` sui booleani riduce anche la dimensione del frame da 5 byte a 1 byte.

---

### 7. SDK Python: evitare `json.dumps(True)` e batchare la serializzazione

**Problema** (`sdk/py/src/nexo/codec.py:205-211`):

```python
elif isinstance(data, bool):
    json_bytes = json.dumps(data, ...).encode("utf-8")
    # True -> 5 byte: 0x02 + b'true'
```

**Proposta**:

1. Aggiungere `DataType.BOOL = 0x04` e codificare `True/False` con un byte `0x01/0x00` (o un singolo bit dentro `MAP_SET` flags). Questo è il caso d'uso più evidente di “bitwise game changer” per i booleani: un bit invece di 5 byte + parsing JSON.
2. In `finish()`, sostituire le cinque scritture singole con un unico `struct.pack('>BBBII', version, frame_type, opcode, corr_id, payload_len)` e concatenare una sola volta.
3. In `any()`, usare una `match`/`dispatch` su un intero di tipo pre-calcolato (es. tramite `type_code = _TYPE_MAP[type(data)] & TYPE_MASK`) invece di `isinstance` a catena.

**Impatto**: Python è tipicamente il collo di bottiglia del client. Ridurre le chiamate a `struct.pack_into`, `isinstance` e `json.dumps` può dare il maggior beneficio assoluto tra i tre linguaggi.

---

## Impatto atteso (ordine di grandezza)

| Ottimizzazione | Throughput Rust | Throughput SDK | Complessità |
|-----------------|-----------------|----------------|-------------|
| TTL packed in `u64` | +10–25% | – | Media |
| Bit-packed `MAP_SET` flags | +5–15% | +10–30% | Alta (wire break) |
| Inline/tagged value | +50–200% su piccoli valori | 0% (server side) | Molto alta |
| `MAP_INCR` zero-alloc | +20–40% su `incr` | 0% | Media |
| Opcode bit mask | +2–5% | – | Bassa |
| SDK lookup table / no JSON bool | – | +15–40% latenza | Media |

Le percentuali sono stime qualitative basate sul codice attuale. Il throughput di riferimento (`tests/stress_tests.rs`) è ~8M ops/sec per `get`/`set` in locale.

---

## Requisiti trasversali

Tutte le proposte che toccano il wire format o i `DataType` richiedono:

1. Bump di `PROTOCOL_VERSION` in `src/transport/tcp/protocol/frame.rs` e in `sdk/*/protocol.ts|py`.
2. Aggiornamento di `sdk/codec-fixtures.json`.
3. Nuovi test in `tests/store_tests.rs`, `sdk/ts/tests/brokers/test-store.test.ts`, `sdk/py/tests/brokers/test_store.py`.
4. Aggiornamento di `sdk/integration-test-matrix.md` se si aggiungono nuovi opcodes/metodi.

---

## Conclusione

La risposta breve è **sì**, ma il “game changer” sta in tre mosse:

1. **Bit-packing in-memory**: rendere `Entry` il più piccolo possibile (`u64` per TTL, value inline/tagged) per massimizzare la cache di `DashMap`.
2. **Bit-packing sul wire**: rendere il byte `flags` di `MAP_SET` un vero controllo di flusso multi-bit, evitando il prefisso `DataType` e i branch `if/else`.
3. **SDK senza allocazioni superflue**: specialmente Python e TS, eliminare `JSON.stringify` per booleani e numeri piccoli usando tipi codificati a bit.

Le micro-ottimizzazioni bitwise (maschere opcode, `DataView` header, lookup table decode) sono la ciliegina, ma il vero salto di throughput arriva dalla riduzione di **allocazioni heap**, **dimensione in cache** e **byte sul filo**, tutte cose che si ottengono con un uso più aggressivo del bit-packing.
