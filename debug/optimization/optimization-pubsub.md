# Ottimizzazione PubSub via bitwise — analisi profonda

> File di analisi, **nessuna modifica al codice** è stata apportata.
> Focus: capire se (e dove) l'uso massiccio di bitmap / maschere bit-a-bit può diventare un *game changer* per il throughput del broker `pub-sub`, sia sul server Rust che — forse ancora di più — sugli SDK TypeScript e Python.

---

## 1. Panorama attuale e numeri di riferimento

I benchmark esistenti in `tests/stress_tests.rs` danno un'ottima baseline (profilo release, macOS):

| Scenario | Throughput | Dettaglio |
|---|---|---|
| PubSub exact match | ~5.03M ops/sec | 1 publisher, 1 subscriber, topic fisso |
| PubSub wildcard `+` | ~4.41M ops/sec | 1 publisher, 1 subscriber su pattern `bench/+/metric` |
| PubSub fan-out 1→100 | ~180K ops/sec | 1 publisher, 100 subscriber sullo stesso topic |

La differenza di un ordine di grandezza tra *single-subscriber* e *fan-out* ci dice esattamente dov'è il collo di bottiglia: **non nella ricezione/invio di un singolo messaggio, ma nel matching, nella deduplica e nella distribuzione a molti clienti**. È lì che le operazioni bit-a-bit possono fare la differenza più grande.

---

## 2. Server Rust — dove il bitwise può cambiare le cose

### 2.1 Flusso critico di `PubSubManager::publish`

Il percorso caldo è in `src/brokers/pub-sub/manager.rs` (righe 223-268):

1. Validazione del topic (`split('/')`, check di ogni segmento).
2. Lock in scrittura / lettura sul radix tree.
3. `radix_tree.match_subscribers()` — raccoglie `Arc<str>` in un `Vec`.
4. Deduplica via `HashSet<Arc<str>>` (`matched.retain(|id| seen.insert(id.clone()))`).
5. Per ogni sottoscrittore: `clients.get(client_id.as_ref())` su `DashMap`, poi `mpsc::Sender::try_send()`.

Il costo non è nel tree-walk in sé (è logaritmico sul numero di livelli), ma nelle allocazioni e copie che genera ogni publish:

- `Vec<Arc<str>>` da riempire.
- `HashSet<Arc<str>>` per deduplicare subscription sovrapposte (stesso client su più pattern).
- `Arc::clone()` di stringhe client id per ogni sottoscrittore trovato.
- Lookup hash in `DashMap` per ogni cliente finale.

Nel benchmark fan-out 1→100, stiamo pagando queste allocazioni + lookup per ognuno dei 100 destinatari, **anche se i destinatari sono noti a priori**.

### 2.2 La proposta bitwise: subscriber bitmap + senders array piatto

L'idea è sostituire gli insiemi di `Arc<str>` con **insiemi di indici client rappresentati come bitmap**, e i client con **indici densi `u32`** in una struttura piatta.

#### Schema concettuale

```text
connect(client)  -> assegna client_index: u32 (da uno Slab/Vec con free-list)
disconnect(idx)  -> rilascia l'indice

Node.subscribers: BitSet   // un bit per client_index
Node.plus_child / hash_child: come oggi

publish(topic):
    1. tree.match_subscribers(parts, &mut output_bitset)
       → per ogni nodo visitato: output.bit_or(&node.subscribers)
    2. iteri i bit impostati in output_bitset
       → senders[idx].try_send(msg.clone())
```

#### Vantaggi concreti

1. **Deduplica implicita**: `OR` di bitset è idempotente. Se lo stesso client è raggiunto da più pattern, il bit è 1 una sola volta. Sparisce l'intero `HashSet` di deduplica.
2. **Niente più `Arc<str>` nel matching**: gli indici sono `u32`, copiabili con `Copy`. Non servono allocazioni nel percorso caldo.
3. **Lookup O(1) senza hash**: `senders[idx]` è un accesso array, molto più amichevole per la cache rispetto a `DashMap::get`.
4. **Cache friendly**: l'unione di bitmap è un loop su `u64` (8 byte) alla volta, con istruzioni `OR` a 64 bit. Per 100 subscriber servono solo 2 parole (`u64`). Per 1000 subscriber, 16 parole.
5. **Minore pressione sul GC / allocator**: meno `Vec` e `HashSet` creati e distrutti per publish.

#### Impatto atteso

- **Fan-out denso** (1→100 / 1→1000): potenziale miglioramento di **2×-5×** sul server, perché il costo del dispatch diventa quasi solo il costo dei `try_send` sul canale (che è il vero floor fisico).
- **Single-subscriber** (5M ops/sec): miglioramento marginale (~5-15%), perché lì il percorso è già molto snello.
- **Matching wildcard**: il tree-walk rimane, ma l'accumulo dei risultati diventa `OR` di bitmap invece di `Vec::extend` di `Arc`. Miglioramento moderato ma misurabile.

#### Implementazione suggerita (solo bozza)

In `src/brokers/pub-sub/domain/radix_tree.rs`:

```rust
pub(crate) struct Node {
    pub(crate) children: HashMap<String, Node>,
    pub(crate) plus_child: Option<Box<Node>>,
    pub(crate) hash_child: Option<Box<Node>>,
    pub(crate) subscribers: BitSet,  // es. Vec<u64> o RoaringBitmap
    pub(crate) retained: Option<RetainedMessage>,
}

pub(crate) fn match_subscribers(&self, parts: &[String], out: &mut BitSet) {
    if let Some(hash) = &self.hash_child {
        out.or(&hash.subscribers);
    }
    if parts.is_empty() {
        out.or(&self.subscribers);
        return;
    }
    // ... stessa logica di oggi, ma con OR invece di Vec::extend
}
```

In `src/brokers/pub-sub/manager.rs`:

```rust
let mut matched = bitset_pool.acquire();
{
    let root = tree.read();
    root.match_subscribers(&parts, &mut matched);
}
for idx in matched.iter_set_bits() {
    if let Some(info) = clients.get_by_index(idx) {
        if info.sender.try_send(msg.clone()).is_err() { ... }
    }
}
```

### 2.3 Scelta della libreria bitmap

- **`bitvec`** / **`bit-set`** o un semplice `Vec<u64>`: minimo overhead per set piccoli/densi.
- **`roaring`**: migliore se il numero di client totali è molto maggiore del numero di sottoscrittori per nodo (sparsità). Comprime automaticamente e unisce container con `OR`. Aggiunge una dipendenza esterna.

Suggerimento: iniziare con un `Vec<u64>` dinamico (o `BitSet` interno) e misurare; passare a `roaring` solo se la sparsità diventa un problema di memoria.

### 2.4 Altri piccoli usi di bitwise nel server

- **Flags del comando `PUB`** in `src/brokers/pub-sub/tcp.rs`: già bitwise (`0x01` retain, `0x02` TTL, `0x04` clear). È corretto e compatto.
- **Stato dei canali / slow consumer**: il check `try_send().is_err()` è binario; non c'è molto altro da comprimere.
- **Frame header `meta`**: oggi per i push è `0`. Si potrebbe usare come bit-field per segnalare batch/multi-push, ma il vantaggio è minore rispetto al dispatch bitmap.

---

## 3. SDK — dove il bitwise è ancora più promettente

Il server può scalare con più core, ma il client è spesso single-threaded (Node.js event loop o Python GIL). Se un client ha molte subscription wildcard, il dispatch in-bound diventa rapidamente un collo di bottiglia CPU.

### 3.1 TypeScript — `pubsub.ts`

In `sdk/ts/src/brokers/pubsub.ts` (righe 130-143):

```typescript
private dispatch(topic: string, data: any) {
  const exactSub = this.exact.get(topic);
  if (exactSub) this.enqueue(exactSub, data);

  if (this.wild.size === 0) return;
  const tParts = topic.split('/');
  for (const { parts, sub } of this.wild.values()) {
    if (NexoPubSub.matchesParts(parts, tParts)) {
      this.enqueue(sub, data);
    }
  }
}
```

**Problemi:**

- Per ogni push con almeno una wildcard, splitta il topic.
- Ciclo lineare su **tutte** le wildcard registrate, anche se il 99% non può mai corrispondere.
- `matchesParts` fa confronti stringa per ogni segmento concreto.

**Proposta bitwise:**

1. **Indicizzare le wildcard** per numero di livelli e per primo segmento concreto.
2. **Rappresentare ogni pattern con una maschera a 64 bit** che indica quali posizioni sono wildcard (`+`/`#`) e quali sono concrete.
3. **Pre-calcolare un hash a 64 bit per ogni segmento concreto del pattern**.
4. All'arrivo di un topic, calcolare gli hash dei suoi segmenti una sola volta; poi, per ogni candidato, verificare:
   ```
   (topic_hashes ^ pattern_hashes) & ~wild_mask == 0
   ```
   Questa è un'operazione XOR + AND + compare su interi, molto più veloce di `string == string` ripetuto.

#### Esempio semplificato

Pattern `a/+/b/#` (4 livelli):
- `wild_mask = 0b0110` (posizioni 1 e 3 sono wildcard)
- `hash[0] = hash("a")`, `hash[2] = hash("b")`

Topic `a/x/b/c/d`:
- `topic_hash[0] = hash("a")`, `topic_hash[2] = hash("b")`, altri non confrontati per `#`
- XOR mascherato: se posizioni concrete coincidono, il risultato è 0.

#### Impatto atteso

- Con **poche wildcard** (< 10): miglioramento modesto, forse 1.2×-1.5×.
- Con **molte wildcard** (> 100, scenario IoT/dashboard): potenziale **5×-20×** sul dispatch, perché si passa da O(numero wildcard totali) a O(numero wildcard candidate).

### 3.2 Python — `pubsub.py`

In `sdk/py/src/nexo/brokers/pubsub.py` (righe 153-164):

```python
def _enqueue(self, topic: str, data: Any) -> None:
    sub = self._exact.get(topic)
    if sub is not None:
        sub.queue.put_nowait(data)
    if not self._wild:
        return
    t_parts = topic.split("/")
    for parts, sub in self._wild.values():
        if self._matches_parts(parts, t_parts):
            sub.queue.put_nowait(data)
```

**Problema aggiuntivo rispetto a TS**: il **GIL**. `_enqueue` e `_matches_parts` girano tutto nel thread principale asyncio. Se `_matches_parts` fa molti confronti stringa, blocca il loop per decine/migliaia di microsecondi, rallentando anche le altre coroutine.

**Stessa cura:** indice + maschere bit. In Python si possono usare:

- `int` nativo a precisione arbitraria come maschera a 64/128 bit.
- `functools.reduce` o `operator.xor` per il confronto XOR mascherato.
- `topic.split('/')` una sola volta e caching dei segmenti hash.

#### Impatto atteso in Python

A causa del GIL, ridurre il lavoro CPU per dispatch in-bound è **particolarmente importante**. Con 50+ wildcard, il passaggio a matching bitwise + indice può dare un **3×-10×** di throughput in-bound.

### 3.3 Decodifica del push

In `sdk/ts/src/connection.ts` (righe 175-181):

```typescript
case FrameType.PUSH_PUBSUB: {
  const pushCursor = new Cursor(payload);
  const topic = pushCursor.readString();
  const data = pushCursor.decodeAny();
  this.onPush(topic, data);
}
```

- `readString()` è obbligatoria (serve il topic).
- `decodeAny()` chiama `JSON.parse` se il tipo è `DataType.JSON`. Questo è un costo fisso non eliminabile, ma per payload raw/binari si evita.
- Non c'è molto spazio per bitwise qui, se non usare `DataView`/`Uint32Array` per leggere header e lunghezza. Il vantaggio è marginale rispetto all'indice wildcard.

In `sdk/py/src/nexo/connection.py` (righe 137-142) la stessa logica: `read_string()` + `decode_any()` con `json.loads`. Anche qui il grosso è JSON, non il framing.

---

## 4. Cosa NON è un game changer

È importante non sparare troppo in alto. Questi punti non migliorerebbero significativamente il throughput:

- **Header frame 11 byte**: già decodificato efficientemente in Rust con `bytemuck`/`try_from_bytes` e in TS/Python con `Buffer`/`struct`. Bitwise manuale non darebbe vantaggi misurabili.
- **Flags del comando PUB**: già bitwise, corretti.
- **Topic validation (`split('/')`)**: il costo è O(livelli) e succede una volta per publish. Non vale la pena micro-ottimizzarlo con maschere.
- **Retained messages / cleanup / SQLite flush**: sono percorsi asincroni e sporadici, non nel percorso caldo del publish.

---

## 5. Rischi e vincoli

### 5.1 Server

- **Memoria**: bitmap per nodo possono diventare grandi se il numero totale di client è elevato. Con `Vec<u64>`: 8 byte × ceil(N/64) per ogni nodo con sottoscrittori. Se ci sono 100.000 client e 1.000 nodi, potrebbe essere ~12 MB (100.000/64 × 8 × 1.000), accettabile. Con milioni di client, usare `RoaringBitmap`.
- **Concorrenza**: l'indice cliente (`u32`) deve essere stabile durante il publish. `connect`/`disconnect` devono sincronizzare l'assegnazione/rilascio degli indici e l'aggiornamento dei bitset. Si può usare `parking_lot::RwLock` o una struttura lock-free con `AtomicPtr`.
- **Ciclo di vita**: quando un client si disconnette, il suo bit deve essere rimosso da **tutti** i nodi in cui è sottoscritto. Questo è lo stesso costo di oggi (`remove_subscriber`), ma in più bisogna pulire il bit. Se si mantiene un elenco di nodi per cliente, la pulizia è O(numero di pattern sottoscritti).

### 5.2 SDK

- **Collisioni hash**: se si usano hash 64-bit per confrontare i segmenti, la probabilità di collisione è bassissima ma non zero. Si può usare l'hash come filtro rapido e, in caso di match, fare il confronto stringa di fallback.
- **Compatibilità**: le wildcard `+` e `#` devono continuare a funzionare esattamente come oggi. L'indice bitwise deve replicare la semantica di `matchesParts`, incluso `#` in fondo che matcha qualsiasi suffisso.
- **Regressione**: ogni cambiamento al matching wildcard richiede test di regressione coprenti tutti gli scenari in `sdk/integration-test-matrix.md` (es. `pubsub_combined_wildcards`, `pubsub_wildcard_plus`, `pubsub_wildcard_hash`, `pubsub_retained_wildcard_*`).

---

## 6. Allineamento con le regole del progetto

Da `AGENTS.md`:

- **Cross-cutting alignment**: la proposta di bitmap impatta `src/`, `sdk/ts/`, `sdk/py/` in modo coerente (stessa semantica di matching). Non impatta `docs/` se non per eventuale documentazione opzionale.
- **Test parity**: se implementata, serve:
  - Unit test Rust per `BitSet` OR / iterazione bit.
  - Test di integrazione Rust in `tests/pubsub_tests.rs` su wildcard, fan-out, deduplica.
  - Test TS in `sdk/ts/tests/` e Python in `sdk/py/tests/` con molte wildcard.
  - Eventuale fuzz test sul parser/matcher bitwise.
- **Performance**: prima/dopo da misurare con `tests/stress_tests.rs`, `sdk/ts` stress, `sdk/py` stress.
- **Algorithm**: il matching bitmap rimane O(relevant patterns × levels) o O(nodes visited × word count), quindi rispetta il vincolo O(log n) / O(1)-ish per operazioni bit.

---

## 7. Conclusione

**Sì, ci sono loghe bitwise che possono essere un game changer per il throughput PubSub, ma in due zone specifiche e con impatto diverso:**

1. **Server Rust — dispatch a molti subscriber**: sostituire `HashSet<Arc<str>>` con **bitmap di indici cliente** nel radix tree elimina deduplica esplicita, allocazioni e lookup `DashMap` per fan-out. È il punto più forte per carichi 1→N.
2. **SDK TS/Python — dispatch in-bound wildcard**: sostituire la scansione lineare con **indice + maschere bit su hash di segmenti** rende il matching wildcard cache-friendly e riduce il carico CPU, specialmente in Python con il GIL. È il punto più forte per client con molte subscription wildcard.

Non sono ottimizzazioni universali: su un singolo subscriber o pochi wildcard il vantaggio è marginale. Ma appena il sistema scala in numero di subscriber e pattern, il passaggio a rappresentazioni bit-packed può spostare il collo di bottiglia dal software ai canali di rete / al loop di eventi, che è esattamente dove un broker deve arrivare.
