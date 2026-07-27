# Stream broker: audit di correttezza e piano di ottimizzazione

Data audit: 2026-07-25 (aggiornato 2026-07-27)

## Executive summary

Il broker stream non va riscritto da zero. Il modello di dominio e valido:

- log append-only unificato;
- consumer group indipendenti;
- consegna at-least-once;
- ordering per chiave senza partizioni pubbliche;
- segmenti con CRC e retention per segmento.

I bug critici (P0/P1) sono stati risolti nei commit post-v4.1.4. Vedere sezione
"Finding risolti" per il dettaglio.

Rimangono aperti problemi di performance/scalabilita (non bloccanti per use case
attuali) e due finding P1/P2 minori. Vedere sezione "Finding rimanenti".

Verdetto: **modello funzionale mantenuto, bug critici risolti, refactor di
scalabilita rinviato a quando i volumi lo richiederanno**.

## Ambito analizzato

Core Rust:

- `src/brokers/stream/manager.rs`
- `src/brokers/stream/domain/group.rs`
- `src/brokers/stream/domain/persistence.rs`
- `src/brokers/stream/domain/topic.rs`
- `src/brokers/stream/tcp.rs`
- `src/transport/tcp/connection.rs`

SDK e protocollo:

- `sdk/ts/src/brokers/stream.ts`
- `sdk/ts/src/connection.ts`
- `sdk/py/src/nexo/brokers/stream.py`
- `sdk/py/src/nexo/connection.py`
- codec e test stream Rust, TypeScript e Python.

## Finding risolti (post-v4.1.4)

| Finding | Priorita | Risoluzione |
|---|---|---|
| STR-001 | P0 | `append_gate` serializza append; indice aggiornato solo dopo I/O completata via oneshot callback |
| STR-002 | P0 | `publish_batch` usa oneshot reply, attende I/O, propaga errori al publisher |
| STR-003 | P0 | `validate_topic_name` rifiuta path traversal, symlink, `..`, caratteri non allowlistati |
| STR-004 | P1 | `redeliver_entries` persistito in `GroupPersistentState`, restaurato al restart da `restore()` |
| STR-005 | P1 | Recovery tronca sempre a `valid_bytes` (anche zero), impone `MAX_STREAM_RECORD_BYTES`, verifica continuita, quarantena segmenti corrotti |
| STR-006 | P1 | `lifecycle_gate` + `append_gate` + `retention_gate` prevengono race create/delete/publish |
| STR-008 | P1 | `MAX_PUBLISH_BATCH = 65536` + check `count <= remaining / MIN_ITEM_BYTES` prima di allocare |
| STR-009 | P1 | `is_inline_opcode()` in `connection.rs`: ACK/LEAVE/SEEK/JOIN/ACK/NACK processati inline, solo FETCH/PUBLISH/CREATE/DELETE spawnati |
| STR-010 | P1 | SDK esistente aspetta callback prima di LEAVE quando `phase === 'processing'`; `active = false` + check in callback wrapper prevengono ACK post-LEAVE; STR-009 garantisce ordering server-side |
| STR-011 | P2 | Chiavi vuote rifiutate in Rust (`publish_batch`), TS SDK, Python SDK |
| STR-014 | P1 | `KeyState` rimosso quando idle in `ack`; `clamp_head` pulisce stale keys con `keys.retain()` |
| STR-018 | P2 | `dlt_key_counts: HashMap<Bytes, usize>` con decremento O(1); `redeliver_idx: BTreeSet` per O(1) lookup |

## Finding rimanenti

Le priorita usate sono:

- **P1:** bug funzionale grave, OOM, duplicazioni evitabili o stato non
  recuperabile;
- **P2:** scalabilita, contratto ambiguo o edge case non bloccante da solo.

### STR-007 - P1 - Coda storage e task di read illimitati (parzialmente risolto)

Il `mpsc::channel` e ora bounded (`config.storage_queue_capacity`), ma:

- ogni `ReadRange` genera ancora un `tokio::spawn` senza limite;
- un client puo aprire molti long-poll simultanei senza limite per sessione.

Fix rimanente: semaphore bounded per read I/O; limite in-flight per sessione.
Non bloccante per use case attuali (pochi consumer concorrenti).

### STR-012 - P2 - Errori create/delete/config vengono mascherati

`create_topic` logga alcuni errori filesystem e continua con `Ok(())`;
serializzazione e write di `config.json` possono essere ignorate. Anche delete
ignora errori di send/storage e `DropTopic` ignora `remove_dir_all` failure.

Ogni API amministrativa deve essere fail-fast e restituire un errore tipizzato.
Il topic non deve entrare nella registry finche directory, config e runtime non
sono creati con successo.

## Finding di complessita e memoria (non bloccanti per use case attuali)

### STR-013 - P1 - Indice completo per record e recovery completa

`TopicState.index` e una `BTreeMap<u64, u64>` con una entry per ogni record
retained. `recover_topic` legge e valida il payload di ogni record e ricostruisce
l'intera mappa a ogni startup.

Costi attuali:

- RAM: O(N record retained), oltre ai byte del log;
- startup: O(byte totali del log + N log N);
- retention: O(record eliminati log N) per rimuovere le entry.

Questo e il principale punto di rottura per stream grandi. Refactor:

- `SegmentMeta { start_seq, end_seq, size, path, generation }` in RAM;
- sparse index ogni K record, persistito in `.idx` per segmenti sealed;
- full index solo per active segment e record pending/redelivery;
- footer sealed con last seq, record count e checksum;
- startup O(numero segmenti + scan dell'active tail).

Target: RAM O(segmenti + N/K), lookup O(log segmenti + log checkpoint + K).
Non bloccante con volumi attuali.

### STR-015 - P1 - ReadRange e O(S log S + B*S)

`read_range` usa `partition_point` (binary search) per trovare il segmento.
Il catalog segmenti e in memoria (`state.segments`). `find_segments` fa
`read_dir` + sort ma e chiamato solo in retention/recovery, non a ogni fetch.

Non bloccante con pochi segmenti per topic.

### STR-016 - P1 - ACK per record serializza rete, task e lock

Ogni callback riuscita genera un frame, un task server (ora inline post-STR-009),
un lock del topic, una mutazione e una wake. Il costo e O(numero messaggi).

`ACK_BATCH`/`COMMIT_FETCH` ridurrebbe il costo a O(numero batch). Non bloccante
a basso throughput.

### STR-017 - P2 - Snapshot gruppi riscrive tutto lo stato del topic

Un solo `groups_dirty` causa snapshot di tutti i gruppi in un nuovo `state.log`.
Costo O(stato totale di tutti i gruppi) per snapshot. Non bloccante con pochi
gruppi per topic.

### STR-019 - P2 - Sweep globale ogni 100 ms

Il task redelivery itera tutti i topic+gruppi ogni 100ms. Le `deadlines` BTreeMap
evitano lavoro se non ci sono expired. Costo O(topic + gruppi) per tick. Non
bloccante con pochi topic/gruppi.

### STR-020 - P2 - Disconnect scansiona tutti i topic

`StreamManager::disconnect` visita ogni topic: O(topic) per disconnect. Non
bloccante con pochi topic.

### STR-021 - P2 - `try_advance_floor` ha spike lineari

Il while avanza una seq alla volta: O(ampiezza gap) per quella ACK. Ammortizzato
O(1) sul flusso. Spike solo con gap enormi di ACK out-of-order.

## SDK: ulteriori difetti di boundary

- `seek` mappa ogni valore runtime diverso da `beginning` a `end`, anche in
  Python dove il tipo e `str`. Deve validare esattamente i due valori.
- `batchSize == 0` e `waitMs == 0` sono accettati da subscribe. Insieme creano
  un tight polling loop. Servono limiti min/max e backoff per nonblocking poll.
- ACK fire-and-forget non rispetta backpressure socket: TypeScript ignora il
  boolean di `socket.write`, Python chiama `writer.write` senza `drain`.
- limiti `u16/u32/u64` non sono validati in modo uniforme. Il manager Rust
  pubblico puo inoltre ricevere key/payload che il record format tronca con cast.
- callback arity Python e inferita con `inspect.signature`; callable opachi o
  `partial` possono essere invocati con due argomenti e finire in redelivery
  permanente. Una API callback uniforme a due argomenti e piu semplice.

## Piano di refactor (rinviato)

I bug critici sono risolti. Il refactor architetturale (actor per topic, sparse
index, protocol vNext con COMMIT_FETCH/COMMIT_LEAVE) rimane valido come roadmap
di scalabilita ma non e piu bloccante. Da valutare quando:

- un topic supera milioni di record retained (STR-013);
- il numero di topic/gruppi cresce significativamente (STR-019, STR-020);
- si vuole ridurre il overhead per-record di ACK (STR-016);
- serve durability policy esplicita con fsync group commit.

## Benchmark eseguiti

Ambiente: macOS locale, build release per server/benchmark release, SDK eseguiti
in una copia isolata sotto `/tmp` per non modificare `data/` del repository.

Performance osservata:

| Benchmark | Risultato |
|---|---:|
| Rust publish release | 1,570,299 ops/s |
| TypeScript publish concorrente | 188,795 ops/s |
| TypeScript publish batch | 1,737,154 msg/s |
| TypeScript callback stream | 41,594 msg/s |
| TypeScript publish sequenziale | 27,633 ops/s |
| Python publish concorrente | 68,138 ops/s |
| Python publish batch | 474,277 msg/s |
| Python callback stream | 36,838 msg/s |
| Python publish sequenziale | 13,530 ops/s |

## Conclusione

I finding rimanenti (STR-007 parziale, STR-012, STR-013..STR-021) sono
ottimizzazioni di scalabilita e edge case minori, non bloccanti per i use case
attuali. Il refactor architetturale (actor per topic, sparse index, protocol
vNext) rimane come roadmap per quando i volumi lo richiederanno.
