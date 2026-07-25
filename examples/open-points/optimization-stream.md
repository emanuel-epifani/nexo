# Stream broker: audit di correttezza e piano di ottimizzazione

Data audit: 2026-07-25

## Executive summary

Il broker stream non va riscritto da zero. Il modello di dominio e valido:

- log append-only unificato;
- consumer group indipendenti;
- consegna at-least-once;
- ordering per chiave senza partizioni pubbliche;
- segmenti con CRC e retention per segmento.

Non e pero sufficiente un lavoro di fino prima della produzione. Il confine tra
stato in memoria, storage e protocollo va rifattorizzato. Oggi esistono race che
possono associare un offset al record sbagliato, publish confermate prima di
qualsiasi esito I/O, redrive DLT perse al riavvio, crescita RAM lineare rispetto
ai record e alle chiavi viste, e operazioni di lettura che riscansionano la
directory a ogni fetch.

Verdetto: **mantenere il modello funzionale, rifare ownership e persistence
pipeline**. La soluzione proposta e un actor bounded per topic, eventualmente
eseguito su storage shard, che possiede sequenze, segment catalog, commit
watermark e stato gruppi. Non serve introdurre partizioni visibili agli utenti
finche un singolo topic non supera la capacita di un core o di un device.

Prima della produzione considero bloccanti almeno `STR-001`..`STR-009`.

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

## Decisione architetturale

### Struttura proposta

Usare un **single owner per topic** per tutte le mutazioni del log e del gruppo.
L'owner puo essere un task Tokio per topic attivo o, con moltissimi topic, uno
di N storage shard che serializza i topic assegnati per hash.

Questa e applicazione locale del modello actor, non un framework o una nuova
gerarchia di pattern. Serve a risolvere un problema concreto: oggi sequenza,
offset, enqueue I/O, retention e delete hanno owner diversi e non formano una
transazione ordinata.

Responsabilita proposte:

```text
TCP session ordered admission
        |
        v
StreamManager: registry + routing + TopicName validation
        |
        v
TopicRuntime actor: seq, commit watermark, segment catalog,
                    groups, waiters, retention generation
        |
        v
SegmentStore: append/read/sync/rotate/delete, fault-injectable nei test
```

Il mailbox deve essere bounded. L'append assegna le sequenze e prepara un batch,
scrive, opzionalmente esegue group fsync, aggiorna indice e commit watermark, poi
risponde al publisher e sveglia i fetch. Mai il contrario.

### Alternative considerate

1. **Patch minima:** inviare `StorageCommand::Append` mentre il mutex del topic e
   ancora acquisito, cache dei segmenti e qualche validazione. Corregge parte
   delle race, ma lascia publish senza esito I/O, coda illimitata, indice RAM
   per record, recovery completa e ACK per messaggio. Utile solo come hotfix.
2. **Actor per topic o storage shard:** scelta raccomandata. Localizza le
   invarianti, permette group commit e backpressure, ed e testabile con uno
   store iniettato. Il costo e un refactor significativo del manager.
3. **Partizioni Kafka-like:** non necessarie ora e incompatibili con il valore
   distintivo del log unificato e dell'ordering broker-side per chiave. Diventano
   necessarie solo se un singolo topic deve superare il throughput sequenziale
   di un core/device.
4. **Database embedded per tutto il broker:** semplificherebbe transazioni e
   indici, ma aggiungerebbe write amplification e dipendenza da un engine. Un
   segment store dedicato resta piu coerente col workload append-only.

### Assunzioni di scala

Il refactor proposto mira a:

- milioni o decine di milioni di record retained per topic;
- migliaia di topic attivi e molte migliaia di gruppi complessivi;
- payload da poche decine di byte a circa 1 MiB;
- consumer group con backlog elevato e chiavi ad alta cardinalita;
- semantica at-least-once esplicita;
- un singolo nodo e un singolo writer logico per topic.

Il design smette di bastare quando un solo topic richiede piu throughput di un
writer sequenziale o quando serve replica sincrona multi-nodo. A quel punto
servono partizioni interne/raft e un commit index replicato, non altri lock nel
manager attuale.

## Finding di correttezza e sicurezza

Le priorita usate sono:

- **P0:** possibile corruzione, perdita dati confermati come persistiti o delete
  arbitraria;
- **P1:** bug funzionale grave, OOM, duplicazioni evitabili o stato non
  recuperabile;
- **P2:** scalabilita, contratto ambiguo o edge case non bloccante da solo.

### STR-001 - P0 - Sequenza, offset e ordine append possono divergere

Stato: confermato staticamente; il test concorrente non lo ha riprodotto in 20
esecuzioni, ma l'interleaving e valido.

In `StreamManager::publish_batch`, sotto `TopicState` lock vengono assegnate le
sequenze e calcolati gli offset. Il lock viene rilasciato prima di
`storage_tx.send(Append)`.

Interleaving possibile:

```text
T1: riserva seq 1, offset 0; unlock; viene sospeso
T2: riserva seq 2, offset L1; unlock; enqueue append(seq 2)
T1: enqueue append(seq 1)

file:  [seq 2][seq 1]
index: seq 1 -> 0, seq 2 -> L1
```

La read a offset 0 trova `seq 2`, fallisce il controllo `msg.seq == requested`,
e scarta silenziosamente il record. L'indice resta incoerente fino al restart;
dopo recovery l'ordine fisico e comunque diverso dall'ordine delle sequenze.

Hotfix: enqueue sincrono dentro il lock. Fix definitivo: assegnazione sequenza,
append e pubblicazione del nuovo commit watermark nello stesso TopicRuntime.

Test richiesto: `SegmentStore` con barrier tra reservation e append per forzare
deterministicamente T1/T2; verificare mapping `seq -> payload`, ordine fisico e
recovery.

### STR-002 - P0 - Publish risponde prima dell'I/O e gli errori sono persi

Stato: confermato dal codice.

`StorageCommand::Append` e fire-and-forget. Il manager:

- aggiorna `next_seq`, `file_offset` e `index` prima della scrittura;
- ignora l'errore di `storage_tx.send`;
- risponde con le sequenze prima di `open`, `write_all`, flush o fsync;
- nello storage logga gli errori ma non puo restituirli al publisher.

Un disco pieno, permission error, file descriptor error o storage task morto
produce quindi un publish riuscito con record assente. La wake dei consumer
avviene comunque. Anche `shutdown()` drena comandi, ma non esegue `sync_all` sui
segmenti o sul file di stato: protegge dal normale restart di processo, non da
power loss/kernel crash.

La configurazione deve dichiarare una durability policy:

```text
Memory   = risposta dopo enqueue bounded (non durable, nome esplicito)
Write    = risposta dopo write_all
Fsync    = risposta dopo group fsync; default raccomandato per stream durable
```

In tutti i casi indice e high watermark diventano visibili solo dopo il commit
scelto. Gli errori devono faultare il topic o essere restituiti; non possono
essere solo loggati.

### STR-003 - P0 - Path traversal nei nomi topic

Stato: confermato dal codice.

Il topic arriva dal wire e viene passato a `persistence_path.join(name)` senza
validazione in create, exists, delete, bootstrap e append. `DropTopic` usa poi
`remove_dir_all` sul path risultante. Valori come `../sibling`, path assoluti o
componenti multipli possono leggere, creare o cancellare fuori dalla root dello
stream con i permessi del processo.

Fix: introdurre un value object `TopicName` validato al confine TCP e riusato
dal manager. Deve essere un solo componente normale, non vuoto, con lunghezza
limitata e allowlist esplicita, ad esempio `[A-Za-z0-9._-]`, rifiutando `.` e
`..`. La validazione SDK migliora l'errore utente, ma quella server e obbligatoria.

Test richiesti: path assoluto, `..`, slash/backslash, NUL, nome vuoto, nome oltre
limite e symlink preesistente nella persistence root.

### STR-004 - P1 - `move_to_stream` viene perso al restart

Stato: riprodotto deterministicamente con un test temporaneo.

Una entry DLT e considerata completata da `try_advance_floor`, quindi
`ack_floor` puo superarne la sequenza. `move_to_stream` trasforma la entry in
`MsgState::Redeliver`, ma redeliver e volatile. Lo snapshot successivo salva un
floor gia avanzato e nessuna DLT; dopo restart la sequenza non appartiene piu a
nessuno stato e non viene consegnata.

Probe eseguito:

1. portare seq 1 in DLT;
2. chiamare `move_to_stream(1)`;
3. `shutdown()` immediato;
4. ricreare il manager e fare fetch;
5. risultato osservato: zero messaggi invece di seq 1.

Abbassare semplicemente `ack_floor` a `seq - 1` non e sufficiente: farebbe
riapparire anche record successivi gia ACKati. Due soluzioni corrette:

- persistere esplicitamente redelivery set e sparse ACK state;
- scelta raccomandata: redrive come append atomico di una copia in coda, con
  nuova sequenza e `original_seq`, poi rimozione DLT solo dopo il commit.

Il secondo approccio richiede protocol bump e `moveToStream` deve restituire la
nuova sequenza, ma evita rewind del cursore e semplifica il recovery.

### STR-005 - P1 - Recovery non garantisce un prefisso contiguo

Stato: confermato dal codice e in parte da probe.

Problemi distinti:

1. Se il primo `read_exact` legge solo 1-3 byte del length prefix, `read_record`
   restituisce `Eof`, non `UnexpectedEof`, e il file non viene troncato.
   Probe: segmento valido da 30 byte piu un byte; dopo recovery la size resta 31.
2. Se il primo record del segmento e corrotto, `valid_bytes == 0` impedisce la
   `set_len(0)`. Il segmento resta corrotto a ogni restart.
3. Il `len: u32` letto dal disco viene usato in `vec![0; len]` senza limite.
   Un tail corrotto o un file manipolato puo richiedere fino a circa 4 GiB per
   un singolo record e causare OOM.
4. Dopo corruzione in un vecchio segmento, recovery continua sui segmenti
   successivi. Il log puo quindi avere un buco. `fetch` cerca esattamente
   `next_deliver_seq`; se quella seq manca, non avanza al successore presente e
   il gruppo resta fermo per sempre.

Policy raccomandata: recovery del **massimo prefisso contiguo valido**. Al primo
record invalido:

- troncare sempre a `valid_bytes`, incluso zero;
- eliminare/quarantinare i segmenti successivi, oppure faultare il topic e
  richiedere un comando esplicito di salvage;
- imporre `MAX_RECORD_BYTES` prima di allocare;
- verificare continuita di sequenza, segment start/end e footer.

Silenziosamente saltare il buco non e compatibile con ack floor e ordering.

### STR-006 - P1 - Delete/create/publish non hanno una lifecycle generation

Stato: confermato staticamente.

Un publish puo trattenere un `Arc<TopicShared>`, mentre delete rimuove il topic e
accoda `DropTopic`. Se l'append arriva dopo il drop, `handle_append` ricrea la
directory e resuscita un topic senza config. In modo simile, una create
concorrente puo costruire la nuova directory prima che il vecchio DropTopic la
cancelli.

Fix: tutte le operazioni lifecycle passano dallo stesso TopicRuntime e portano
una `topic_generation`. Delete scrive un tombstone, chiude/cancella waiters,
attende append in-flight, chiude handle e rimuove i file. I comandi della vecchia
generation vengono rifiutati. Create successiva parte solo dopo il commit del
delete e usa una nuova generation.

### STR-007 - P1 - Coda storage e task di request/read sono illimitati

Stato: confermato dal codice.

- `mpsc::unbounded_channel` conserva payload e metadata finche il writer non li
  processa;
- ogni ReadRange genera un nuovo `tokio::spawn`;
- ogni frame della connessione genera un task nel `JoinSet`;
- un client puo aprire molti long poll contemporanei senza limite per sessione.

Il benchmark Rust misura proprio la velocita di produzione di questo backlog,
non il throughput del disco. Quando producer > writer, memoria e task crescono
con il backlog: O(comandi in-flight), senza backpressure.

Fix: mailbox bounded in byte, non solo in numero di comandi; limite in-flight per
sessione; semaphore bounded per read I/O; risposta `BUSY/BACKPRESSURE` o await
della capacity. Metriche obbligatorie: queue bytes, oldest command age, fsync
latency, open readers e waiter count.

### STR-008 - P1 - Count batch non validato prima dell'allocazione

Stato: confermato dal codice.

`StreamCommand::parse(OP_S_PUB)` legge un `u32` e chiama immediatamente
`Vec::with_capacity(count)`. Un frame minuscolo puo dichiarare `u32::MAX` e
tentare una riserva enorme prima che il parser scopra che gli item non esistono.

Fix:

- `MAX_PUBLISH_BATCH` esplicito;
- verificare `count <= remaining_bytes / MIN_ITEM_BYTES` prima di allocare;
- `try_reserve` e errore di protocollo invece di panic/abort;
- limiti equivalenti per nomi, group, key, reason, fetch e response count;
- fuzz test sui parser Rust e fixture negative nei due SDK.

### STR-009 - P1 - ACK e LEAVE possono essere applicati fuori ordine

Stato: confermato staticamente.

La socket legge i frame in ordine, ma `handle_connection` spawna ogni dispatch
in un task indipendente. ACK(seq) seguito da LEAVE puo quindi essere applicato
come LEAVE poi ACK:

1. LEAVE rimuove il member e rimette il pending in redelivery;
2. ACK trova `NOT_MEMBER`;
3. essendo fire-and-forget, l'SDK non vede l'errore;
4. una callback gia completata viene eseguita di nuovo.

Serializzare ingenuamente tutta la connessione non funziona: un FETCH long-poll
bloccherebbe proprio il LEAVE che deve cancellarlo. Serve ordered admission:

- la connection loop assegna/enqueue i comandi nell'ordine TCP;
- FETCH registra un waiter e restituisce una future senza bloccare l'admission;
- ACK/SEEK/LEAVE vengono applicati in ordine dal topic owner;
- il completamento della response puo restare concorrente.

La proposta protocollo in questo documento elimina inoltre ACK/LEAVE separati.

### STR-010 - P1 - Stop SDK puo duplicare side effect

Stato: confermato in TypeScript e Python; Python ha un ulteriore timeout fisso.

Entrambi gli SDK impostano `active = false` e inviano LEAVE prima di attendere
le callback gia in-flight. Una callback gia iniziata puo completare il side
effect e inviare ACK quando il server ha gia rimosso il member; l'ACK fallisce in
silenzio e il messaggio viene riconsegnato.

Python, dopo due secondi, cancella anche `_loop_task`: callback legittimamente
piu lente di due secondi vengono interrotte e restano senza ACK. TypeScript
attende senza il timeout fisso, quindi i due SDK non hanno la stessa semantica.

Fix protocollo raccomandato:

- ACK riusciti accumulati per batch;
- `COMMIT_FETCH` applica ACK/range e registra il fetch successivo in una sola
  richiesta confermata;
- `COMMIT_LEAVE` applica gli ultimi ACK e rimuove il member atomicamente;
- callback fallite non entrano nell'ACK batch;
- stop blocca nuovi callback, attende quelli iniziati, poi `COMMIT_LEAVE`.

In alternativa serve almeno `CANCEL_FETCH` che cancella il long-poll senza
rimuovere il member, seguito da ACK confermati e LEAVE.

### STR-011 - P2 - Chiave vuota collassa in assenza di chiave

Stato: riprodotto deterministicamente nel core; vale anche per entrambi gli SDK.

Il wire e il record format usano `key_len == 0` per rappresentare `None`.
`Some(b"")`, stringa vuota e `Uint8Array(0)` tornano quindi come `None` e perdono
silenziosamente l'ordering per chiave.

Probe: publish con `Some(Bytes::new())`; read restituisce `key == None`.

Breaking change consigliata: `key_present: u8` seguito da `key_len: u32` e bytes.
In alternativa vietare esplicitamente chiavi vuote nei tre boundary. Il formato
con presence bit e preferibile perche una chiave e dichiarata opaque bytes.

### STR-012 - P2 - Errori create/delete/config vengono mascherati

`create_topic` logga alcuni errori filesystem e continua con `Ok(())`;
serializzazione e write di `config.json` possono essere ignorate. Anche delete
ignora errori di send/storage e `DropTopic` ignora `remove_dir_all` failure.

Ogni API amministrativa deve essere fail-fast e restituire un errore tipizzato.
Il topic non deve entrare nella registry finche directory, config e runtime non
sono creati con successo.

## Finding di complessita e memoria

### STR-013 - P1 - Indice completo per record e recovery completa

`TopicState.index` e una `BTreeMap<u64, u64>` con una entry per ogni record
retained. `recover_topic` legge e valida il payload di ogni record e ricostruisce
l'intera mappa a ogni startup.

Costi attuali:

- RAM: O(N record retained), oltre ai byte del log;
- startup: O(byte totali del log + N log N);
- retention: O(record eliminati log N) per rimuovere le entry;
- minimo record circa 26 byte piu payload, quindi log con moltissimi record
  piccoli puo avere un indice dello stesso ordine di grandezza del log.

Questo e il principale punto di rottura per stream grandi. Refactor:

- `SegmentMeta { start_seq, end_seq, size, path, generation }` in RAM;
- sparse index ogni K record, persistito in `.idx` per segmenti sealed;
- full index solo per active segment e record pending/redelivery;
- footer sealed con last seq, record count e checksum;
- startup O(numero segmenti + scan dell'active tail), con verifica completa
  opzionale in background/offline;
- seek al checkpoint precedente e scan massimo K record.

Target: RAM O(segmenti + N/K), lookup O(log segmenti + log checkpoint + K), con
K configurabile e piccolo/costante.

### STR-014 - P1 - Stato chiavi cresce con ogni chiave distinta ACKata

In `ConsumerGroup::ack`, dopo aver liberato `in_flight`, un `KeyState` vuoto non
viene rimosso. `clamp_head` potrebbe pulirlo, ma ritorna subito quando head non e
cambiata; senza retention ogni gruppo conserva quindi tutte le chiavi mai viste.

RAM: O(numero di chiavi uniche per gruppo), anche quando non esiste alcun
messaggio attivo. Con gruppi multipli il costo si moltiplica.

Inoltre una hot key lenta accumula tutte le seq successive in
`KeyState.blocked`: O(backlog della chiave). Non conta contro
`max_ack_pending`, quindi il broker puo leggere e parcheggiare un backlog molto
piu grande del limite dichiarato.

Fix:

- rimuovere immediatamente `KeyState` quando non e in-flight, blocked o
  poisoned;
- includere blocked nel budget/backpressure;
- evitare di materializzare l'intero backlog della hot key: mantenere solo il
  prossimo candidato o un cursore per chiave, derivato da indice persistente;
- metriche per active keys e blocked seqs.

### STR-015 - P1 - ReadRange e O(S log S + B*S)

Ogni read:

1. esegue `read_dir` sull'intera directory;
2. ordina nuovamente i segmenti: O(S log S);
3. per ogni seq cerca il segmento con `rposition`: worst case O(B*S);
4. apre file fuori dalla LRU dello StorageManager;
5. ordina offset e risultato: O(B log B).

Con catalog in memoria e location `(segment_id, offset)` il costo diventa
O(B + segmenti toccati), oppure O(B log S) con binary search. Per fresh fetch
contigui e preferibile una singola read sequenziale dal primo offset, non un
seek per record.

### STR-016 - P1 - ACK per record serializza rete, task e lock

Ogni callback riuscita genera un frame fire-and-forget, un task server, un lock
del topic, una mutazione e una wake. Il costo e O(numero messaggi) in syscalls,
dispatch e lock contention anche quando fetch e callback sono batch.

`ACK_BATCH`/`COMMIT_FETCH` riduce il costo a O(numero batch). Le sequenze ACKate
possono essere codificate come range piu eccezioni. Il server applica il batch
in una mutazione e risponde con il nuovo committed floor.

### STR-017 - P2 - Snapshot gruppi riscrive tutto lo stato del topic

Un solo `groups_dirty` causa snapshot di tutti i gruppi, DLT e parked keys in un
nuovo `state.log`. Una modifica in un gruppo costa O(stato totale di tutti i
gruppi) e il costo si ripete a ogni intervallo.

Refactor: per-topic group WAL con record batch (`ACK_RANGE`, `DLT_PUT`,
`DLT_DELETE`, `SEEK`) e snapshot/compaction periodica. In alternativa file per
gruppo con dirty set per gruppo. WAL e group ACK batch si rafforzano a vicenda.

### STR-018 - P2 - Cleanup DLT per chiave puo diventare quadratico

`cleanup_poisoned_key` scansiona tutti i `msgs` per sapere se esiste un'altra
DLT con la stessa key. Cancellare/redrive D entry una alla volta puo costare
O(D^2). Anche `peek_dlt` scansiona la mappa unificata e applica offset lineare.

Mantenere:

- `dlt: BTreeMap<seq, DltEntry>` separata;
- `dlt_count_by_key: HashMap<Bytes, usize>`;
- decremento O(1), unpoison quando il contatore arriva a zero;
- pagination con cursor `after_seq`, non offset numerico.

### STR-019 - P2 - Sweep globale ogni 100 ms

Il task redelivery itera tutti i topic, prende ogni topic lock e visita tutti i
gruppi ogni 100 ms, anche senza pending. Costo O(topic + gruppi) per tick e lock
contention periodica.

Usare la deadline minima del TopicRuntime in `tokio::select!` oppure una
`DelayQueue` centrale keyed per gruppo. Il lavoro diventa O(log pending groups)
per schedule e O(expired) al risveglio.

### STR-020 - P2 - Disconnect scansiona tutti i topic

`StreamManager::disconnect` visita ogni topic per trovare binding della
sessione: O(topic) per disconnect. Una registry globale
`session_id -> [(topic_generation, group, consumer)]`, aggiornata in ordered
admission, rende il cleanup O(binding reali).

### STR-021 - P2 - `try_advance_floor` ha spike lineari

Quando si chiude il primo gap dopo molti ACK out-of-order, il while avanza una
sequenza alla volta: O(ampiezza gap) per quella ACK, pur essendo ammortizzato
sul flusso complessivo. Un ordered set/range set degli outstanding permette di
calcolare il nuovo floor in O(log pending) senza spike proporzionale al backlog.

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

## Protocollo stream vNext proposto

Il cambio e giustificato perche elimina race e riduce drasticamente il numero di
frame. Richiede bump di `PROTOCOL_VERSION` e aggiornamento simmetrico Rust/TS/PY.

### Publish

```text
PUBLISH_BATCH(topic, durability, items[])
item = key_present:u8, key_len:u32, key, payload_len:u32, payload

response = first_seq:u64, count:u32, committed_at:u64, durability:u8
```

Il server limita item count e byte totali. La risposta arriva solo al livello di
durability richiesto.

### Fetch e commit

```text
COMMIT_FETCH(
  topic, group, member_id, generation,
  ack_ranges[], limit, wait_ms
)

response = committed_floor, delivery_token, messages[]
```

Il server prima applica gli ACK della batch precedente, poi registra il fetch.
Per stop:

```text
COMMIT_LEAVE(topic, group, member_id, generation, ack_ranges[])
```

L'operazione e atomica e confermata. Non esiste piu la finestra ACK/LEAVE.

### DLT redrive

```text
REDRIVE(topic, group, original_seq) -> new_seq
```

Il TopicRuntime legge il record originale, appende una nuova entry con metadata
`original_seq`, committa, poi rimuove la DLT e aggiorna lo state WAL. Se append
fallisce, la DLT resta intatta.

## Segment store proposto

```rust
struct SegmentMeta {
    id: u64,
    start_seq: u64,
    end_seq: u64,
    size: u64,
    generation: u64,
    sparse_index: Vec<(u64, u64)>,
}

struct TopicRuntime {
    next_seq: u64,
    committed_seq: u64,
    active: ActiveSegment,
    sealed: Vec<SegmentMeta>,
    groups: HashMap<GroupId, GroupState>,
    mailbox_bytes: usize,
}
```

Il codice concreto puo differire; le invarianti non devono:

1. una seq appartiene a un solo record;
2. offset e segmento diventano visibili solo dopo append riuscita;
3. `committed_seq` non supera mai il record fisicamente committato;
4. segment catalog e retention hanno un solo owner;
5. nessun comando della generation cancellata puo scrivere;
6. recovery produce un prefisso contiguo o un errore esplicito;
7. mailbox, read e waiter hanno limiti osservabili.

## Piano di refactor

### Fase 0 - Correttezza e sicurezza immediata

- `TopicName` validato e limiti parser prima delle allocazioni;
- hotfix ordine enqueue sotto lock;
- Append con oneshot result; errore I/O propagato;
- indice/wake aggiornati solo dopo write riuscita;
- strict recovery, record length cap, truncate anche a zero;
- lifecycle tombstone/generation;
- conservare e attendere JoinHandle in shutdown;
- test deterministici con store fault-injectable.

Questa fase riduce il rischio, ma non e il target finale di scalabilita.

### Fase 1 - Ownership e backpressure

- estrarre `TopicRuntime` actor o storage shard;
- mailbox bounded per bytes;
- group append e fsync;
- segment catalog in memoria;
- retention coordinata dall'actor;
- ordered admission delle richieste stream.

### Fase 2 - Indice e recovery scalabili

- sparse sidecar index e sealed footer;
- scan startup limitato all'active tail;
- read sequenziale per fresh batch;
- binary search per segment e bounded random reads;
- tool offline `verify/salvage` per corruzione.

### Fase 3 - Group state e protocollo vNext

- `COMMIT_FETCH`, `COMMIT_LEAVE`, ACK ranges;
- DLT separata e counter per key;
- state WAL + snapshot/compaction;
- redrive append-to-tail atomico;
- key presence bit;
- SDK stop drain identico in TS/Python.

### Fase 4 - Scheduler e osservabilita

- deadline-driven redelivery;
- session binding registry;
- metriche commit/fsync/backlog/recovery;
- benchmark durable e memory profiles;
- aggiornamento claims e documentazione.

## Test obbligatori prima della produzione

Rust unit/integration:

- append T1/T2 forzata con barrier;
- open/write/sync failure senza seq visibili o buchi;
- kill del processo nei punti before-write, after-write e after-fsync;
- truncation a ogni byte del frame, inclusi 1-3 byte del length prefix;
- len dichiarata `u32::MAX` senza allocazione enorme;
- corruzione nel primo record, segmento intermedio e active tail;
- redrive seguito da restart immediato;
- delete/create/publish concorrenti con generation fencing;
- retention mentre una read usa il segmento;
- milioni di unique keys con memoria che torna al baseline dopo ACK;
- hot key backlog rispettando il limite;
- mailbox piena con backpressure e nessuna OOM;
- fuzz di frame e record parser.

Parita TypeScript/Python e matrice:

- stop durante callback lenta: side effect una volta e commit confermato;
- commit+leave atomico con batch parzialmente riuscita;
- reconnect con ACK non confermato;
- chiave vuota round-trip oppure rifiuto identico;
- seek e limiti invalidi rifiutati localmente;
- redrive restituisce new seq;
- batch max e payload max;
- protocol fixture per ogni nuovo opcode/layout.

I restart test devono chiamare sempre `shutdown()` oppure usare un processo
separato. Oggi diversi test fanno solo `drop(manager)`, ma i background task
mantengono sender/token e possono lasciare vivo il vecchio storage manager sullo
stesso path; questo rende alcuni test di recovery meno affidabili.

## Benchmark eseguiti

Ambiente: macOS locale, build release per server/benchmark release, SDK eseguiti
in una copia isolata sotto `/tmp` per non modificare `data/` del repository.

Correttezza:

- Rust stream integration: 63/63 pass;
- TypeScript stream integration: 27/27 pass;
- Python stream integration: 26/26 pass;
- test concorrente Rust rafforzato a 64 publisher su runtime multi-thread,
  validando anche `seq -> payload`: pass ripetuto;
- probe redrive/restart: fail, zero messaggi dopo restart;
- probe partial prefix: fail, 31 byte invece di 30;
- probe empty key: fail, `None` invece di `Some(empty)`.

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

Questi numeri **non sono throughput durable**:

- Rust termina il timer dopo enqueue e attende solo 200 ms, senza verificare
  count/hash dopo restart;
- publish SDK riceve la seq prima che lo storage abbia scritto;
- `SUBSCRIBE+ACK` incrementa il contatore nella callback, mentre ACK e
  fire-and-forget e non viene verificato `ack_floor`.

I claim attuali fino a circa 1.9M stream ops/s vanno rinominati in
`accepted/enqueued throughput` oppure sospesi. Il benchmark corretto deve:

1. pubblicare N record con payload/seq hash noto;
2. attendere commit/fync confermato;
3. fermare e riavviare il processo;
4. verificare count, sequenze e hash;
5. per consume, verificare server-side committed floor dopo reconnect;
6. riportare throughput, p50/p99, RSS, queue peak e bytes fsync.

## Criteri di accettazione del refactor

Correttezza:

- nessun publish riuscito senza il livello di durability richiesto;
- nessuna seq visibile prima del commit;
- nessuna inversione tra sequenza e ordine fisico;
- nessuna DLT/redrive persa dopo crash nei punti documentati;
- ACK/LEAVE non possono riordinarsi;
- recovery contigua o errore esplicito, mai stall silenzioso.

Complessita:

- lookup segment O(log S), fresh batch O(B + log S);
- RAM index O(S + N/K), non O(N);
- key state O(chiavi attive), non O(chiavi storiche);
- DLT key cleanup O(1)/O(log N);
- redelivery O(expired log G), non sweep O(T*G) ogni 100 ms;
- disconnect O(binding sessione), non O(topic);
- memoria di ingress/read/waiter sempre bounded.

Testing:

- fault injection e crash test verdi;
- Rust, TypeScript, Python e `integration-test-matrix.md` allineati;
- protocol version bump e fixture simmetriche;
- benchmark durable prima/dopo, inclusi stress Rust/TS/Python;
- documentazione aggiornata con semantica at-least-once e durability levels.

## Conclusione

La parte da salvare e importante: il log unificato e la state machine per gruppo
sono una buona direzione e non richiedono un ritorno a Kafka-like partitions.
La parte da rifare e altrettanto netta: un durable stream non puo avere sequenze,
indice e risposta client separati dall'esito dello storage, ne un indice completo
in RAM per tutta la retention.

Quindi non e una riscrittura da zero del broker, ma e un refactor architetturale
prima della produzione. Dopo ownership per topic, commit pipeline, sparse index
e batch ACK, il resto torna a essere lavoro di fino misurabile.