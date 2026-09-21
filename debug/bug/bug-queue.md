# Queue — Bug nelle logiche end-to-end

> **Stato:** analisi e documentazione; nessuna correzione applicata.
> **Ambito:** server Rust, protocollo TCP, persistenza/recovery, SDK TypeScript e Python, interoperabilità dei payload.
> **Priorità:** diciotto problemi: due P0, otto P1 e otto P2.

Ogni bug è contenuto in una sezione inizialmente chiusa. Nei renderer che supportano l'attributo HTML `name` di `<details>`, aprire una sezione chiude automaticamente quella precedente.

## Vista architetturale

```text
push
SDK anyWithLen → Q_PUSH → parser TCP → QueueManager → QueueState → QueueStore/SQLite → OK

consume
SDK poll → Q_CONSUME → QueueState ready→in-flight → risposta batch → callback
                                                          ├─ successo → ACK
                                                          └─ errore   → NACK

retry
lease scaduta → sweeper → ready oppure DLQ → replay/delete/purge

restart
SQLite + config.json → recover QueueState/DlqState → ripresa delle lease e dei contatori
```

L'invariante centrale è duplice:

1. ogni transizione visibile in RAM deve essere persistita nello stesso ordine logico;
2. un ACK deve identificare una sola specifica consegna, anche dopo restart.

## Evidenze eseguite

- Suite corrente: `cargo test --test queue_tests -- --test-threads=1` → **44 passati, 0 falliti**.
- Cinque probe Rust temporanei e un probe Python sono stati eseguiti separatamente: tutti falliscono sull'invariante attesa e confermano **QUEUE-01, 02, 03, 06, 07 e 08**.
- Il file temporaneo `tests/queue_review_repro.rs` è stato rimosso; non è parte del repository.
- **QUEUE-04, 05 e 09..18** sono finding statici ad alta confidenza sul codice corrente. Le sezioni indicano la regressione minima necessaria.
- Sono stati verificati anche questi comportamenti runtime: Node converte `writeUInt8(1.5)` in `1` e `writeUInt8(NaN)` in `0`; timer Node oltre `2^31-1` ms vengono ridotti a `1` ms; un `asyncio.Future` è awaitable ma non è una coroutine; Python serializza `NaN`, mentre `JSON.parse('NaN')` fallisce; un `INT` pari a `2^60` perde precisione convertito in `number`.
- Le suite E2E complete degli SDK non sono state rieseguite durante questa analisi; i relativi test e flow sono stati ispezionati staticamente.

### Indice

| ID | Priorità | Attore principale | Evidenza | Sintesi |
| --- | --- | --- | --- | --- |
| QUEUE-01 | P0 | Server + SDK TS/Py | Probe Rust | `Q_CONSUME` accetta `u32::MAX` e prealloca il batch |
| QUEUE-02 | P0 | Server + SDK TS/Py | Probe Rust | Nessun budget byte sulle risposte consume/DLQ |
| QUEUE-03 | P1 | Server | Probe Rust | `delivery_token` riutilizzato dopo restart |
| QUEUE-04 | P1 | Server/persistenza | Statica + storia Git | Ordine RAM/SQLite nuovamente concorrente |
| QUEUE-05 | P1 | Server/persistenza | Statica | Errore storage dopo mutazione lascia RAM divergente |
| QUEUE-06 | P1 | Server/recovery | Probe Rust | Config corrotta o assente sostituita dai default correnti |
| QUEUE-07 | P1 | Server + SDK TS/Py | Probe Rust + statica | Visibility timeout zero o eccessivo crea lease invalide |
| QUEUE-08 | P1 | SDK Python | Probe Python | `Future` e awaitable custom ACKati prima del completamento |
| QUEUE-09 | P1 | Server + SDK TS/Py | Statica | La cancellazione client non cancella il long-poll server |
| QUEUE-10 | P2 | Server + SDK TS/Py | Statica | Delete non interrompe i consume già pendenti |
| QUEUE-11 | P1 | SDK TS/Py + server | Statica | ACK/NACK fire-and-forget: `stop()` non ne conosce l'esito |
| QUEUE-12 | P2 | SDK TypeScript | Statica | `pushBatch` ignora il limite protocollo di 10.000 item |
| QUEUE-13 | P2 | SDK TypeScript | Probe runtime + statica | Validazione numerica incompleta e polling patologico |
| QUEUE-14 | P2 | SDK TS/Py | Probe runtime + statica | Interoperabilità non lossless per interi e JSON non finito |
| QUEUE-15 | P2 | Server/DLQ | Statica | Paginazione DLQ `O(offset)` sotto lock |
| QUEUE-16 | P2 | Server/lifecycle | Statica | Drop del manager non termina sweeper e writer |
| QUEUE-17 | P2 | Server/filesystem | Statica | Protezione symlink incompleta sul file di configurazione |
| QUEUE-18 | P2 | SDK TypeScript | Statica | Encoder UUID non verifica esattamente 16 byte |

<details name="queue-bugs">
<summary><strong>QUEUE-01 · P0 — `Q_CONSUME` accetta un batch capace di esaurire la memoria</strong></summary>

### Comportamento atteso

Il server deve rifiutare il `batch_size` prima di qualsiasi allocazione quando supera un limite esplicito del protocollo.

### Riproduzione confermata

È stato inviato al parser un payload minimo con `batch_size = u32::MAX` e `wait_ms = 0`.

```text
Atteso:   ParseError
Ottenuto: Ok(Consume { batch_size: 4294967295, ... })
```

Il percorso successivo esegue `Vec::with_capacity(max)` anche se la queue è vuota. La richiesta è di pochi byte, ma può chiedere centinaia di GB virtuali e provocare panic, abort dell'allocator o OOM del processo.

Entrambi gli SDK controllano soltanto che il batch sia positivo; nessuno applica un massimo di consume.

### Correzione e regressioni

- Aggiungere `QUEUE_MAX_CONSUME_BATCH_SIZE` a `protocol.json`.
- Validarlo nel parser Rust, poi negli SDK.
- Non preallocare più di `min(max, ready_len)` come difesa aggiuntiva.
- Testare `max`, `max + 1` e `u32::MAX` senza entrare nel manager.

### Riferimenti

- [`src/brokers/queue/tcp.rs:155`](../../src/brokers/queue/tcp.rs#L155) — parsing senza limite.
- [`src/brokers/queue/domain/queue.rs:184`](../../src/brokers/queue/domain/queue.rs#L184) — preallocazione richiesta dal client.
- [`protocol.json:139`](../../protocol.json#L139) — esiste il limite push, non quello consume.
- [`sdk/ts/src/brokers/queue.ts:386`](../../sdk/ts/src/brokers/queue.ts#L386) e [`sdk/py/src/nexo/brokers/queue.py:577`](../../sdk/py/src/nexo/brokers/queue.py#L577) — nessun limite superiore.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-02 · P0 — Consume e peek DLQ possono costruire risposte senza limite di byte</strong></summary>

### Comportamento atteso

`MAX_PAYLOAD_SIZE` deve limitare anche i frame in uscita. Il server deve scegliere un batch che rientri nel budget prima di riservare i messaggi e prima di copiarne i payload.

### Riproduzione confermata

Un `NexoCodec` configurato con massimo `4` byte ha codificato con successo una risposta da `5` byte:

```text
Atteso:   errore encoder
Ottenuto: frame accettato
```

Il limite è verificato soltanto dal `Decoder`. `encode_consume_batch` e `encode_peek_dlq` concatenano tutti i payload senza budget. Più messaggi singolarmente validi possono quindi produrre una risposta enorme; oltre `u32::MAX`, il cast della lunghezza altera anche il framing.

### Impatto

- OOM durante la costruzione della risposta.
- Risposta oltre il limite dichiarato dal server.
- Messaggi già marcati in-flight ma mai consegnati se l'encoding o la write fallisce.
- Stesso problema per payload e `failure_reason` della DLQ.

### Correzione e regressioni

- Introdurre un budget encoded-byte in consume e peek DLQ.
- Aggiungere un controllo finale nell'encoder, come difesa e non come unico controllo.
- Definire paginazione/continuation quando il prossimo item non entra nel frame.
- Testare molti payload che singolarmente stanno sotto 10 MB ma insieme lo superano.

### Riferimenti

- [`src/protocol/codec.rs:22`](../../src/protocol/codec.rs#L22) — limite solo in decode.
- [`src/protocol/codec.rs:69`](../../src/protocol/codec.rs#L69) — encoder senza controllo.
- [`src/brokers/queue/tcp.rs:235`](../../src/brokers/queue/tcp.rs#L235) — consume encoding.
- [`src/brokers/queue/tcp.rs:246`](../../src/brokers/queue/tcp.rs#L246) — DLQ encoding.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-03 · P1 — Un restart riutilizza il `delivery_token` e rende valido un ACK vecchio</strong></summary>

### Comportamento atteso

Una nuova consegna dello stesso messaggio deve avere un token diverso da qualsiasi consegna precedente, anche dopo requeue e restart.

### Riproduzione confermata

```text
prima consegna:       token 1
NACK e requeue:       token persistito 0
restart
seconda consegna:     token 1
ACK con il token vecchio: accettato
```

Il probe ha ottenuto:

```text
stale token 1 was accepted for the post-restart delivery token 1
```

### Causa e impatto

NACK e timeout azzerano il token. Il recovery ricostruisce il contatore prendendo il massimo dei soli token correntemente persistiti; se tutti i messaggi sono ready o in DLQ, riparte da zero.

Un callback lento appartenente alla vecchia consegna può quindi completare dopo reconnect e cancellare la consegna corrente posseduta da un altro worker. Il controllo anti-stale funziona nello stesso processo, ma non attraversa il restart.

### Correzione e regressioni

- Persistire un contatore monotono per queue, oppure conservare l'ultimo token sul messaggio ready.
- Coprire restart dopo NACK, timeout e replay DLQ.
- Verificare che un vecchio ACK/NACK sia sempre rifiutato mentre la nuova lease resta valida.

### Riferimenti

- [`src/brokers/queue/domain/queue.rs:134`](../../src/brokers/queue/domain/queue.rs#L134) — recovery del contatore.
- [`src/brokers/queue/domain/queue.rs:202`](../../src/brokers/queue/domain/queue.rs#L202) — NACK.
- [`src/brokers/queue/domain/queue.rs:237`](../../src/brokers/queue/domain/queue.rs#L237) e [`src/brokers/queue/domain/queue.rs:276`](../../src/brokers/queue/domain/queue.rs#L276) — reset a zero.
- [`src/brokers/queue/domain/queue.rs:289`](../../src/brokers/queue/domain/queue.rs#L289) — nuova assegnazione.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-04 · P1 — Le transizioni RAM e le operazioni SQLite possono essere riordinate</strong></summary>

### Comportamento atteso

Se due transizioni sullo stesso messaggio sono serializzate dal lock della queue, le rispettive `StorageOp` devono entrare nel writer nello stesso ordine.

### Scenario

```text
Task A push:    lock → RAM ready → unlock ─────────────→ enqueue INSERT
Task B consume:        lock → RAM in-flight → unlock → enqueue UPDATE
```

Su executor multi-thread, `UPDATE` può entrare nel canale prima di `INSERT`. SQLite aggiorna zero righe e poi inserisce lo snapshot ready. Un crash ripristina quindi ready un messaggio che in RAM era in-flight; combinazioni con DELETE possono far riapparire un messaggio ACKato.

La storia Git conferma la regressione: `5fc39f0` aveva corretto QUE-002 mettendo mutazione ed enqueue nello stesso ordine; `c3ae501` ha spostato tutti gli `await` fuori dal lock per introdurre backpressure, riaprendo la finestra.

### Correzione e regressioni

- Non mantenere un `parking_lot::MutexGuard` attraverso `await`.
- Riservare prima un permit del canale bounded, poi mutare e inviare sincronicamente sotto il lock; in alternativa usare un sequencer/Tokio mutex per queue.
- Aggiungere hook/barrier deterministici nei test per forzare `push→consume`, `timeout→consume` e `replay→consume` concorrenti, poi verificare il DB dopo restart.

### Riferimenti

- [`src/brokers/queue/manager.rs:408`](../../src/brokers/queue/manager.rs#L408) — push: unlock prima di `Insert`.
- [`src/brokers/queue/manager.rs:439`](../../src/brokers/queue/manager.rs#L439) — pop: unlock prima di `UpdateState`.
- [`src/brokers/queue/manager.rs:547`](../../src/brokers/queue/manager.rs#L547) — batch consume.
- [`src/brokers/queue/domain/persistence.rs:99`](../../src/brokers/queue/domain/persistence.rs#L99) — enqueue asincrono bounded.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-05 · P1 — Un errore storage lascia applicata la mutazione in RAM</strong></summary>

### Comportamento atteso

Se l'enqueue storage fallisce, l'operazione deve essere un no-op visibile oppure la queue deve passare esplicitamente in stato unhealthy. Non deve restare una mezza transazione.

### Causa e casi concreti

Il manager muta prima lo stato e solo dopo attende `store.execute`:

- push fallito lascia il messaggio consumabile;
- consume fallito lascia messaggi in-flight mai restituiti al client;
- replay/delete/purge DLQ restituiscono errore dopo aver già modificato la RAM;
- ACK/NACK e timeout registrano soltanto il problema e comunicano comunque successo/no-op al chiamante.

Il caso si attiva quando il receiver del writer è chiuso, durante race di shutdown o dopo il fallimento del task writer. Gli errori SQLite di flush sono inoltre solo loggati: non esiste un segnale di queue unhealthy verso il manager.

### Correzione e regressioni

- Riservare capacità e verificare la salute del writer prima della mutazione.
- Rendere atomici mutazione + enqueue, con rollback se l'operazione non può essere consegnata.
- Propagare i fallimenti permanenti del writer alle API successive.
- Testare ogni operazione con writer chiuso e verificare sia RAM sia recovery.

### Riferimenti

- [`src/brokers/queue/manager.rs:389`](../../src/brokers/queue/manager.rs#L389) — push.
- [`src/brokers/queue/manager.rs:458`](../../src/brokers/queue/manager.rs#L458) — ACK ignora l'errore storage.
- [`src/brokers/queue/manager.rs:640`](../../src/brokers/queue/manager.rs#L640) — replay.
- [`src/brokers/queue/manager.rs:674`](../../src/brokers/queue/manager.rs#L674) e [`src/brokers/queue/manager.rs:700`](../../src/brokers/queue/manager.rs#L700) — delete/purge DLQ.
- [`src/brokers/queue/domain/persistence.rs:229`](../../src/brokers/queue/domain/persistence.rs#L229) — flush senza feedback al manager.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-06 · P1 — Config corrotta o assente viene sostituita silenziosamente dai default correnti</strong></summary>

### Comportamento atteso

Una queue persistita deve riaprire con la configurazione originale oppure non registrarsi. Non deve cambiare lease e policy DLQ a seguito di un file corrotto.

### Riproduzione confermata

Queue iniziale: `visibility_timeout_ms=12345`, `max_deliveries=7`. Dopo aver corrotto `config.json` e riavviato con default `54321/9`:

```text
Ottenuto: QueueDefinition { visibility_timeout_ms: 54321, max_deliveries: 9 }
```

Lo stesso fallback viene usato quando il file manca o non è leggibile. Contraddice il comportamento documentato secondo cui le queue esistenti ignorano i nuovi default d'ambiente.

### Correzione e regressioni

- Fallire chiuso su config mancante, illeggibile o invalida.
- Salvare la configurazione nello stesso SQLite oppure con write atomica `temp → fsync → rename`.
- Testare file mancante, JSON troncato, schema invalido e permesso negato.

### Riferimenti

- [`src/brokers/queue/manager.rs:106`](../../src/brokers/queue/manager.rs#L106) — fallback ai default.
- [`src/brokers/queue/manager.rs:334`](../../src/brokers/queue/manager.rs#L334) — write non atomica della config.
- [`docs/guide/queue.md:256`](../../docs/guide/queue.md#L256) — configurazione snapshot documentata.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-07 · P1 — Visibility timeout zero o eccessivo produce lease non valide</strong></summary>

### Comportamento atteso

Il server, unica autorità del dominio, deve accettare soltanto timeout che producano una lease futura rappresentabile.

### Riproduzione confermata

Con `visibility_timeout_ms=0`, il pop assegna `visible_at=now`; l'ACK immediato viene rifiutato perché la lease è già scaduta. Il messaggio viene riconsegnato fino alla DLQ anche quando il callback ha avuto successo.

Per valori vicini a `u64::MAX`, `now + visibility_timeout_ms` può inoltre andare in overflow: panic con overflow checks o wrap a una scadenza errata in release.

### Correzione e regressioni

- Definire e generare limiti `min=1` e `max` coerenti con server e SDK.
- Validare anche i default caricati dall'ambiente.
- Usare `checked_add` come ultima difesa.
- Testare zero, massimo, massimo+1 e creazione cross-SDK.

### Riferimenti

- [`src/brokers/queue/domain/queue.rs:72`](../../src/brokers/queue/domain/queue.rs#L72) — opzioni accettate senza validazione.
- [`src/brokers/queue/domain/queue.rs:170`](../../src/brokers/queue/domain/queue.rs#L170) — lease scaduta rifiutata.
- [`src/brokers/queue/domain/queue.rs:293`](../../src/brokers/queue/domain/queue.rs#L293) — addizione non checked.
- [`sdk/ts/src/brokers/queue.ts:73`](../../sdk/ts/src/brokers/queue.ts#L73) e [`sdk/py/src/nexo/brokers/queue.py:113`](../../sdk/py/src/nexo/brokers/queue.py#L113) — nessuna regola di dominio equivalente.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-08 · P1 — Python ACKa `Future` e awaitable custom prima che terminino</strong></summary>

### Comportamento atteso

Qualunque risultato awaitable del callback deve completare con successo prima dell'ACK. Un'eccezione dell'awaitable deve produrre NACK.

### Riproduzione confermata

Un callback ha restituito un `asyncio.Future` non risolto. Il probe ha osservato l'ACK mentre il Future era ancora pending:

```text
AssertionError: ACK was emitted while the callback Future was unresolved
```

`asyncio.iscoroutine` riconosce gli oggetti coroutine, non tutti gli awaitable. `Future`, `Task` e oggetti con `__await__` possono quindi continuare o fallire dopo che il job è già stato cancellato dal broker.

### Correzione e regressioni

- Usare `inspect.isawaitable(result)`, già usato dal broker Pub/Sub Python.
- Testare Future risolto, Future con eccezione, Task e awaitable custom.
- Verificare che `stop()` attenda il risultato e l'ACK conseguente.

### Riferimenti

- [`sdk/py/src/nexo/brokers/queue.py:461`](../../sdk/py/src/nexo/brokers/queue.py#L461) e [`sdk/py/src/nexo/brokers/queue.py:472`](../../sdk/py/src/nexo/brokers/queue.py#L472) — callback e test `iscoroutine`.
- [`sdk/py/src/nexo/brokers/pubsub.py:410`](../../sdk/py/src/nexo/brokers/pubsub.py#L410) — implementazione corretta con `inspect.isawaitable`.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-09 · P1 — Abort e timeout SDK non cancellano il long-poll sul server</strong></summary>

### Comportamento atteso

Quando una subscription annulla un consume, il lavoro server associato deve terminare oppure essere strettamente limitato.

### Causa end-to-end

- TypeScript rimuove la Promise da `pending` quando l'`AbortSignal` scatta.
- Python cancella il task locale e rimuove il Future da `_pending`.
- Nessuno invia un comando di cancellazione al server.
- Il server mantiene il `Q_CONSUME` nel `JoinSet` fino a messaggio o deadline; la risposta tardiva viene ignorata dal client.
- Il `JoinSet` non ha un limite di richieste concorrenti per connessione.

Ripetuti subscribe/stop sulla stessa connessione accumulano quindi task e Arc server temporaneamente. Il caso peggiora in Node per wait oltre `2^31-1` ms: il timer client viene ridotto a 1 ms, mentre il server conserva il long-poll per settimane.

Il disconnect della socket abortisce i task; il problema riguarda cancellazioni e timeout mantenendo aperta la connessione.

### Correzione e regressioni

- Aggiungere cancellazione server correlata al request ID, oppure un token di sessione per consume.
- Limitare sempre le richieste pendenti per connessione con un semaforo.
- Testare ripetuti start/stop e timeout, osservando che il numero di task torna a zero.

### Riferimenti

- [`sdk/ts/src/transport/tcp/connection.ts:212`](../../sdk/ts/src/transport/tcp/connection.ts#L212) — timeout/abort solo locali.
- [`sdk/py/src/nexo/transport/tcp/connection.py:249`](../../sdk/py/src/nexo/transport/tcp/connection.py#L249) — cancellazione solo locale.
- [`src/transport/tcp/connection.rs:68`](../../src/transport/tcp/connection.rs#L68) — `JoinSet` non bounded.
- [`src/transport/tcp/connection.rs:140`](../../src/transport/tcp/connection.rs#L140) — abort soltanto al disconnect.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-10 · P2 — Delete non sveglia i consume già pendenti</strong></summary>

### Comportamento atteso

La cancellazione della queue deve far terminare subito i long-poll esistenti con `RESOURCE_NOT_FOUND`.

### Causa e impatto

`consume_batch` conserva un `Arc<QueueShared>`. `delete_queue` rimuove la voce dalla `DashMap`, chiude lo store e cancella i file, ma non marca lo shared come deleted e non notifica i waiter.

Un consume già parcheggiato resta quindi attivo fino al proprio `wait_ms`. Con il default SDK sono fino a 20 secondi. Se una queue con lo stesso nome viene ricreata prima del poll successivo, la vecchia subscription può proseguire sulla nuova incarnazione senza osservare la delete.

Il test corrente elimina la queue mentre è ancora attivo un callback da 500 ms e poi attende due secondi: non copre un long-poll vuoto già parcheggiato.

### Correzione e regressioni

- Inserire tombstone/cancellation token in `QueueShared` e notificare tutti i waiter durante delete.
- Ritornare not-found dal consume risvegliato.
- Testare delete con poll da 20 secondi e delete+recreate immediata.

### Riferimenti

- [`src/brokers/queue/manager.rs:359`](../../src/brokers/queue/manager.rs#L359) — delete senza notify/tombstone.
- [`src/brokers/queue/manager.rs:530`](../../src/brokers/queue/manager.rs#L530) — consume conserva lo shared.
- [`sdk/ts/tests/brokers/test-queue.test.ts:64`](../../sdk/ts/tests/brokers/test-queue.test.ts#L64) e [`sdk/py/tests/brokers/test_queue.py:80`](../../sdk/py/tests/brokers/test_queue.py#L80) — copertura con deadline corta.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-11 · P1 — ACK/NACK fire-and-forget rende `stop()` non end-to-end</strong></summary>

### Comportamento atteso

Dopo un callback iniziato, `stop()` deve poter distinguere tra ACK/NACK applicato, rifiutato e non inviato. In caso contrario non è un drain affidabile.

### Causa e impatto

Entrambi gli SDK inviano ACK e NACK come `REQUEST_NO_RESPONSE` e proseguono subito. Il server calcola un booleano, ma non invia il frame. `stop()` attende il loop locale, non l'applicazione server né la persistenza.

Se la connessione chiude subito dopo, il server può abortire quel task nel `request_set`. Un callback riuscito verrà allora riconsegnato; un NACK perso diventa timeout e perde il `failure_reason`. Anche stale token, queue rimossa e storage failure restano invisibili.

La consegna at-least-once ammette duplicati durante failure, ma una chiusura dichiarata graceful non dovrebbe nascondere l'esito delle conferme già iniziate.

### Correzione e regressioni

- Rendere ACK/NACK richieste attese dal loop, almeno durante il drain.
- Propagare il `false` e gli errori storage alla subscription.
- Aggiungere gli equivalenti queue dei test stream: stop durante callback, ACK failure durante stop e failure mentre active.

### Riferimenti

- [`sdk/ts/src/brokers/queue.ts:143`](../../sdk/ts/src/brokers/queue.ts#L143) — ACK/NACK TS.
- [`sdk/py/src/nexo/brokers/queue.py:227`](../../sdk/py/src/nexo/brokers/queue.py#L227) — ACK/NACK Python.
- [`sdk/ts/src/transport/tcp/connection.ts:252`](../../sdk/ts/src/transport/tcp/connection.ts#L252) e [`sdk/py/src/nexo/transport/tcp/connection.py:275`](../../sdk/py/src/nexo/transport/tcp/connection.py#L275) — write senza risposta.
- [`src/transport/tcp/connection.rs:165`](../../src/transport/tcp/connection.rs#L165) — risposta soppressa.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-12 · P2 — TypeScript costruisce batch oltre il limite prima che il server li rifiuti</strong></summary>

### Comportamento atteso

Il limite generato `QUEUE_MAX_PUSH_ITEMS=10000` deve essere applicato da entrambi gli SDK prima della serializzazione.

### Causa e impatto

Python importa e controlla il limite. TypeScript serializza l'intero array senza controllo; solo il parser Rust lo rifiuta. Il server resta protetto, ma il client spreca CPU/memoria e può andare OOM prima di ricevere l'errore protocollo.

### Correzione e regressioni

- Importare il limite generato nel broker TS e fallire prima di `conn.send`.
- Verificare con uno spy che 10.001 item non costruiscano né scrivano alcun frame.

### Riferimenti

- [`protocol.json:139`](../../protocol.json#L139) — limite canonico.
- [`sdk/ts/src/brokers/queue.ts:111`](../../sdk/ts/src/brokers/queue.ts#L111) — nessun check.
- [`sdk/py/src/nexo/brokers/queue.py:547`](../../sdk/py/src/nexo/brokers/queue.py#L547) — check Python.
- [`src/brokers/queue/tcp.rs:126`](../../src/brokers/queue/tcp.rs#L126) — difesa server.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-13 · P2 — La validazione numerica TypeScript accetta valori patologici</strong></summary>

### Comportamento atteso

Opzioni pubbliche devono essere finite, intere e nel range del dominio prima di creare una subscription o un frame.

### Casi confermati/statici

- `priority=1.5` viene serializzato come `1`; `NaN` come `0`.
- `batchSize`, `concurrency` e `waitMs` non usano `Number.isInteger`/`isFinite`.
- `waitMs` non viene validato affatto.
- `waitMs=0` crea un loop RPC senza backoff perché ogni batch vuoto esegue subito `continue`; Python invece rifiuta zero.
- Wait sopra `2^31-1` supera il limite timer Node: il timeout diventa 1 ms mentre il server mantiene la richiesta lunga.
- Molti errori emergono solo dentro il loop dopo che `subscribe()` ha già restituito un handle, che passa rapidamente a terminal error.

### Correzione e regressioni

- Centralizzare validator TS per u8/u32, batch, concurrency e duration.
- Per `subscribe`, scegliere e documentare `waitMs >= 1`; mantenere zero soltanto per un'eventuale API one-shot non bloccante.
- Limitare il wait al massimo timer effettivo meno il margin.
- Testare negativi, frazioni, `NaN`, `Infinity`, zero e boundary massimi.

### Riferimenti

- [`sdk/ts/src/brokers/queue.ts:101`](../../sdk/ts/src/brokers/queue.ts#L101) — priority senza validator.
- [`sdk/ts/src/brokers/queue.ts:386`](../../sdk/ts/src/brokers/queue.ts#L386) — validazione parziale subscribe.
- [`sdk/ts/src/brokers/queue.ts:308`](../../sdk/ts/src/brokers/queue.ts#L308) — loop vuoto immediato.
- [`sdk/ts/src/protocol/codec.ts:112`](../../sdk/ts/src/protocol/codec.ts#L112) — `u8` delegato a Buffer.
- [`sdk/py/src/nexo/brokers/queue.py:577`](../../sdk/py/src/nexo/brokers/queue.py#L577) — validator Python più stretto.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-14 · P2 — Payload Python→TypeScript non sempre lossless o decodificabili</strong></summary>

### Comportamento atteso

Lo stesso payload prodotto da uno SDK deve conservare il valore quando consumato dall'altro, oppure essere rifiutato al producer boundary.

### Casi confermati

1. Python codifica tutti gli interi signed a 64 bit come `DataType.INT`; TypeScript li converte sempre in `number`. `2^60` diventa `1152921504606847000`, perdendo precisione.
2. `json.dumps` Python consente per default `NaN` e `Infinity`; `JSON.parse` TypeScript li rifiuta.
3. Un errore di decode avviene prima dei callback del batch: la subscription termina e tutti i messaggi già riservati restano in-flight fino al timeout.
4. TypeScript non ha un percorso esplicito per payload `bigint`: `JSON.stringify` genera errore.

### Correzione e regressioni

- Definire il contratto INT: `bigint` in TS oppure rifiuto oltre `Number.MAX_SAFE_INTEGER`.
- Usare `allow_nan=False` in Python e validazione equivalente.
- Aggiungere fixture cross-SDK per estremi i64, interi unsafe, NaN/Infinity e batch con un payload invalido.

### Riferimenti

- [`sdk/py/src/nexo/protocol/codec.py:238`](../../sdk/py/src/nexo/protocol/codec.py#L238) — encoding Python.
- [`sdk/ts/src/protocol/codec.ts:58`](../../sdk/ts/src/protocol/codec.ts#L58) — conversione INT e JSON parse.
- [`sdk/ts/src/brokers/queue.ts:124`](../../sdk/ts/src/brokers/queue.ts#L124) — decode dell'intero batch prima dei callback.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-15 · P2 — `peek` DLQ è `O(offset)` e blocca tutte le operazioni della queue</strong></summary>

### Comportamento atteso

La paginazione deve raggiungere l'offset in `O(log n)` e produrre `k` risultati in `O(k)`, in linea con la regola prestazionale del progetto.

### Causa e impatto

`LinkedHashMap.values().rev().skip(offset)` attraversa linearmente gli elementi precedenti. L'intera scansione e la clonazione dei payload avvengono mentre `QueueInner` è protetto dal mutex. Offset controllati dal client possono quindi monopolizzare push, consume, ACK/NACK e timeout della stessa queue.

### Correzione e regressioni

- Preferire pagination cursor-based su `dlq_seq`, con `BTreeMap::range` e lookup UUID separato: `O(log n + k)`.
- Se il contratto offset deve restare, serve un indice order-statistics o equivalente; un semplice `BTreeMap` con `skip` resterebbe lineare.
- Misurare pagina iniziale, profonda e oltre fine su una DLQ grande.

### Riferimenti

- [`src/brokers/queue/domain/dlq.rs:51`](../../src/brokers/queue/domain/dlq.rs#L51) — struttura corrente.
- [`src/brokers/queue/domain/dlq.rs:93`](../../src/brokers/queue/domain/dlq.rs#L93) — `skip(offset)`.
- [`src/brokers/queue/manager.rs:625`](../../src/brokers/queue/manager.rs#L625) — chiamata sotto lock.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-16 · P2 — Drop del `QueueManager` non termina task e writer</strong></summary>

### Comportamento atteso

Quando l'ultimo owner del manager scompare, almeno lo sweeper deve ricevere cancellazione e non deve mantenere vive queue, sender, SQLite connection e file handle.

### Causa e impatto

`spawn_timeout_task` cattura clone di `queues` e `CancellationToken`. Il task mantiene viva la `DashMap`, che mantiene vivi `QueueShared` e `QueueStore`. Non esiste `Drop`; soltanto `shutdown()` cancella esplicitamente il token.

Questo interessa embedding/test e rende ambigui diversi test di restart che lasciano semplicemente uscire il manager dallo scope: fino alla chiusura del runtime, il vecchio sweeper e writer possono ancora esistere sullo stesso DB.

### Correzione e regressioni

- Conservare il `JoinHandle` dello sweeper in un supervisor.
- Cancellare sincronicamente in `Drop`; mantenere `shutdown().await` per drain e join completi.
- Nei test di recovery chiamare sempre shutdown e verificare con `Weak`/handle che il vecchio stato sia rilasciato.

### Riferimenti

- [`src/brokers/queue/manager.rs:48`](../../src/brokers/queue/manager.rs#L48) — manager senza handle del task.
- [`src/brokers/queue/manager.rs:206`](../../src/brokers/queue/manager.rs#L206) — task self-sustaining.
- [`src/brokers/queue/manager.rs:596`](../../src/brokers/queue/manager.rs#L596) — unico percorso di cancellazione.
- [`tests/queue_tests.rs:610`](../../tests/queue_tests.rs#L610) — esempio di restart per uscita dallo scope.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-17 · P2 — Il controllo symlink protegge il DB ma non `config.json`</strong></summary>

### Comportamento atteso

La creazione di una queue non deve seguire link simbolici per nessun file sotto il persistence root.

### Causa e precondizione

`create_queue` usa `symlink_metadata` soltanto su `<name>.db`. Subito dopo esegue `std::fs::write` su `<name>.config.json`, che segue un symlink esistente.

Con accesso locale al persistence directory, un attore meno privilegiato può predisporre il link; una successiva `Q_CREATE` remota fa sovrascrivere al processo Nexo un file arbitrario scrivibile con i privilegi del server. La precondizione locale riduce la priorità, ma il boundary è incompleto.

### Correzione e regressioni

- Creare DB e config tramite primitive relative al directory handle con `NOFOLLOW`/create-new.
- Evitare il semplice check-then-open, soggetto a TOCTOU.
- Testare in una directory temporanea che né DB né config possano essere symlink.

### Riferimenti

- [`src/brokers/queue/manager.rs:301`](../../src/brokers/queue/manager.rs#L301) — controllo limitato al DB.
- [`src/brokers/queue/manager.rs:334`](../../src/brokers/queue/manager.rs#L334) — write della config che può seguire il link.

</details>

<details name="queue-bugs">
<summary><strong>QUEUE-18 · P2 — L'encoder UUID TypeScript non verifica 32 cifre esadecimali</strong></summary>

### Comportamento atteso

`uuid()` deve scrivere esattamente 16 byte oppure fallire prima di modificare il frame.

### Causa e impatto

Il writer riserva 16 byte, ma avanza l'offset in base alle coppie effettivamente incontrate. Non verifica numero di nibble, nibble finale spaiato o lunghezza totale. Un ID corto fa iniziare il campo successivo troppo presto; uno lungo sposta tutto il layout.

ACK/NACK sono fire-and-forget, quindi il chiamante non vede neppure il `ProtocolError`. Replay e delete DLQ falliscono con errori di protocollo poco riconducibili all'ID. Python esegue invece il controllo dei 32 caratteri.

Gli ID restituiti normalmente dal server sono validi; il bug riguarda input amministrativo o applicativo passato a replay/delete e accesso interno ai command.

### Correzione e regressioni

- Validare esattamente 32 cifre hex dopo la rimozione dei quattro trattini canonici.
- Rifiutare trattini in posizioni arbitrarie e nibble dispari.
- Testare forma canonica, dashless, corta, lunga, dispari e caratteri invalidi.

### Riferimenti

- [`sdk/ts/src/protocol/codec.ts:179`](../../sdk/ts/src/protocol/codec.ts#L179) — encoder senza controllo di lunghezza.
- [`sdk/py/src/nexo/protocol/codec.py:186`](../../sdk/py/src/nexo/protocol/codec.py#L186) — validazione Python.
- [`sdk/ts/src/brokers/queue.ts:177`](../../sdk/ts/src/brokers/queue.ts#L177) e [`sdk/ts/src/brokers/queue.ts:185`](../../sdk/ts/src/brokers/queue.ts#L185) — replay/delete DLQ.

</details>

## Aree verificate senza mismatch di base

- Opcode, ordine dei campi e big-endian sono simmetrici fra Rust, TypeScript e Python.
- Priority/FIFO in memoria usa correttamente priorità decrescente e `ready_seq` crescente.
- Token stale, NACK e timeout funzionano correttamente finché non avviene un restart.
- Il push count è limitato lato server e lato Python.
- La registrazione `Notify` prima del controllo evita il classico lost wakeup.
- DB SQLite corrotto impedisce la registrazione della queue; il problema fail-open riguarda specificamente la config.
- Replay DLQ azzera attempts e rende il messaggio nuovamente ready.

## Ordine suggerito di correzione

```text
1. QUEUE-01 + QUEUE-02  sicurezza memoria e limiti wire
2. QUEUE-03             correttezza anti-stale attraverso restart
3. QUEUE-04 + QUEUE-05  transazioni RAM/storage
4. QUEUE-06..11         recovery e lifecycle end-to-end
5. QUEUE-12..18         boundary SDK, interoperabilità e scalabilità
```
