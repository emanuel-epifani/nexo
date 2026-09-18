# Stream — Bug nelle logiche end-to-end

> **Stato:** analisi e documentazione; nessuna correzione applicata.
> **Ambito:** server Rust, framing e limiti TCP, consumer group, persistenza/retention/recovery, SDK TypeScript e Python e interoperabilità dei payload.
> **Priorità:** quindici problemi: un P0, dieci P1 e quattro P2.

Ogni bug è contenuto in una sezione inizialmente chiusa. Nei renderer che supportano l'attributo HTML `name` di `<details>`, aprire una sezione chiude automaticamente quella precedente. Negli altri renderer le sezioni possono essere aperte e chiuse individualmente.

## Come leggere le evidenze

- **Probe**: in questo documento un probe è un test diagnostico temporaneo che verifica un'invariante attesa seguendo lo schema *stato iniziale → operazione sospetta → invariante*. Non è una correzione né una modifica al codice di produzione.
- La suite Rust esistente è stata eseguita con `cargo test --offline --test stream_tests -- --test-threads=1`: **78 test superati**.
- Per **STREAM-01..06** sono stati creati ed eseguiti **7 probe temporanei**, che falliscono sulle invarianti attese e confermano i **6 bug distinti**. Il bug DLS ha due probe, uno per ciascuna operazione coinvolta.
- I probe usano le API del `StreamManager` con directory di persistenza temporanee; non sono mock della logica del gruppo.
- Il file temporaneo `tests/stream_review_repro.rs` è stato rimosso al termine della review. I nomi e i risultati riportati qui documentano quelle esecuzioni: **non identificano test attualmente presenti nel repository**.
- Per **STREAM-07..15** l'evidenza è **statica ad alta confidenza sul codice corrente**: nessun nuovo probe è stato eseguito per questi finding. Ogni sezione propone un probe minimo da eseguire e richiede regressioni permanenti al momento della correzione.
- La simmetria di base tra opcode e layout del wire (Rust/TypeScript/Python) è stata ispezionata senza rilevare mismatch di base.
- Gli SDK TypeScript e Python sono stati verificati leggendo il codice: **in questa estensione non è stata eseguita una nuova verifica TCP live degli SDK**.
- Le proposte di correzione e i test di regressione elencati nelle sezioni sono lavoro futuro, non modifiche già implementate.

### Indice dei finding

| ID | Priorità | Attore principale | Evidenza | Sintesi |
| --- | --- | --- | --- | --- |
| STREAM-01 | P1 | Server → SDK TS/Py | Dinamica storica + verifica statica corrente | Seek durante il polling: subscriber attivo, ma incapace di ripartire |
| STREAM-02 | P1 | Server (persistenza del gruppo) | Dinamica storica + verifica statica corrente | Redrive dalla DLS già consegnato: un riavvio perde la consegna non confermata |
| STREAM-03 | P1 | Server (retention + gruppo) | Dinamica storica + verifica statica corrente | Un messaggio ancora nel log resta bloccato e viene superato dal checkpoint |
| STREAM-04 | P1 | Server (DLS) | Dinamica storica + verifica statica corrente | Delete/move DLS rifiutati: lo stato pending viene comunque rimosso |
| STREAM-05 | P1 | Server (recovery) | Dinamica storica + verifica statica corrente | Redelivery e cursore fresh consegnano la stessa sequenza nello stesso batch |
| STREAM-06 | P2 | Server (long-poll) | Dinamica storica + verifica statica corrente | Un batch interamente bloccato nasconde messaggi pronti di altre chiavi |
| STREAM-07 | P0 | Server + SDK TS/Py | Statica corrente; probe da eseguire | FETCH senza budget di byte: risposta fuori limite e rischio OOM |
| STREAM-08 | P1 | Server (retention) | Statica corrente; probe da eseguire | L'ultimo segmento non scade mai |
| STREAM-09 | P1 | Server (recovery) | Statica corrente; probe da eseguire | config.json corrotto sostituito dai default (fail-open) |
| STREAM-10 | P1 | Server (persistenza) + wire | Statica corrente; probe da eseguire | Group ID oltre u16 corrompe lo snapshot persistente |
| STREAM-11 | P1 | SDK TS/Py | Statica corrente; probe da eseguire | Timeout o ACK fallito può lasciare membri orfani |
| STREAM-12 | P1 | SDK Python | Statica corrente; probe da eseguire | Future e Task non vengono attesi prima dell'ACK |
| STREAM-13 | P2 | SDK TypeScript | Statica corrente; probe da eseguire | `concurrency` non validata e diversa da Python |
| STREAM-14 | P2 | SDK TS/Py (interoperabilità) | Statica corrente; probe da eseguire | Payload non interoperabili per interi grandi e JSON non finito |
| STREAM-15 | P2 | Server (notifiche) | Statica corrente; probe da eseguire | ACK risveglia tutti i long-poll dello stream |

### Termini essenziali

| Termine | Significato in questa analisi |
| --- | --- |
| `seq` | Numero di sequenza del messaggio nel log. |
| Consumer group | Stato condiviso di avanzamento, consegna e ACK di un insieme di consumer. Gruppi diversi avanzano indipendentemente. |
| `ack_floor` | Checkpoint persistente del gruppo. Nell'implementazione attuale può avanzare anche oltre messaggi finiti in DLS o eliminati dalla retention: non significa che ogni messaggio precedente abbia completato con successo il callback. |
| `next_deliver_seq` | Cursore volatile usato per cercare i prossimi messaggi del percorso di consegna "fresh". |
| `Pending` | Messaggio consegnato a un consumer, ma non ancora confermato con ACK. |
| `Redeliver` | Messaggio in attesa di una nuova consegna. |
| DLS | Dead Letter Topic: stato dei messaggi che hanno esaurito i tentativi o sono stati parcheggiati per una chiave avvelenata. |
| Redrive | Recupero esplicito di un messaggio dalla DLS tramite `moveToStream` / `move_to_stream`. |
| `in_flight` / `blocked` | Per una chiave, il messaggio che ne mantiene il blocco di consegna e i successivi messaggi che devono aspettarlo. |
| `generation` / `FENCED` | Versione dello stato del gruppo e relativo errore quando un consumer usa una generazione non più valida. |

**Distinzione importante:** nei casi descritti come perdita di consegna, il messaggio può essere ancora presente nel log. Il problema è che il gruppo non lo considera più da consegnare, oppure non riesce più a confermarlo. Non si sta affermando che questi bug cancellino fisicamente il suo payload dal disco.

<details name="stream-bugs">
<summary><strong>STREAM-01 · P1 — Seek durante il polling: subscriber attivo, ma incapace di ripartire</strong></summary>

### Comportamento atteso

Un `seek` modifica la posizione del gruppo. I subscriber già attivi devono recuperare la nuova appartenenza al gruppo e riprendere a consumare dalla posizione richiesta, senza richiedere un riavvio manuale della subscription.

### Scenario che attiva il bug

Il subscriber è in attesa di messaggi, oppure sta per eseguire il prossimo fetch, e un client esegue `seek(group, 'beginning')` o `seek(group, 'end')`. Non c'è un callback in corso il cui ACK possa far emergere il fencing.

### Riproduzione minima

1. Creare uno stream e fare join a un gruppo, ottenendo `consumer_id` e `generation`.
2. Eseguire `seek` sullo stesso gruppo.
3. Eseguire un nuovo fetch con la vecchia identità, `limit = 1` e `wait_ms = 100`.
4. Verificare che il consumer riceva `FENCED`, così da poter fare rejoin.

Riproduzione temporanea: `seek_must_fence_an_idle_subscription`.

```text
Atteso:   Err("FENCED")
Ottenuto: Ok([])
```

La riproduzione verifica il fetch successivo al reset. Se un long-poll era già aperto, può prima terminare tramite il token di cancellazione; il problema resta sui fetch successivi, che continuano a usare l'identità scaduta.

### Causa nel server e propagazione negli SDK

1. Il `seek` richiama il reset del gruppo: vengono rimossi i membri e incrementata la generazione.
2. `ensure_active_consumer` rileva la generazione scaduta e restituisce `FENCED`.
3. Nel ramo long-poll, `StreamManager::fetch` intercetta `FENCED` e `NOT_MEMBER` e li converte in una lista vuota se il consumer non è più membro.
4. Gli SDK interpretano `count = 0` come un fetch riuscito senza messaggi.
5. Il rejoin avviene nel percorso di gestione degli errori, che non viene mai raggiunto. Il vecchio `consumer_id` rimane quindi in uso.

Il ramo con `wait_ms = 0` non applica questa conversione. Una verifica limitata ai fetch non bloccanti può quindi non rilevare il comportamento dei subscriber reali.

### Impatto

- Subscription apparentemente attiva, ma nessun nuovo callback.
- Nessun recupero automatico della nuova generazione.
- Fetch vuoti immediati e ripetuti, invece della normale attesa del long-poll: carico inutile su client e server.
- Problema comune a TypeScript e Python, perché entrambi fanno rejoin a seguito di un errore, non di una risposta vuota.

### Perché i test vicini non bastano

I casi che fermano il subscriber, eseguono `seek` e creano una nuova subscription non riutilizzano l'identità scaduta. Inoltre, un `seek` durante un callback può essere recuperato correttamente: il successivo ACK riceve `FENCED` e attiva il rejoin. È diverso dal caso idle descritto qui.

### Direzione della correzione e regressioni necessarie

- Non nascondere il fencing nei fetch successivi al cambio di generazione.
- Distinguere l'interruzione di un fetch per `leave` dalla necessità di ristabilire una subscription ancora attiva dopo `seek`.
- Verificare in Rust il risultato del fetch con identità scaduta e `wait_ms > 0`.
- Verificare in entrambi gli SDK `seek` durante il polling, sia verso inizio sia verso fine, senza fermare e ricreare la subscription.
- Conservare la proprietà per cui `stop()` interrompe rapidamente un long-poll e non avvia nuovi callback.

### Riferimenti

- [`src/brokers/stream/manager.rs:604`](../../src/brokers/stream/manager.rs#L604) — `fetch`; conversione degli errori di appartenenza in risposta vuota alle linee 663-668.
- [`src/brokers/stream/domain/group.rs:457`](../../src/brokers/stream/domain/group.rs#L457) — `reset_runtime` (457-467): reset dei membri e della generazione.
- [`sdk/ts/src/brokers/stream.ts:276`](../../sdk/ts/src/brokers/stream.ts#L276) — loop e rejoin TypeScript (276-300); `consumerId = null` a 287.
- [`sdk/py/src/nexo/brokers/stream.py:443`](../../sdk/py/src/nexo/brokers/stream.py#L443) — `_loop` Python (443-479); `_consumer_id = None` a 459.
- [`docs/guide/stream.md:511`](../../docs/guide/stream.md#L511) — comportamento documentato di seek con subscriber attivi.

</details>

<details name="stream-bugs">
<summary><strong>STREAM-02 · P1 — Redrive dalla DLS già consegnato: un riavvio perde la consegna non confermata</strong></summary>

### Comportamento atteso

Un messaggio recuperato dalla DLS deve rimanere recuperabile dopo un riavvio finché non riceve un ACK. Riceverlo una seconda volta è compatibile con at-least-once; non riceverlo più, pur senza ACK, non lo è.

### Scenario che attiva il bug

Il messaggio recuperato ha una sequenza già raggiunta o superata dall'`ack_floor`. Il server si riavvia **dopo il fetch del redrive e prima del suo ACK**.

La condizione temporale è essenziale: il riavvio immediatamente dopo `moveToStream`, prima della nuova consegna, è un caso diverso e già coperto da un test esistente.

### Riproduzione minima

Configurazione: `max_deliveries = 1`, `ack_wait_ms = 60_000`, persistenza in una directory temporanea.

1. Pubblicare il messaggio `1`, senza chiave.
2. Fare join e fetch di `1`, senza ACK.
3. Disconnettere quel consumer: avendo esaurito i tentativi, `1` passa in DLS e il checkpoint può arrivare a `1`.
4. Eseguire `move_to_stream(..., 1)`.
5. Fare join con un altro consumer e ricevere nuovamente `1`.
6. Senza inviare l'ACK né disconnettere prima quel secondo consumer, chiamare `StreamManager::shutdown()`.
7. Ricreare il manager sulla stessa directory, fare join ed eseguire fetch.

Riproduzione temporanea: `pending_dls_redrive_must_survive_restart`.

```text
Prima del riavvio: 1 riconsegnato, nessun ACK
Atteso al ritorno: [1]
Ottenuto:          []
```

La riproduzione usa uno shutdown con salvataggio finale, non la perdita casuale di un flush periodico. Il dato mancante è assente dallo snapshot per costruzione.

### Causa interna

Il checkpoint non viene riportato indietro quando si recupera `1` dalla DLS. La sequenza attraversa questi stati:

```text
DLS → Redeliver → Pending → riavvio
```

`redeliver_snapshot()` include soltanto `MsgState::Redeliver`. Quando il messaggio viene consegnato, diventa `Pending` e scompare da quello snapshot. Anche il salvataggio finale del manager usa questa funzione.

Al recupero, il cursore fresh viene ricostruito da `ack_floor + 1`. Se il floor vale `1`, il gruppo riparte da `2`. Il messaggio `1` non è più in DLS, non compare nelle redelivery salvate e non viene raggiunto dal cursore fresh.

### Impatto e limiti del caso

- Violazione dell'at-least-once sul tentativo recuperato dalla DLS.
- La perdita riguarda lo stato di consegna del gruppo, non necessariamente il record nel log.
- Non significa che tutti i messaggi pending spariscano dopo ogni riavvio: quelli oltre il checkpoint possono essere ritrovati dal percorso fresh. Il caso critico è il redrive a una sequenza già superata.
- Entrambi gli SDK sono esposti, anche quando eseguono normalmente il callback: il riavvio può precederne la conferma.

### Direzione della correzione e regressioni necessarie

- Persistire anche le consegne non confermate che non sarebbero raggiungibili dal checkpoint, conservando le informazioni necessarie per la riconsegna.
- Ricostruire un retry valido senza ripristinare come attivo il vecchio consumer disconnesso.
- Verificare le tre finestre: riavvio prima del fetch del redrive, dopo il fetch senza ACK, dopo l'ACK confermato e salvato.
- Coprire messaggi con e senza chiave, con checkpoint coincidente con la sequenza o già più avanti.
- Coordinare questa correzione con STREAM-05: salvare ulteriori retry non deve creare sovrapposizioni con il percorso fresh.

### Riferimenti

- [`src/brokers/stream/domain/group.rs:537`](../../src/brokers/stream/domain/group.rs#L537) — `redeliver_snapshot` (537+): snapshot che esclude i pending.
- [`src/brokers/stream/domain/group.rs:147`](../../src/brokers/stream/domain/group.rs#L147) — `restore` (147+): ricostruzione del cursore dal floor.
- [`src/brokers/stream/manager.rs:136`](../../src/brokers/stream/manager.rs#L136) — `shutdown` (136+): salvataggio finale allo shutdown.
- [`tests/stream_tests.rs:2444`](../../tests/stream_tests.rs#L2444) — `dls_move_to_stream_survives_restart`: test esistente del riavvio prima della nuova consegna.
- [`docs/guide/stream.md:666`](../../docs/guide/stream.md#L666) — persistenza documentata dei redrive.

</details>

<details name="stream-bugs">
<summary><strong>STREAM-03 · P1 — Retention: un messaggio ancora nel log rimane bloccato e viene superato dal checkpoint</strong></summary>

### Comportamento atteso

La retention può eliminare i messaggi fuori dalla finestra di conservazione. Se elimina il predecessore che blocca una chiave, il primo successore ancora conservato deve tornare consegnabile, mantenendo l'ordine tra i messaggi sopravvissuti.

### Scenario che attiva il bug

Il gruppo ha già esaminato almeno due messaggi della stessa chiave: il primo è in flight e il secondo è trattenuto in `blocked`. La retention elimina soltanto il primo, lasciando il secondo nel log.

### Riproduzione minima

Configurazione usata: segmenti da `30` byte, retention `max_bytes = 60`, retention per età disabilitata, controllo ogni `1_000` ms e ACK timeout da `60_000` ms.

1. Pubblicare separatamente `1:A`, `2:A`, `3:B`, usando chiavi da un byte e payload da tre byte: ciascun record occupa un segmento da `30` byte.
2. Prima del controllo di retention, fare join e fetch con `limit = 3`.
3. Il consumer riceve `[1, 3]`; `2` è correttamente bloccato da `1`.
4. Attendere il controllo di retention: vengono conservati esattamente i segmenti di `2` e `3`.
5. Verificare tramite `read` che il log contenga ancora `[2, 3]`.
6. Confermare `3` e chiedere altri messaggi allo stesso gruppo.

Riproduzione temporanea: `retention_must_release_surviving_blocked_key`.

```text
Prima della retention: consegnati [1, 3], bloccato 2
Log dopo retention:   [2, 3]
Fetch atteso:         [2]
Fetch ottenuto:       []
```

### Causa interna

`clamp_head()` elimina gli stati precedenti al nuovo `head_seq` e aggiorna le chiavi:

- Il vecchio `in_flight = 1` viene azzerato.
- La sequenza `2`, ancora valida, resta in `KeyState.blocked`.
- Nessuno la trasferisce in `Redeliver` e nel relativo indice.

Il cursore fresh aveva già superato `2` durante il fetch iniziale. Quindi non la visiterà nuovamente da solo.

Inoltre, i messaggi soltanto bloccati non sono rappresentati come pending o redelivery in `msgs`. `try_advance_floor()` controlla proprio quella mappa: può interpretare `2` come una sequenza superabile e avanzare il checkpoint oltre un messaggio mai consegnato.

### Impatto

- Una sequenza conservata viene esclusa dal normale avanzamento del gruppo.
- Il checkpoint può consolidare il salto anche nel recupero successivo.
- Il blocco non è giustificato dalla retention: `1` è stato eliminato legittimamente, `2` no.
- Dal codice deriva anche un rischio di riordino: un nuovo messaggio della stessa chiave può ottenere il lock libero e il suo ACK può successivamente liberare il vecchio messaggio bloccato. Questo percorso ulteriore non è quello misurato dalla riproduzione sopra.

### Direzione della correzione e regressioni necessarie

- Quando scompare il predecessore in flight, rendere consegnabile il primo successore conservato, se la chiave non è ancora avvelenata.
- Aggiornare stato della chiave, indice delle redelivery e checkpoint come un'unica transizione coerente.
- Impedire che l'assenza in `msgs` venga interpretata come completamento quando esiste ancora una consegna bloccata da preservare.
- Testare retention con predecessore pending e in redelivery, più successori conservati e presenza di DLS per la chiave.
- Aggiungere una verifica dopo riavvio e una verifica con pubblicazione successiva sulla stessa chiave.

### Riferimenti

- [`src/brokers/stream/domain/group.rs:356`](../../src/brokers/stream/domain/group.rs#L356) — `clamp_head` (356-422): allineamento del gruppo al nuovo head.
- [`src/brokers/stream/domain/group.rs:397`](../../src/brokers/stream/domain/group.rs#L397) — pulizia della chiave senza riattivare il successore (397-415).
- [`src/brokers/stream/domain/group.rs:557`](../../src/brokers/stream/domain/group.rs#L557) — `try_advance_floor` (557+): avanzamento del floor basato su `msgs`.
- [`src/brokers/stream/domain/group.rs:283`](../../src/brokers/stream/domain/group.rs#L283) — `ack_pending` (283-315): percorso ACK che normalmente libera un successore.

</details>

<details name="stream-bugs">
<summary><strong>STREAM-04 · P1 — Delete/move DLS rifiutati: lo stato pending viene comunque rimosso</strong></summary>

### Comportamento atteso

Una richiesta di cancellazione o recupero DLS applicata a una sequenza non presente in DLS deve fallire senza modificare lo stato del gruppo. Il consumer proprietario deve poter continuare l'elaborazione e inviare l'ACK normalmente.

### Scenario che attiva il bug

Un messaggio è `Pending`, ma un client richiede `deleteDls` oppure `moveToStream` per quella sequenza. Può trattarsi di un parametro sbagliato o di un'operazione amministrativa basata su uno stato non più aggiornato.

### Riproduzione minima

1. Pubblicare `1`, fare join e riceverlo con fetch, senza ACK.
2. Chiamare `delete_dls(..., 1)`.
3. Verificare l'errore `seq not in DLS`.
4. Inviare l'ACK di `1` con consumer e generazione corretti.
5. Ripetere su un nuovo gruppo/stream usando `move_to_stream(..., 1)` al punto 2.

Riproduzioni temporanee:

- `invalid_dls_delete_must_preserve_pending_ack`.
- `invalid_dls_move_must_preserve_pending_ack`.

```text
Richiesta DLS: errore "seq not in DLS", come atteso
ACK atteso:   Ok(())
ACK ottenuto: Err("seq 1 not pending")
```

### Causa interna

Entrambe le operazioni eseguono prima `self.msgs.remove(&seq)` e controllano solo dopo la variante rimossa:

```text
remove(seq) → controlla se era Dls → altrimenti ritorna errore
```

Se il valore era `Pending`, viene consumato dalla rimozione e non viene reinserito nel ramo di errore. Lo stato è già stato modificato quando il chiamante riceve il rifiuto.

La rimozione non passa nemmeno dal percorso che mantiene sincronizzati pending, contatore, deadline e chiave. Rimangono quindi strutture che descrivono una consegna non più presente nella mappa principale.

### Impatto

- L'ACK legittimo fallisce dopo un'operazione DLS rifiutata.
- `pending_count` può mantenere uno slot occupato senza una corrispondente voce pending, riducendo la capacità effettiva del gruppo.
- Il timeout non trova più il pending da riconsegnare; per un messaggio con chiave può restare anche un blocco non risolvibile tramite il normale ACK.
- Le API degli SDK espongono l'errore DLS, ma non possono ripristinare lo stato rimosso nel server.

Il difetto di rimozione si applica anche a una voce `Redeliver`, perché il ramo di errore consuma qualunque variante diversa da `Dls`. Le due riproduzioni eseguite misurano specificamente il caso `Pending`.

### Direzione della correzione e regressioni necessarie

- Validare la variante prima di rimuoverla, all'interno della stessa sezione critica.
- Garantire che ogni operazione DLS fallita lasci invariati mappa, indici, deadline, chiavi, checkpoint e contatori.
- Per entrambe le API, coprire sequenza pending, in redelivery, inesistente, già confermata e realmente in DLS.
- Verificare non solo l'errore restituito, ma anche il successivo ACK, l'eventuale retry e la capacità di fetch del gruppo.
- Ripetere i casi d'errore tramite le API TypeScript e Python, per coprire il flusso richiesta rifiutata → elaborazione legittima → ACK.

### Riferimenti

- [`src/brokers/stream/domain/group.rs:751`](../../src/brokers/stream/domain/group.rs#L751) — `move_to_stream` (751-761).
- [`src/brokers/stream/domain/group.rs:765`](../../src/brokers/stream/domain/group.rs#L765) — `delete_dls` (765-772).
- [`src/brokers/stream/domain/group.rs:283`](../../src/brokers/stream/domain/group.rs#L283) — `ack_pending` (283+): verifica del pending durante l'ACK.
- [`src/brokers/stream/domain/group.rs:319`](../../src/brokers/stream/domain/group.rs#L319) — `check_redelivery` e deadlines (319+): elaborazione dei timeout di consegna.

</details>

<details name="stream-bugs">
<summary><strong>STREAM-05 · P1 — Recovery: redelivery e cursore fresh consegnano la stessa sequenza nello stesso batch</strong></summary>

### Comportamento atteso

Una sequenza in attesa di retry deve essere assegnata una sola volta per tentativo. Dopo il riavvio, redelivery e lettura fresh non devono produrre due assegnazioni contemporanee della stessa sequenza, né violare il vincolo di una sola consegna in flight per chiave.

### Riproduzione minima

Configurazione: `ack_wait_ms = 60_000`, `max_deliveries = 5` e capacità pending sufficiente per il batch.

1. Pubblicare `1:A` e `2:A`.
2. Fare join e ricevere soltanto `1` con `limit = 1`.
3. Disconnettere il consumer senza ACK: `1` passa in redelivery e il floor rimane `0`.
4. Eseguire lo shutdown del manager, salvando lo stato.
5. Ricreare il manager sulla stessa directory e fare join.
6. Richiedere un batch con `limit = 10` e `wait_ms = 0`.

Riproduzione temporanea: `restart_must_not_deliver_same_sequence_twice_in_one_batch`.

```text
Atteso:   [1]    (2 deve aspettare l'ACK di 1)
Ottenuto: [1, 1]
```

Nella prima variante, con due messaggi senza chiave, il risultato osservato era `[1, 1, 2]` invece di `[1, 2]`. La variante con chiave rende esplicita anche la violazione della serializzazione per chiave.

### Causa interna

Il recovery ripristina la redelivery di `1`, ma ricostruisce anche `next_deliver_seq = ack_floor + 1`, quindi nuovamente `1`.

Durante il fetch:

1. Il percorso redelivery assegna `1` e lo trasforma in `Pending`.
2. Il percorso fresh parte dallo stesso numero e trova nuovamente `1` nel batch letto dal log.
3. `issue_delivery()` non esclude una sequenza già pending.
4. Per la chiave, il controllo consente `in_flight == msg.seq`, pensato per riconsegnare la stessa sequenza. Non distingue però una vera redelivery da una duplicazione fresh di un pending appena creato.
5. La stessa chiave della mappa `msgs` viene sovrascritta, mentre `pending_count` viene incrementato una seconda volta.

### Impatto

- Due callback dello stesso messaggio possono partire contemporaneamente con `concurrency > 1`, anche con la stessa chiave.
- Con elaborazione sequenziale, dopo l'ACK della prima copia il secondo ACK può fallire perché la sequenza non è più pending.
- Contatore pending e numero di consegne effettivamente tracciate divergono, lasciando capacità apparentemente occupata.
- Anche il conteggio dei tentativi viene aumentato senza una vera scadenza o disconnessione tra le due assegnazioni.

L'at-least-once richiede comunque callback idempotenti, ma non giustifica questo difetto: qui vengono violati il tracking interno e la promessa di serializzazione per chiave, non soltanto l'assenza di duplicati applicativi.

### Direzione della correzione e regressioni necessarie

- Riconciliare le redelivery ripristinate con l'intervallo ancora attraversato dal cursore fresh.
- Impedire che `issue_delivery` trasformi nuovamente un pending già assegnato in una seconda consegna dello stesso tentativo.
- Non limitarsi a deduplicare la risposta finale: a quel punto contatori, tentativi e stato del gruppo potrebbero essere già stati modificati due volte.
- Coprire restart con e senza chiave, batch maggiore di uno e più consumer.
- Verificare unicità delle sequenze nel batch, serializzazione per chiave, ACK successivi, contatori e limite dei tentativi.
- Testare insieme a STREAM-02, che richiede di non perdere retry ancora aperti durante la persistenza.

### Riferimenti

- [`src/brokers/stream/domain/group.rs:147`](../../src/brokers/stream/domain/group.rs#L147) — `restore` (147+): ripristino delle redelivery e del cursore fresh.
- [`src/brokers/stream/domain/group.rs:202`](../../src/brokers/stream/domain/group.rs#L202) — `fetch` (202-267): consegna dalla lista redelivery e seconda visita dal percorso fresh.
- [`src/brokers/stream/domain/group.rs:429`](../../src/brokers/stream/domain/group.rs#L429) — `fetch_plan` (429+).
- [`src/brokers/stream/domain/group.rs:581`](../../src/brokers/stream/domain/group.rs#L581) — `issue_delivery` (581-640): assegnazione, controllo della chiave e incremento del contatore pending.

</details>

<details name="stream-bugs">
<summary><strong>STREAM-06 · P2 — Long-poll: un batch interamente bloccato nasconde messaggi pronti di altre chiavi</strong></summary>

### Comportamento atteso

Il blocco di una chiave deve ritardare soltanto i messaggi successivi di quella chiave. Un consumer con capacità disponibile deve poter ricevere messaggi già pronti di altre chiavi, senza attendere un nuovo evento esterno.

### Riproduzione minima

Configurazione: `ack_wait_ms = 60_000`, in modo che il retry del primo messaggio non interferisca con l'attesa breve del test.

1. Pubblicare `1:A`, `2:A`, `3:B`.
2. Fare join con due consumer dello stesso gruppo.
3. Il primo consumer esegue un fetch con `limit = 1` e riceve `1:A`, senza ACK.
4. Il secondo esegue un fetch con `limit = 1` e `wait_ms = 150`.
5. Non pubblicare altro e non inviare ACK durante l'attesa.

Riproduzione temporanea: `blocked_key_must_not_hide_another_ready_key_during_long_poll`.

```text
Pending:           1:A
Bloccato:          2:A
Già disponibile:   3:B
Atteso dal fetch:  [3]
Ottenuto:          [] dopo l'attesa del long-poll
```

### Causa interna

`fetch_plan()` seleziona un numero di sequenze limitato dal budget di consegna. Nel caso minimo sceglie soltanto `2`.

Il manager legge quel record; il gruppo lo riconosce come bloccato dalla chiave e avanza il cursore fresh, ma restituisce zero consegne. `3` non era nel piano letto, quindi non può essere consegnato da quella chiamata a `ConsumerGroup::fetch`.

Il loop long-poll del manager interpreta la lista vuota come assenza di lavoro da consegnare. Non distingue tra:

- Log effettivamente esaurito.
- Capacità pending esaurita.
- Batch esaminato ma filtrato, con ulteriori messaggi potenzialmente pronti nel log.

Il solo avanzamento del cursore non incrementa il contatore di risveglio del topic. In assenza di altre notifiche, il fetch resta quindi in attesa fino al timeout, pur essendo disponibile `3:B`.

### Impatto e distinzione da STREAM-03

- Latenza artificiale e perdita dell'isolamento tra chiavi.
- Il ritardo può arrivare all'intero `waitMs` configurato; il default SDK è `20_000` ms. Un ACK, una pubblicazione o un'altra notifica possono abbreviare l'attesa.
- Il caso non è limitato ai batch da uno: si verifica anche quando un batch più grande contiene soltanto candidati non consegnabili e una chiave pronta si trova oltre quel batch.
- In questa riproduzione `3` non è perso né superato dal checkpoint: può essere trovato da un fetch successivo. STREAM-03 riguarda invece il tracking di un messaggio già bloccato, che può essere saltato dal checkpoint dopo retention.

### Direzione della correzione e regressioni necessarie

- Distinguere il progresso nella selezione dei candidati dall'effettiva assenza di messaggi consegnabili.
- Riesaminare il lavoro pronto senza aspettare notifiche esterne quando il batch è stato soltanto filtrato.
- Progettare il percorso con indici di disponibilità e operazioni O(1)/O(log n) per transizione: una scansione lineare indiscriminata dell'intero backlog a ogni fetch non è una soluzione accettabile per il progetto.
- Verificare il caso minimo, batch maggiori di uno, molti messaggi della chiave bloccata e messaggi senza chiave dietro il backlog.
- Misurare anche il caso idle e il caso di reale backpressure, per evitare che la correzione introduca busy polling.

### Riferimenti

- [`src/brokers/stream/domain/group.rs:429`](../../src/brokers/stream/domain/group.rs#L429) — `fetch_plan` (429+): piano limitato al budget.
- [`src/brokers/stream/domain/group.rs:202`](../../src/brokers/stream/domain/group.rs#L202) — `fetch` (202-267) e `issue_delivery` (581+): filtraggio e avanzamento del cursore.
- [`src/brokers/stream/manager.rs:604`](../../src/brokers/stream/manager.rs#L604) — long-poll `fetch` (604-687): interpretazione del risultato vuoto.
- [`src/brokers/stream/manager.rs:1209`](../../src/brokers/stream/manager.rs#L1209) — `try_fetch_once` (1209+): lettura ed esecuzione di un singolo piano.
- [`sdk/ts/src/config.ts:38`](../../sdk/ts/src/config.ts#L38) — impostazioni predefinite dello stream nel client TypeScript (38-40).

</details>

<details name="stream-bugs">
<summary><strong>STREAM-07 · P0 — FETCH senza budget di byte: risposta fuori limite e rischio OOM</strong></summary>

### Stato dell'evidenza

Evidenza statica ad alta confidenza sul codice corrente; il probe qui sotto è proposto e **non è stato eseguito**.

### Comportamento atteso

`MAX_PAYLOAD_SIZE` limita ogni frame in entrambe le direzioni. Un FETCH può restituire meno record del batch richiesto pur di rispettare il budget di byte; encoder e client costituiscono le barriere finali di difesa.

### Scenario che attiva il bug

Con il default di 10 MiB per payload, molti messaggi pubblicati singolarmente restano ciascuno entro il limite e quindi validi. Un solo FETCH aggrega però 100 o più payload ciascuno quasi al limite: con 100 × ~10 MiB la risposta supera ~1 GiB già prima delle copie ulteriori. Lo scenario è raggiungibile anche dagli SDK, il cui batch predefinito è 100.

### Probe minimo proposto

Probe proposto, non eseguito:

1. Avviare il test con `max_payload_size` di 1024 byte.
2. Pubblicare separatamente quattro payload da ~300 byte.
3. Eseguire un fetch con `batch = 4`.
4. Invariante: la risposta è parziale e `<= 1024` byte, oppure un errore tipizzato; mai un frame oltre il limite.

Difesa unitaria: `NexoCodec::new(16)` che codifica una `Response::Data` da 17 byte deve fallire.

### Causa interna e flusso end-to-end

- Il decoder Rust applica `max_payload_size` in ingresso, ma l'encoder Rust non lo usa affatto.
- `encode_fetch` materializza un'unica `Bytes` concatenando tutti i record, senza budget di byte.
- `fetch_plan` pianifica per conteggio e capacità pending, non per byte.
- Il parser del server non applica esplicitamente `STREAM_MAX_FETCH_BATCH_SIZE` al `limit` richiesto da client custom.
- Lato client, TypeScript accumula `Buffer` in base al `u32` dichiarato nell'header senza cap; Python usa `readexactly(payload_len)` senza cap.
- L'encoder del frame scrive `payload.len() as u32`: oltre 4 GiB la lunghezza può troncare e desincronizzare il framing, dopo aver già eseguito l'allocazione enorme.

### Impatto

- OOM lato server e/o client.
- Amplificazione di memoria per lettura, encoding e buffer del socket; rischio di disponibilità.
- Violazione del limite documentato di 10 MB.
- Framing invalido oltre 4 GiB.

### Direzione della correzione e regressioni necessarie

- Applicare un budget di byte nel manager/storage prima di leggere e materializzare i record: la risposta parziale è già compatibile con il wire attuale e non richiede modifiche di protocollo.
- Mantenere un hard cap nell'encoder come safety net: rifiutare solo nell'encoder è però insufficiente, perché a quel punto l'allocazione enorme è già avvenuta.
- Cap inbound configurabile negli SDK e validazione server anche del count massimo del batch.
- Regressioni: cap piccolo deterministico nel manager Rust e nel codec; header oversized verso client TS/Py; batch count da client custom; risposta exactly-at-limit e one-byte-over; molti payload piccoli e pochi grandi.

### Riferimenti

- [`src/protocol/codec.rs:48`](../../src/protocol/codec.rs#L48) — controllo `payload_len > max_payload_size` nel decoder (linee 47-53).
- [`src/protocol/codec.rs:72`](../../src/protocol/codec.rs#L72) — encoder senza controllo del limite; `payload.len() as u32` alle linee 103 e 111.
- [`src/brokers/stream/tcp.rs:169`](../../src/brokers/stream/tcp.rs#L169) — parse di `OP_S_FETCH` senza bound sul `limit`.
- [`src/brokers/stream/tcp.rs:325`](../../src/brokers/stream/tcp.rs#L325) — `encode_fetch` materializza l'intera risposta in una `Bytes`; dispatch a [`src/brokers/stream/tcp.rs:472`](../../src/brokers/stream/tcp.rs#L472).
- [`src/brokers/stream/domain/group.rs:429`](../../src/brokers/stream/domain/group.rs#L429) — `fetch_plan` con budget per conteggio/pending, non per byte.
- [`sdk/ts/src/transport/tcp/connection.ts:127`](../../sdk/ts/src/transport/tcp/connection.ts#L127) — `processBuffer` senza cap su `payloadLen` (127-149).
- [`sdk/py/src/nexo/transport/tcp/connection.py:131`](../../sdk/py/src/nexo/transport/tcp/connection.py#L131) — `_read_loop` con `readexactly(payload_len)` senza cap (131-157).
- [`src/protocol/generated.rs:97`](../../src/protocol/generated.rs#L97) — `STREAM_MAX_FETCH_BATCH_SIZE` definito ma non applicato nel parse.
- [`docs/guide/deployment.md:77`](../../docs/guide/deployment.md#L77) — default documentato `MAX_PAYLOAD_SIZE` (10 MB).

</details>

<details name="stream-bugs">
<summary><strong>STREAM-08 · P1 — Retention: l'ultimo segmento non scade mai</strong></summary>

### Stato dell'evidenza

Evidenza statica ad alta confidenza sul codice corrente; il probe qui sotto è proposto e **non è stato eseguito**.

### Comportamento atteso

Superato `maxAge`, anche uno stream inattivo con un solo segmento deve smettere di esporre i record. `maxBytes` deve essere un bound dichiarato, salvo granularità di segmento esplicitamente documentata.

### Scenario che attiva il bug

Un solo segmento attivo con record vecchi, oppure più segmenti in cui l'ultimo non riceve più append. Il task di retention gira periodicamente, ma lascia sempre almeno l'ultimo segmento.

### Probe minimo proposto

Probe proposto, non eseguito: `max_segment` grande, `maxAge` molto breve, publish di un solo record, attesa di più intervalli di retention. Invariante: `read` deve restituire vuoto e `head` deve avanzare. Ripetere il caso con l'ultimo segmento di una serie di più segmenti.

### Causa interna e flusso end-to-end

`apply_retention` ritorna subito quando `segments.len() <= 1`. Per `maxAge`, il controllo esclude sempre il segmento con `start_seq == last_start_seq`. Per `maxBytes`, il ciclo di eliminazione itera solo fino a `len - 1`. Non esiste un head a livello di record, né una compaction, né una rotazione temporale che renda eliminabile un segmento inattivo.

### Impatto

- Retention temporale potenzialmente infinita sull'ultimo segmento; dati storici ancora leggibili oltre il limite.
- Crescita di spazio e possibili problemi di compliance.
- `maxBytes` può eccedere almeno della dimensione dell'ultimo segmento.

### Direzione della correzione e regressioni necessarie

- Soluzione semplice a granularità di segmento: ruotare il segmento attivo quando è scaduto e aprirne uno nuovo vuoto, rendendo eliminabile il precedente.
- Alternativa più precisa ma più complessa: head logico intra-segmento con compaction.
- Qualunque soluzione deve restare O(log n) o ammortizzata, preservare `next_seq` e il recovery su uno stream completamente scaduto.
- Regressioni: segmento singolo, ultimo di molti, restart, `maxAge`+`maxBytes` combinati, append dopo scadenza totale.

### Riferimenti

- [`src/brokers/stream/domain/persistence.rs:282`](../../src/brokers/stream/domain/persistence.rs#L282) — `apply_retention` (282-353): early return a ~296-298, esclusione di `last_start_seq` a ~303-306, ciclo `maxBytes` fino a `len - 1` a ~331-345.
- [`docs/guide/stream.md:733`](../../docs/guide/stream.md#L733) — intervallo del task di retention e default documentati.

</details>

<details name="stream-bugs">
<summary><strong>STREAM-09 · P1 — Recovery fail-open: config.json corrotto viene sostituito dai default</strong></summary>

### Stato dell'evidenza

Evidenza statica ad alta confidenza sul codice corrente; il probe qui sotto è proposto e **non è stato eseguito**.

### Comportamento atteso

Un `config.json` mancante in un bootstrap legacy può avere un fallback deliberato e documentato. Un file presente ma illeggibile, non JSON o non conforme deve fallire esplicitamente o essere messo in quarantena: mai sostituire la configurazione in silenzio.

### Scenario che attiva il bug

Uno stream creato con retention o `maxAckPending` custom viene spento; `config.json` viene troncato, corrotto o reso illeggibile (permission error); al restart il bootstrap applica i default.

### Probe minimo proposto

Probe proposto, non eseguito: creare uno stream con config custom, corrompere `config.json`, eseguire il bootstrap. Invariante attesa: errore di startup oppure stream non montato con errore esplicito. Il comportamento corrente, dedotto staticamente, è una ricostruzione della config da `StreamConfig::from_options` con i default e un `describe` diverso da quello originale.

### Causa interna e flusso end-to-end

`load_stream_config` ritorna direttamente `StreamConfig` (non un `Result`) e usa il fallback sia su read failure sia su serde failure. Il bootstrap prosegue e può persino riscrivere `config.json` quando il path risulta assente, mentre la corruzione di un file presente resta invisibile all'operatore. Gli errori di I/O non vengono propagati.

### Impatto

- Retention più aggressiva o più permissiva di quella configurata.
- `maxAckPending`, ACK timeout e delivery count inattesi.
- Comportamento diverso dopo un restart senza alcun segnale per l'operatore.

### Direzione della correzione e regressioni necessarie

- Cambiare la firma in `Result<StreamConfig, RecoveryError>` e distinguere `NotFound` (fallback legacy deliberato) da errori di lettura e corruzione.
- Validare la config deserializzata; scrittura atomica temp + fsync + rename per gli aggiornamenti; log/metrica dedicata.
- Non "curare" la corruzione con i default.
- Regressioni: file mancante legacy, JSON malformato, campi obbligatori mancanti, tipi/range invalidi secondo il contratto, permission/read error dove supportato, config custom valida che resta identica.

### Riferimenti

- [`src/brokers/stream/manager.rs:925`](../../src/brokers/stream/manager.rs#L925) — `load_stream_config` con fallback silenzioso (925-937).
- [`src/brokers/stream/manager.rs:939`](../../src/brokers/stream/manager.rs#L939) — `bootstrap_from_disk`; riscrittura condizionale della config a ~997-1000.

</details>

<details name="stream-bugs">
<summary><strong>STREAM-10 · P1 — Group ID oltre u16 corrompe lo snapshot persistente</strong></summary>

### Stato dell'evidenza

Evidenza statica ad alta confidenza sul codice corrente; il probe qui sotto è proposto e **non è stato eseguito**.

### Comportamento atteso

Ogni identificatore accettato dal wire deve essere serializzabile e recuperabile, oppure deve essere rifiutato prima di mutare lo stato.

### Scenario che attiva il bug

Un group ID UTF-8 oltre 65535 **byte** — il limite è in byte, non in caratteri — è consentito dal prefisso stringa `u32` del wire e non è validato né dal server né dagli SDK. Join/seek crea lo stato del gruppo, un ACK lo rende dirty, e snapshot/shutdown/restart lo serializzano.

### Probe minimo proposto

Probe proposto, non eseguito: group ID di 65536 byte, publish/fetch/ack, shutdown e restart. Invariante attesa: rifiuto upfront oppure checkpoint intatto. Il comportamento corrente, dedotto staticamente: `as u16` tronca la lunghezza mentre vengono scritti tutti i byte; il CRC mismatch in lettura fa interrompere il loop di `load_state_file` e lo stato del gruppo viene ignorato insieme agli entry successivi.

### Causa interna e flusso end-to-end

In `write_state_entry`, `group_len` è `group_bytes.len() as u16` (troncato); `content_len` è calcolato con la lunghezza troncata, ma `put_slice(group_bytes)` scrive tutti i byte reali: il record su disco contiene più byte di quanti ne dichiara `content_len`. Il CRC è però calcolato sull'intero `content_buf`, mentre in lettura `read_record` verifica il CRC solo sui `content_len` byte dichiarati: il risultato è `ReadOutcome::Corrupted`. Sul wire `read_string` usa un prefisso `u32`: non esiste un limite condiviso generato.

### Impatto

- Il primo record oversized produce un CRC mismatch e `load_state_file` interrompe il loop di lettura: vengono ignorati **lo stato del gruppo oversized e tutti gli entry successivi** nello snapshot, non solo il singolo record.
- Conseguenza: perdita di checkpoint, DLS e redelivery dei gruppi coinvolti dopo il restart; replay duplicato o stato ignorato.
- Il record su disco contiene byte extra rispetto alla lunghezza dichiarata; possibile record molto grande fino al cap del frame.

### Direzione della correzione e regressioni necessarie

- Introdurre una costante condivisa `STREAM_MAX_GROUP_BYTES` in `protocol.json`/codegen; validarla lato server per JOIN/SEEK/DLS e negli SDK TS/Py.
- Conversione checked (non `as u16`) nella persistenza; valutare versione/migrazione del formato.
- Regressioni: boundary 65535 accettato, 65536 rifiutato, lunghezze UTF-8 misurate in byte, tutte le operazioni che materializzano group, round-trip dello stato.

### Riferimenti

- [`src/brokers/stream/domain/persistence.rs:753`](../../src/brokers/stream/domain/persistence.rs#L753) — `write_state_entry` (753-816): `as u16` a ~761, `content_len` a ~766, `put_slice(group_bytes)` a ~785.
- [`src/brokers/stream/domain/persistence.rs:823`](../../src/brokers/stream/domain/persistence.rs#L823) — `load_state_file`; `get_u16` a ~840.
- [`src/protocol/wire.rs:159`](../../src/protocol/wire.rs#L159) — `read_string` con prefisso `u32`.
- [`src/brokers/stream/tcp.rs:184`](../../src/brokers/stream/tcp.rs#L184) — parse di `OP_S_JOIN` senza limite sul group.
- [`sdk/ts/src/brokers/stream.ts`](../../sdk/ts/src/brokers/stream.ts) e [`sdk/py/src/nexo/brokers/stream.py`](../../sdk/py/src/nexo/brokers/stream.py) — costruttori/`subscribe` che accettano il group senza validazione di lunghezza.

</details>

<details name="stream-bugs">
<summary><strong>STREAM-11 · P1 — SDK: timeout o ACK fallito può lasciare membri orfani</strong></summary>

### Stato dell'evidenza

Evidenza statica ad alta confidenza sul codice corrente; il probe qui sotto è proposto e **non è stato eseguito**.

### Comportamento atteso

L'identità lato server resta disponibile fino a un LEAVE riuscito, a un'invalidazione esplicita del server o al disconnect; il rejoin non deve perdere la possibilità di pulire la vecchia membership.

### Scenario che attiva il bug

- **Scenario A**: JOIN produce `ID1`; un FETCH va in request timeout mentre il socket resta connesso. Il catch azzera `consumerId`; `RequestTimeoutError` è un `NexoError` e diventa terminale nel loop, quindi `stop()` non può più inviare LEAVE per `ID1`.
- **Scenario B**: il callback termina ma l'ACK fallisce con un errore transiente non di membership; `consumerId` viene azzerato e il loop può fare rejoin come `ID2`, lasciando `ID1` registrato nel gruppo.

### Probe minimo proposto

Probe proposto, non eseguito: transport stub o proxy controllato che esegue JOIN `ID1` e poi fa andare in timeout FETCH/ACK; successivamente stop/rejoin. Invariante: LEAVE per `ID1` oppure cleanup alla disconnect. Variante E2E: ripetere fino al max consumers e verificare che il gruppo non si esaurisca.

### Causa interna e flusso end-to-end

Nel loop TypeScript `this.consumerId = null` viene eseguito prima della classificazione dell'errore; in Python `self._consumer_id = None` occupa la stessa posizione. `stop()`/`leave()` leggono il campo già nullo e non possono inviare LEAVE per la vecchia identità. Il server non ha TTL sui membri: li elimina solo con leave, disconnect o seek. Gli errori di membership recoverable (`FENCED`/`NOT_MEMBER`) non sono il problema, perché il server ha già invalidato l'identità; il problema sono il timeout locale e l'ACK failure, in cui l'identità resta valida sul server ma persa sul client.

### Impatto

- Membri e binding orfani lato server fino al disconnect.
- Esaurimento del max consumers.
- Pending trattenuti fino al timeout.
- Shutdown logico della subscription incompleto.

### Direzione della correzione e regressioni necessarie

- Separare l'identità attiva dall'intento di rejoin: azzerare l'identità solo dopo un leave riuscito, un'invalidazione esplicita del server o il disconnect del socket.
- Il timeout del fetch deve avere una politica recoverable coerente che non nasconda l'ownership del consumer.
- `stop()` deve pulire tutte le identità ancora possibili, senza mascherare uno stato server corrotto lato client.
- Regressioni TS/Py: fetch timeout con socket vivo, ACK timeout/error, stop dopo errore, recovery ripetuta vicino al max consumers, percorso di disconnect.

### Riferimenti

- [`sdk/ts/src/brokers/stream.ts:287`](../../sdk/ts/src/brokers/stream.ts#L287) — `consumerId = null` prima della classificazione nel loop (276-300); `stop` a 220-233 e `leave` a 253-265.
- [`sdk/py/src/nexo/brokers/stream.py:459`](../../sdk/py/src/nexo/brokers/stream.py#L459) — `_consumer_id = None` prima della classificazione in `_loop` (443-479); `stop` a 388-413 e `_leave` a 415-427.
- [`src/brokers/stream/manager.rs:750`](../../src/brokers/stream/manager.rs#L750) — `leave_group`; [`src/brokers/stream/manager.rs:784`](../../src/brokers/stream/manager.rs#L784) — `join_group` e `client_map` (senza TTL sui membri).

</details>

<details name="stream-bugs">
<summary><strong>STREAM-12 · P1 — Python: Future e Task non vengono attesi prima dell'ACK</strong></summary>

### Stato dell'evidenza

Evidenza statica ad alta confidenza sul codice corrente; il probe qui sotto è proposto e **non è stato eseguito**. Il difetto riguarda **solo Python**: TypeScript usa `await this.callback(...)` incondizionatamente e gestisce correttamente Promise e thenable.

### Comportamento atteso

Qualsiasi oggetto compatibile con `Awaitable` restituito dal callback deve terminare con successo prima dell'ACK; una failure o una cancellazione deve lasciare il messaggio non confermato.

### Scenario che attiva il bug

Un callback sincrono restituisce un `asyncio.Future`, un `Task` o un awaitable custom: `asyncio.iscoroutine` è falso almeno per `Future` e `Task`, quindi l'SDK procede direttamente all'ACK mentre l'awaitable può ancora fallire in seguito.

### Probe minimo proposto

Probe proposto, non eseguito: callback che ritorna un `Future` non risolto; invariante: nessun ACK e nessuna completion dello stop prima del resolve; dopo il resolve, ACK inviato. Variante: un `Future` con eccezione deve causare timeout/redelivery, non ACK.

### Causa interna e flusso end-to-end

In `_poll_once`/`process` l'SDK controlla `asyncio.iscoroutine(result)` invece di `inspect.isawaitable(result)`. L'alias `StreamHandler` ritorna `Any`, quindi l'API non esclude questi awaitable: il valore viene trattato come risultato sincrono e il flusso raggiunge l'ACK.

### Impatto

- Semantica di fatto at-most-once per questa forma di callback.
- Eccezioni non associate al processing del messaggio.
- Possibile warning "Task exception was never retrieved".

### Direzione della correzione e regressioni necessarie

- Usare `inspect.isawaitable(result)` e tipizzare il callback come `Awaitable[Any] | Any`; mantenere l'error boundary prima dell'ACK.
- Regressioni Python: coroutine, `Future`, `Task`, `__await__` custom, valore sincrono, eccezione prima e dopo lo scheduling; test TS solo a salvaguardia dell'await su thenable.

### Riferimenti

- [`sdk/py/src/nexo/brokers/stream.py:527`](../../sdk/py/src/nexo/brokers/stream.py#L527) — `asyncio.iscoroutine(result)` in `process` dentro `_poll_once` (481-555).
- [`sdk/py/src/nexo/brokers/stream.py:36`](../../sdk/py/src/nexo/brokers/stream.py#L36) — alias `StreamHandler` che ritorna `Any`.
- [`sdk/ts/src/brokers/stream.ts:341`](../../sdk/ts/src/brokers/stream.ts#L341) — `await this.callback(...)` come confronto corretto (338-357).

</details>

<details name="stream-bugs">
<summary><strong>STREAM-13 · P2 — TypeScript: concurrency non validata e diversa da Python</strong></summary>

### Stato dell'evidenza

Evidenza statica ad alta confidenza sul codice corrente; il probe qui sotto è proposto e **non è stato eseguito**.

### Comportamento atteso

`concurrency` deve essere un intero finito `>= 1`, con rifiuto fail-fast identico nei due SDK.

### Scenario che attiva il bug

`0` e `-1` vengono silenziosamente clampati a 1; un float viene troncato implicitamente dall'allocazione dei worker; `NaN` produce zero worker: il batch già fetched non viene processato né ACKato e può saturare il pending fino al retry. `Infinity` crea invece un worker per elemento del batch (`Math.min(Infinity, items.length)`), eliminando di fatto il limite di concorrenza configurato.

### Probe minimo proposto

Probe proposto, non eseguito: `subscribe` con `0`, `-1`, `1.5`, `NaN`, `Infinity` — tutti i valori invalidi devono fallire prima di JOIN/FETCH; verifica di parità con Python.

### Causa interna e flusso end-to-end

`subscribe` applica `Math.max(1, options.concurrency ?? default)` prima di qualunque validazione e manca il controllo `Number.isInteger(concurrency)` presente per `batchSize`, `waitMs` e `stopTimeoutMs`. Python invece valida che il valore sia un intero positivo e rifiuta gli stessi input.

### Impatto

- Typo di configurazione nascosto, oppure subscription viva ma senza worker/ACK (`NaN`).
- `Infinity` elimina il limite di concorrenza configurato e può generare un picco di Promise e richieste ACK.
- Divergenza di comportamento tra gli SDK.

### Direzione della correzione e regressioni necessarie

- Validare il valore originale, non il risultato del clamp; introdurre un helper condiviso per "positive integer" e allineare docs e test matrix.
- Regressioni: validazione locale senza socket, più un E2E con valore valido.

### Riferimenti

- [`sdk/ts/src/brokers/stream.ts:446`](../../sdk/ts/src/brokers/stream.ts#L446) — `Math.max(1, ...)` in `subscribe` (438-454).
- [`sdk/ts/src/utils/concurrent.ts:11`](../../sdk/ts/src/utils/concurrent.ts#L11) — `runConcurrent`.
- [`sdk/py/src/nexo/brokers/stream.py:653`](../../sdk/py/src/nexo/brokers/stream.py#L653) — validazione di `concurrency` intera e positiva in Python.

</details>

<details name="stream-bugs">
<summary><strong>STREAM-14 · P2 — Payload Py/TS non interoperabili per interi grandi e JSON non finito</strong></summary>

### Stato dell'evidenza

Evidenza statica ad alta confidenza sul codice corrente; il probe qui sotto è proposto e **non è stato eseguito**.

### Comportamento atteso

Un valore pubblicato da un SDK deve produrre lo stesso valore oppure un errore upfront nell'altro; mai corruzione silenziosa né poison-message loop.

### Scenario che attiva il bug

- **Interi**: un `int` Python oltre 2^53 serializzato come `DataType.INT` (i64) viene decodificato da TypeScript come `Number(readBigInt64BE)`, con perdita di precisione.
- **JSON**: `json.dumps` di default emette `NaN`/`Infinity`, che `JSON.parse` in TypeScript rifiuta; nella direzione opposta `JSON.stringify` converte i non-finiti in `null`, alterando il dato.

### Probe minimo proposto

Probe proposto, non eseguito — puro codec cross-language, senza socket: `±(2^53+1)`, boundary i64, `NaN`/`+Inf`/`-Inf` top-level e nested. Invariante: valore esatto oppure rifiuto simmetrico nei due SDK.

### Causa interna e flusso end-to-end

`DataType.INT` è i64 sul wire, ma l'API TypeScript decodifica in `number`. Le policy JSON non sono allineate: Python accetta ed emette non-finiti, TypeScript li rifiuta in lettura e li annulla in scrittura. La `seq` dello stream resta `bigint` in TypeScript e non va confusa col payload `INT`; inoltre `readStreamDefinition` converte anche i `u64` di configurazione in `number`, ulteriore rischio di precisione.

### Impatto

- Corruzione silenziosa dei dati.
- Eccezione durante il decode, prima del callback: il batch viene decodificato prima di invocare il callback, quindi non parte alcun ACK e il loop può ripetere redelivery/rejoin/DLS in base allo stato.
- Incompatibilità non documentata tra gli SDK.

### Direzione della correzione e regressioni necessarie

- Decisione esplicita richiesta: contratto safe-integer + strict finite JSON (compatibile ma restrittivo) oppure `bigint`/nuovo `DataType` esteso (breaking per wire e API). La fase pre-production consente il breaking pulito senza compatibility layer.
- Nel breve: validare e rifiutare simmetricamente; `json.dumps(..., allow_nan=False)` in Python; replacer/validator e safe-integer guard in TypeScript.
- Regressioni: fixture condivise TS/Py, non-finiti nested, boundary i64, publish Python → consume TypeScript e inverso.

### Riferimenti

- [`sdk/ts/src/protocol/codec.ts:36`](../../sdk/ts/src/protocol/codec.ts#L36) — `decodeAny` e `decodeAnyFromBuffer` (58-72): `Number(readBigInt64BE)` alle linee 43 e 65; `JSON.parse` a 45 e 67.
- [`sdk/ts/src/protocol/codec.ts:214`](../../sdk/ts/src/protocol/codec.ts#L214) — write path `any` con `JSON.stringify` a ~224 e ~270.
- [`sdk/py/src/nexo/protocol/codec.py:69`](../../sdk/py/src/nexo/protocol/codec.py#L69) — `decode_any` e `decode_any_from_buffer` (86-100).
- [`sdk/py/src/nexo/protocol/codec.py:206`](../../sdk/py/src/nexo/protocol/codec.py#L206) — `json.dumps` senza `allow_nan=False` (anche a ~229).
- [`sdk/ts/src/brokers/stream.ts:101`](../../sdk/ts/src/brokers/stream.ts#L101) — `readStreamDefinition` con `Number(readU64())` (101-117).
- [`sdk/codec-fixtures.json`](../../sdk/codec-fixtures.json) — fixture codec condivise tra gli SDK.

</details>

<details name="stream-bugs">
<summary><strong>STREAM-15 · P2 — ACK risveglia tutti i long-poll dello stream</strong></summary>

### Stato dell'evidenza

Evidenza statica ad alta confidenza sul codice corrente; il probe qui sotto è proposto e **non è stato eseguito**.

### Comportamento atteso

Publish e delete globale svegliano lo stream; ACK/leave/seek/DLS di un gruppo svegliano solo quel gruppo, salvo un evento realmente globale.

### Scenario che attiva il bug

`G` gruppi sono al tail in long-poll; un gruppo processa `A` messaggi e invia ACK individuali. Ogni ACK incrementa l'unico watch stream-wide, quindi tutti i `G` poll si svegliano e ripianificano senza lavoro.

### Probe minimo proposto

Probe benchmark proposto, non eseguito: `N` gruppi idle più publisher/consumer attivo; strumentare il numero di `try_fetch_once` e la CPU per ACK. Invariante attesa: crescita vicina ad `A` per il gruppo interessato; il costo corrente dedotto staticamente è O(A × G).

### Causa interna e flusso end-to-end

`StreamShared` ha un unico `wake_tx: watch::Sender<u64>`; ogni `with_state_mut` — incluso `ack` — termina con `send_modify`; ogni fetch in long-poll sottoscrive lo stesso receiver. Il notifier non codifica né la causa né il gruppo, quindi qualunque mutazione di stato risveglia tutti i poll.

### Impatto

- Thundering herd, contention sul lock, lookup su storage e latenza di tail; nessuna perdita di correttezza.
- Il costo peggiora linearmente col numero di gruppi, oltre il requisito O(1)/O(log n) per transizione osservato a livello di sistema.

### Direzione della correzione e regressioni necessarie

- Due livelli: generazione globale per append/eventi di stream più notifier per gruppo; l'ACK notifica il gruppo locale solo quando libera capacità o una chiave; delete e cancel restano globali.
- Un event bus generico è più flessibile ma più complesso e non necessario ora.
- Regressioni benchmark con contatori: 1/10/100 gruppi, niente busy polling, ACK che libera una chiave, publish globale, seek/leave, stop cancellation.

### Riferimenti

- [`src/brokers/stream/manager.rs:30`](../../src/brokers/stream/manager.rs#L30) — `StreamShared` con unico `wake_tx` (30-35).
- [`src/brokers/stream/manager.rs:644`](../../src/brokers/stream/manager.rs#L644) — `wake_tx.subscribe()` nel fetch long-poll.
- [`src/brokers/stream/manager.rs:898`](../../src/brokers/stream/manager.rs#L898) — `with_state_mut` con `send_modify` a ~912; usato anche da `ack` (702-719).
- [`src/brokers/stream/manager.rs:473`](../../src/brokers/stream/manager.rs#L473) — notify su publish/commit; ~1166 e ~1201 — notify dei task in background.

</details>

## Criteri comuni per le future correzioni

- Aggiungere regressioni permanenti in Rust e copertura dei comportamenti esposti in TypeScript e Python, aggiornando [`sdk/integration-test-matrix.md`](../../sdk/integration-test-matrix.md).
- Mantenere la separazione tra manager, protocollo e trasporto. Le correzioni di stato non devono introdurre dipendenze del manager dagli adapter TCP.
- Nessuna modifica al wire è implementata o richiesta da questo documento. Se una futura soluzione ne cambia il layout, rispettare la simmetria Rust/TS/Python e il versionamento del protocollo. Eventuali cambiamenti al formato di persistenza richiedono verifiche dedicate di parsing e recovery.
- Tenere sincronizzati stato dei messaggi, indici delle redelivery, chiavi, contatori pending e deadline. Una correzione che nasconde soltanto l'errore nell'SDK o filtra la risposta finale non risolve uno stato server già corrotto.
- Confrontare le prestazioni prima e dopo le modifiche tramite `tests/stress_tests.rs`, `sdk/ts/tests/brokers/test-stress.test.ts` e `sdk/py/tests/brokers/test_stress.py`. In questa attività di sola documentazione non è stato eseguito un nuovo confronto prestazionale.
- Aggiornare la guida stream dove necessario, senza ridefinire le garanzie documentate solo per adattarle ai difetti attuali.
- Per i finding con evidenza statica (STREAM-07..15) aggiungere regressioni permanenti separate: i probe diagnostici proposti non vanno confusi con i test di regressione da mantenere in suite.
- Applicare il budget di byte prima dell'allocazione e della materializzazione dei record, non soltanto come controllo finale nell'encoder.
- La validazione lato server è la source of truth; gli SDK TypeScript e Python devono mantenere parità fail-fast sugli stessi input.
- Aggiungere test di codec cross-language su fixture condivise e fuzz su framing e length prefix.
- Per la scalabilità delle notifiche preferire notifier per gruppo, non un unico watch stream-wide.
- Un FETCH parziale entro il batch richiesto non richiede modifiche al wire; l'introduzione di `bigint`/nuovi `DataType` o un cambio del formato di persistenza possono invece richiedere versioning e migrazione.
