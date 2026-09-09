# Stream — Bug nelle logiche end-to-end

> **Stato:** analisi e documentazione; nessuna correzione applicata.
> **Ambito:** consumer group, fetch/ACK, seek, DLS, retention e recupero dopo riavvio.
> **Priorità:** cinque problemi P1 di correttezza e un problema P2 di latenza.

Ogni bug è contenuto in una sezione inizialmente chiusa. Nei renderer che supportano l'attributo HTML `name` di `<details>`, aprire una sezione chiude automaticamente quella precedente. Negli altri renderer le sezioni possono essere aperte e chiuse individualmente.

## Come leggere le evidenze

- La suite Rust esistente è stata eseguita con `cargo test --offline --test stream_tests -- --test-threads=1`: **78 test superati**.
- Sono state create ed eseguite **7 riproduzioni temporanee**, che falliscono sulle invarianti attese e confermano i **6 bug distinti**. Il bug DLS ha due riproduzioni, una per ciascuna operazione coinvolta.
- Le riproduzioni usano le API del `StreamManager` con directory di persistenza temporanee; non sono mock della logica del gruppo.
- Il file temporaneo `tests/stream_review_repro.rs` è stato rimosso al termine della review. I nomi e i risultati riportati qui documentano quelle esecuzioni: **non identificano test attualmente presenti nel repository**.
- Gli SDK TypeScript e Python sono stati verificati leggendo il codice. Il sandbox ha impedito l'apertura di socket locali: **non è stata eseguita una verifica TCP live degli SDK**.
- Le proposte di correzione e i test di regressione elencati nelle sezioni sono lavoro futuro, non modifiche già implementate.

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

- [`src/brokers/stream/manager.rs:464`](../../src/brokers/stream/manager.rs#L464) — conversione degli errori di appartenenza in risposta vuota.
- [`src/brokers/stream/domain/group.rs:362`](../../src/brokers/stream/domain/group.rs#L362) — reset dei membri e della generazione.
- [`sdk/ts/src/brokers/stream.ts:166`](../../sdk/ts/src/brokers/stream.ts#L166) — loop e rejoin TypeScript.
- [`sdk/py/src/nexo/brokers/stream.py:200`](../../sdk/py/src/nexo/brokers/stream.py#L200) — recupero dell'identità nel percorso di errore Python.
- [`docs/guide/stream.md:494`](../../docs/guide/stream.md#L494) — comportamento documentato di seek e replay.

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

- [`src/brokers/stream/domain/group.rs:436`](../../src/brokers/stream/domain/group.rs#L436) — snapshot che esclude i pending.
- [`src/brokers/stream/domain/group.rs:125`](../../src/brokers/stream/domain/group.rs#L125) — ricostruzione del cursore dal floor.
- [`src/brokers/stream/manager.rs:111`](../../src/brokers/stream/manager.rs#L111) — salvataggio finale allo shutdown.
- [`tests/stream_tests.rs:1795`](../../tests/stream_tests.rs#L1795) — test esistente del riavvio prima della nuova consegna.
- [`docs/guide/stream.md:640`](../../docs/guide/stream.md#L640) — persistenza documentata dei redrive.

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

- [`src/brokers/stream/domain/group.rs:269`](../../src/brokers/stream/domain/group.rs#L269) — allineamento del gruppo al nuovo head.
- [`src/brokers/stream/domain/group.rs:313`](../../src/brokers/stream/domain/group.rs#L313) — pulizia della chiave senza riattivare il successore.
- [`src/brokers/stream/domain/group.rs:454`](../../src/brokers/stream/domain/group.rs#L454) — avanzamento del floor basato su `msgs`.
- [`src/brokers/stream/domain/group.rs:208`](../../src/brokers/stream/domain/group.rs#L208) — percorso ACK che normalmente libera un successore.

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

- [`src/brokers/stream/domain/group.rs:605`](../../src/brokers/stream/domain/group.rs#L605) — `move_to_stream`.
- [`src/brokers/stream/domain/group.rs:621`](../../src/brokers/stream/domain/group.rs#L621) — `delete_dls`.
- [`src/brokers/stream/domain/group.rs:208`](../../src/brokers/stream/domain/group.rs#L208) — verifica del pending durante l'ACK.
- [`src/brokers/stream/domain/group.rs:241`](../../src/brokers/stream/domain/group.rs#L241) — elaborazione dei timeout di consegna.

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

- [`src/brokers/stream/domain/group.rs:99`](../../src/brokers/stream/domain/group.rs#L99) — ripristino delle redelivery e del cursore fresh.
- [`src/brokers/stream/domain/group.rs:160`](../../src/brokers/stream/domain/group.rs#L160) — consegna dalla lista redelivery.
- [`src/brokers/stream/domain/group.rs:189`](../../src/brokers/stream/domain/group.rs#L189) — seconda visita dal percorso fresh.
- [`src/brokers/stream/domain/group.rs:474`](../../src/brokers/stream/domain/group.rs#L474) — assegnazione e controllo della chiave.
- [`src/brokers/stream/domain/group.rs:525`](../../src/brokers/stream/domain/group.rs#L525) — incremento del contatore pending.

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

- [`src/brokers/stream/domain/group.rs:338`](../../src/brokers/stream/domain/group.rs#L338) — piano limitato al budget.
- [`src/brokers/stream/domain/group.rs:144`](../../src/brokers/stream/domain/group.rs#L144) — filtraggio e avanzamento del cursore.
- [`src/brokers/stream/manager.rs:461`](../../src/brokers/stream/manager.rs#L461) — interpretazione del risultato vuoto nel long-poll.
- [`src/brokers/stream/manager.rs:906`](../../src/brokers/stream/manager.rs#L906) — lettura ed esecuzione di un singolo piano.
- [`sdk/ts/src/config.ts:38`](../../sdk/ts/src/config.ts#L38) — impostazioni predefinite dello stream nel client TypeScript.

</details>

## Criteri comuni per le future correzioni

- Aggiungere regressioni permanenti in Rust e copertura dei comportamenti esposti in TypeScript e Python, aggiornando [`sdk/integration-test-matrix.md`](../../sdk/integration-test-matrix.md).
- Mantenere la separazione tra manager, protocollo e trasporto. Le correzioni di stato non devono introdurre dipendenze del manager dagli adapter TCP.
- Nessuna modifica al wire è implementata o richiesta da questo documento. Se una futura soluzione ne cambia il layout, rispettare la simmetria Rust/TS/Python e il versionamento del protocollo. Eventuali cambiamenti al formato di persistenza richiedono verifiche dedicate di parsing e recovery.
- Tenere sincronizzati stato dei messaggi, indici delle redelivery, chiavi, contatori pending e deadline. Una correzione che nasconde soltanto l'errore nell'SDK o filtra la risposta finale non risolve uno stato server già corrotto.
- Confrontare le prestazioni prima e dopo le modifiche tramite `tests/stress_tests.rs`, `sdk/ts/tests/brokers/test-stress.test.ts` e `sdk/py/tests/brokers/test_stress.py`. In questa attività di sola documentazione non è stato eseguito un nuovo confronto prestazionale.
- Aggiornare la guida stream dove necessario, senza ridefinire le garanzie documentate solo per adattarle ai difetti attuali.
