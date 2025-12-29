# Nexo Queue Broker

Un broker di messaggistica robusto ispirato a AWS SQS, progettato per microservizi che richiedono alta affidabilità, priorità e gestione temporale dei messaggi.

## Overview (Cosa fa)

*   **At-Least-Once Delivery**: Garantisce che ogni messaggio venga consegnato almeno una volta.
*   **Competing Consumers**: Supporta più consumatori sulla stessa coda con distribuzione automatica del carico.
*   **Message Priority**: Supporta livelli di priorità (0-255) per processare prima i messaggi critici.
*   **Delayed Jobs**: Permette di posticipare la consegna di un messaggio di un tempo arbitrario.
*   **Visibility Timeout**: Protegge dai crash dei worker; se un worker non conferma il messaggio, questo torna disponibile in coda.
*   **Dead Letter Queue (DLQ)**: Sposta automaticamente i messaggi "velenosi" in una coda di scarto dopo 5 fallimenti consecutivi.
*   **Blocking Pop**: Supporta la sottoscrizione asincrona: i consumatori restano in attesa e ricevono i messaggi istantaneamente senza polling.

## Technical Flow (Come lo fa)

### 1. Storage & Indici
Lo stato della coda è gestito in memoria tramite `QueueState`, protetto da un `Mutex` per la thread-safety:
*   **Registry**: Una `HashMap` che funge da archivio centrale (Single Source of Truth) contenente il corpo e i metadati del messaggio.
*   **Indici Temporali**: `BTreeMap` usati per gestire scadenze (Delay e Visibility) con complessità $O(\log N)$.
*   **Priority Buckets**: `BTreeMap` di code `VecDeque` per garantire l'ordine FIFO all'interno di ogni livello di priorità.

### 2. Ciclo di Vita di un Messaggio
1.  **Push**: Il messaggio viene archiviato nel `Registry`. Se non ha delay, passa al `dispatch`.
2.  **Dispatch**: Il sistema controlla se ci sono `waiting_consumers` (client parcheggiati). Se sì, consegna immediatamente; altrimenti, inserisce l'ID in `waiting_for_dispatch`.
3.  **Consume/Pop**: Il messaggio viene spostato dall'indice di disponibilità a `waiting_for_ack`. In questo momento il visibility timeout inizia a scorrere.
4.  **ACK**: Il client conferma il successo. Il messaggio viene rimosso definitivamente dal `Registry` e da tutti gli indici.

### 3. Background Reaper
Ogni coda ha un task asincrono dedicato (Reaper) che gira ogni secondo:
*   **Sblocco Delay**: Sposta i messaggi da `waiting_for_time` a `waiting_for_dispatch` quando il tempo è scaduto.
*   **Gestione Timeout**: Se un messaggio in `waiting_for_ack` scade:
    *   Se `attempts < 5`: Viene rimesso in coda per una nuova consegna.
    *   Se `attempts >= 5`: Viene rimosso e inviato alla coda `{name}_dlq`.

### 4. Concorrenza
*   **Fine-Grained Locking**: Ogni coda ha il suo lock indipendente. Un'operazione sulla coda A non rallenta mai la coda B.
*   **DashMap**: Il `QueueManager` utilizza una mappa concorrente per gestire migliaia di code simultaneamente con overhead minimo.

