1. Differenza Semantica: Code vs Stream
   QUEUE (RabbitMQ, SQS, Redis List):
   Natura: Transitoria. Il messaggio esiste finché qualcuno non lo consuma e fa ACK. Poi poof, sparisce.
   Persistenza: Serve solo a sopravvivere a un crash del server prima che il messaggio venga consumato. Una volta consumato, cancellarlo dal disco è costoso (compattazione/garbage collection).
   Accesso: Principalmente sequenziale (FIFO), molte cancellazioni sparse. 

   La persistenza è "un'assicurazione".
   Default: AofAsync (flush ogni 1 secondo).
   Perché: Se perdi gli ultimi 50ms di job in coda per un crash, spesso è accettabile. Se sono pagamenti bancari, l'utente configurerà AofSync.
   Sfida: La vera sfida è cancellare i messaggi dal disco quando vengono ACKati. Un semplice append-only file cresce all'infinito. 
   Dovrai implementare una logica di "compattazione" o usare file segmentati che cancelli quando tutti i messaggi dentro sono morti.

   Queue Engine: Append-only con logica di "tombstones" (segno i messaggi cancellati) e compattazione periodica. (Più complesso).


2. STREAM / TOPIC (Kafka, NATS JetStream, EventStore):
   Natura: Log Append-Only. I messaggi sono immutabili e restano lì per sempre (o fino a una retention policy).
   Persistenza: È il cuore del sistema. Il messaggio è il file su disco.
   Accesso: Scrittura sequenziale in coda, lettura sequenziale da vari offset.

   La persistenza è "lo storage".
   Default: AofAsync (flush ogni 1 secondo o raggiunti X byte).
   Perché: Scrivere sequenzialmente è velocissimo.
   Modello: Scrivi in un file (es. topic-001.log). Quando arriva a 100MB, ne apri uno nuovo (topic-002.log). 
   Molto più facile delle code perché non devi cancellare singoli messaggi nel mezzo, ma solo vecchi file interi (retention).

   Stream Engine: Append-only puro, rotazione dei file. (Più facile da fare).




QUEUES:
    ┌─────────────────────────────────────────┐
    │           LIVELLO 1: MEMORIA            │
    │    Jobs attivi, processing, queues      │
    │         (1.9M ops/sec)                  │
    └─────────────────┬───────────────────────┘
                      │
    ┌─────────────────▼───────────────────────┐
    │           LIVELLO 2: SQLite (Locale)    │
    │    WAL mode per durability immediata    │
    │         (recovery < 1 sec)              │
    └─────────────────┬───────────────────────┘
                      │ Backup periodico
    ┌─────────────────▼───────────────────────┐
    │            LIVELLO 3: S3 (Cloud)        │
    │    Backup compressi per disaster rec    │
    │         (retention a lungo termine)     │
    └─────────────────────────────────────────┘

```typescript
// PERSISTENCE CONFIGURATION
// -------------------------
// You can configure how data is persisted to disk when creating a Queue or Stream.

// 1. MEMORY (Default)
// Fastest. Data is lost if server restarts.
const memQueue = await client.queue('logs').create({
    persistence: { strategy: 'memory' }
});

// 2. FILE SYNC (Maximum Durability)
// Slower. Every message is fsync'd to disk immediately.
const safeQueue = await client.queue('payments').create({
    persistence: { strategy: 'file_sync' }
});

// 3. FILE ASYNC (Balanced)
// Fast. Data is flushed to disk periodically (default: 1000ms server-side).
const balancedQueue = await client.queue('analytics').create({
    persistence: {
        strategy: 'file_async',
        flushIntervalMs: 500 // Optional: override flush interval
    }
});
```


STREAM:
