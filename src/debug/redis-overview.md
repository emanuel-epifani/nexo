Redis ha 10+ "motori" diversi:

1. String (KV base)        → SET, GET, INCR
2. Hash                    → HSET, HGET
3. List                    → LPUSH, RPOP
4. Set                     → SADD, SMEMBERS
5. Sorted Set              → ZADD, ZRANGE
6. Bitmap                  → SETBIT, GETBIT
7. HyperLogLog             → PFADD, PFCOUNT
8. Pub/Sub                 → SUBSCRIBE, PUBLISH
9. Stream                  → XADD, XREAD (queue-like)
10. Geospatial            → GEOADD, GEORADIUS
11. JSON (modulo)         → JSON.SET, JSON.GET




Redis è famoso proprio perché non è solo una cache chiave-valore (come Memcached), ma un Data Structure Server. Invece di salvare solo stringhe opache (blob JSON serializzati), Redis "capisce" i dati che memorizzi e ti permette di manipolarli atomicamente senza doverli scaricare, modificare e ricaricare.

Ecco cosa sono davvero quei "motori":

1. Hash (HSET, HGET) - "L'Oggetto"
   Immagina una mappa dentro la mappa.

Cos'è: Invece di key -> "json_gigante", hai key -> { field1: val1, field2: val2 }.
Perché usarlo: Se hai un User Profile e vuoi cambiare solo l'età o l'email, con le String devi riscaricare tutto il JSON, parsarlo, cambiarlo e risalvarlo. Con Hash fai HSET user:100 age 30 e modifichi solo quel campo in memoria. Efficienza pura.
2. List (LPUSH, RPOP) - "La Coda"
   Cos'è: Una Linked List doppiamente concatenata. Mantiene l'ordine di inserimento.
   Perché usarlo: Code di messaggi (Job Queue), timeline di social (i 10 post più recenti), log buffer. Puoi inserire a sinistra e leggere da destra (FIFO) o usare come stack (LIFO). È velocissimo inserire in testa o coda, lento cercare nel mezzo.
3. Set (SADD, SMEMBERS) - "L'Insieme Matematico"
   Cos'è: Una lista non ordinata che garantisce unicità. Niente duplicati.
   Perché usarlo: Tag di un articolo, liste di amici, indirizzi IP bannati. La magia sta nelle operazioni insiemistiche: puoi chiedere a Redis l'intersezione (SINTER) tra "Amici di Alice" e "Amici di Bob" e lui ti dà gli amici in comune istantaneamente.
4. Sorted Set (ZADD, ZRANGE) - "La Classifica"
   Cos'è: Come un Set (elementi unici), ma ogni elemento ha un punteggio (score) associato che lo tiene automaticamente ordinato.
   Perché usarlo: Leaderboards (classifiche giochi), indici temporali, Priority Queue. Non devi mai "ordinare" i dati: sono sempre salvati già ordinati per score.
5. Bitmap (SETBIT, GETBIT) - "Array di Bit"
   Cos'è: Tratta una stringa come un vettore di bit (0/1).
   Perché usarlo: Analytics ultra-compatti. Esempio: "Utenti attivi oggi". Usi l'ID utente come indice del bit. Se l'utente 500 fa login, metti il bit 500 a 1. In pochi MB tracci milioni di utenti.
6. HyperLogLog (PFADD) - "La Statistica Probabilistica"
   Cos'è: Una struttura dati magica che conta elementi unici con un margine di errore minimo (<1%), usando pochissima memoria fissa (12KB), sia che conti 100 elementi sia 100 miliardi.
   Perché usarlo: Contare "Visitatori Unici" su un sito ad alto traffico. Con un Set normale esploderebbe la RAM; con questo occupa sempre solo 12KB.
7. Pub/Sub (PUBLISH) - "La Radio"
   Cos'è: Non salva dati! È un canale di comunicazione. Chi ascolta (SUBSCRIBE) riceve il messaggio, se non c'è nessuno il messaggio si perde.
   Perché usarlo: Chat in tempo reale, notifiche push, invalidazione cache distribuita.
8. Stream (XADD) - "Il Log (tipo Kafka)"
   Cos'è: Una struttura "Append-Only" robusta. A differenza del Pub/Sub, i messaggi vengono salvati e i consumer possono leggerli col proprio passo, confermarli (ACK) e riprenderli se crashano.
   Perché usarlo: Event Sourcing, Activity Feeds affidabili.
9. Geospatial (GEOADD) - "Il GPS"
   Cos'è: Un Sorted Set specializzato per coordinate (latitudine/longitudine).
   Perché usarlo: "Trova ristoranti entro 5km da me". Usa algoritmi di Geohashing per fare query spaziali velocissime.




## Architecture Decision: Custom Binary Protocol

We abandoned RESP (Redis Protocol) in favor of a simpler custom Binary Protocol for learning purposes.

### Structure
`[Opcode: 1 byte] [Length: 4 bytes BE] [Payload: N bytes]`

### Opcodes
- `0x01` PING
- `0x02` KV_SET (Payload: `[KeyLen: 4B] [Key] [Value]`)
- `0x03` KV_GET (Payload: `[Key]`)
- `0x04` KV_DEL (Payload: `[Key]`)

### Architecture
- 1 entrypoint TCP raw su stessa porta (8080)
- 1 dispatcher in `routing.rs`
- N managers (KvManager, etc.)

### TODO
- per eliminare expired key on KV
- uso una struttura sorted per expired + recente in cima (radix-tree)