Questo repo vuole essere un trade off per le startupo che necessitano dei seguenti servizi (db in meory for cache, topics e queue) ma non a livello di peformance/affidabilità enterprise. Ma a ui farebbe comood avere tutti questi servii in un unico progetto esterno.



## **1. KV Store in memory (stile REDIS)**

- get/set
- TTL
- snapshot semplice
- WAL append-only semplice

## **2. TOPIC PUB/SUB (stile MQTT)**

- best-effort
- subscriber multipli
- topic gerarchici (opzionale)
- filtri per il problema che avevamo su p3p (puoi scegliere ogni CONSUMER di sottoscriversi a EDGE/{edge_id}, e poi il PUBLISHER pubblicare solo su EDGE/{edge_id}

## **3. QUEUE (stile RabbitMQ/Kafka lite)**

- 1 publisher → 1 consumer
- consumer groups
- ACK
- retry
- DLQ
- ordering per queue

## **4. Dashboard Web**

- stato di:
    - queue length
    - subscribers
    - ops/sec
    - messaggi recenti
- polling o websocket


1- Architettura Consigliata
Usa un Unico Entrypoint e un Unico Protocollo.
- Entrypoint Unico (TCP Listener): Apri una sola porta (es. 6379 o 8080). Tutti i client si connettono lì.
- Protocollo Unificato: Non creare parser diversi per ogni "motore" (KV, Topic, Queue). Definisci un protocollo comune che incapsuli il "Comando".
Dispatcher: Una volta parsato il comando, lo smisti al modulo competente (kv_db, topic, queue).


// src/protocol.rs (ipotetico)

// 1. Unico Enum per tutti i comandi
pub enum Command {
    // Key-Value
    Set { key: String, value: String },
    Get { key: String },
    
    // Pub/Sub
    Subscribe { topic: String },
    Publish { topic: String, message: String },
    
    // Queue
    Push { queue_name: String, data: Vec<u8> },
    Pop { queue_name: String },
}

// 2. Il Parser è unico
impl Command {
    pub fn parse(input: &[u8]) -> Result<Self, Error> {
        // Qui decodifichi i byte (es. da JSON, o formato binario custom)
        // e restituisci la variante corretta.
    }
}

// src/main.rs

async fn handle_connection(socket: TcpStream, db: DbState) {
    // 3. Loop di lettura unico
    while let Some(frame) = connection.read_frame().await {
        // Parsing centralizzato
        let command = Command::parse(&frame).unwrap();

        // 4. Dispatching ai manager specifici
        match command {
            Command::Set { key, value } => db.kv_manager.set(key, value),
            Command::Subscribe { topic } => db.topic_manager.subscribe(client_id, topic),
            Command::Push { queue_name, data } => db.queue_manager.push(queue_name, data),
            // ...
        }
    }
}

TCP raw + protocollo custom binario ⭐
[1 byte: command] [4 bytes: payload_len] [N bytes: payload]

ok quidni:
- 1 entrypoint TCP raw su stessa porta
- 1 dispatcher (con coamndi prefix per namespace "KV/TOPIC/QUEUE")
- N manager (KeyValueManger, TopicManager, QueueManager ecc)
ti sembra corretto? a questo punto vorrei capire come il RESP protocol si sposa bene il mio custom protocol, come dovrebbe esser la legenda del mio protocollo?

Base: RESP standard
Comandi: NAMESPACE.ACTION format
  - KV.SET, KV.GET
  - TOPIC.SUBSCRIBE, TOPIC.PUBLISH
  - QUEUE.PUSH, QUEUE.POP, QUEUE.ACK