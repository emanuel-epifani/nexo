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
