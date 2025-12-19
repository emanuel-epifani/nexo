Questo repo vuole essere un trade off per le startupo che necessitano dei seguenti servizi (db in meory for cache, topics e queue) ma non a livello di peformance/affidabilità enterprise. Ma a ui farebbe comood avere tutti questi servii in un unico progetto esterno.

All in one high performance broker single instance for all scale up need it
QUEUE, TOPIC, IN MEMORY CACHE
all scale up need in 1 container

## **1. KV Store in memory (stile REDIS)**
- cache in memory
- get/set
- TTL

## **2. TOPIC PUB/SUB (stile MQTT)**
- 1 publisher → N consumer
- topic gerarchici (filter in fase di subscribe subscribe to edge/registration o solo a edge/{edge_id}/oma-result)
- WAL append-only semplice (scivo su file ad aogni messagio))

## **3. QUEUE (stile RabbitMQ/SQS)**
- cheduling job/dialogo tra microservizi interni a cluster k8s
- 1 publisher → 1 consumer
- ACK
- retry
- DLQ
- | Domanda reale                    | Coda         |
  | -------------------------------- | ------------ |
  | Se il worker muore?              | Retry        |
  | Se va lento?                     | Backpressure |
  | Se fallisce?                     | DLQ          |
  | Se deve partire dopo?            | Delay        |
  | Se non voglio bloccare l’utente? | Async        |
  | Se ho 1 o N worker?              | Leasing      |
Nexo dovrebbe avere => Queue con ack + retry + delay + DLQ

## **4. Dashboard Web**

- stato di:
    - queue length
    - subscribers
    - ops/sec
    - messaggi recenti
    - polling o websocket

## **4. Exporter prometheus/opentelemetry?**

## **5. Auth**
