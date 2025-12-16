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

ok quindi:
- 1 entrypoint TCP raw su stessa porta
- 1 dispatcher (con coamndi prefix per namespace "KV/TOPIC/QUEUE")
- N manager (KeyValueManger, TopicManager, QueueManager ecc)
  ti sembra corretto? a questo punto vorrei capire come il RESP protocol si sposa bene il mio custom protocol, come dovrebbe esser la legenda del mio protocollo?

- per eliminare expired key on KV
- uso una struttura sorted per expired + recente in cima (radix-tree)