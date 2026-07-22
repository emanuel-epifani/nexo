# Nexo Client SDK

High-performance Python client for [Nexo](https://nexo-docs-hub.vercel.app/).



## Quick Start


### Run server
```bash
docker run -p 7654:7654 emanuelepifani/nexo:latest
```
This exposes:
- Port 7654 (TCP): Main server socket for SDK clients.

### Install SDK

```bash
uv add nexo-client
```

Or with pip:
```bash
pip install nexo-client
```


### Connection
```python
from nexo import NexoClient

client = await NexoClient.connect(host="localhost", port=7654)
```

### 1. STORE

```python
# Set key
await client.store.map.set("user:1", {"name": "Max", "role": "admin"})
# Get key
user = await client.store.map.get("user:1")
# Delete key
await client.store.map.delete("user:1")
```

### 2. QUEUE

```python
# Create queue
mail_q = await client.queue("emails").create()
# Push message
await mail_q.push({"to": "test@test.com"})
# Subscribe
await mail_q.subscribe(lambda msg: print(msg))
# Delete queue
await mail_q.delete()
```

### 3. PUB/SUB

```python
# Define topic (no need to create, auto-created on first publish)
alerts = client.pubsub("system-alerts")
# Subscribe
await alerts.subscribe(lambda msg: print(msg))
# Publish
await alerts.publish({"level": "high"})
```

### 4. STREAM

```python
# Create topic
stream = await client.stream("user-events").create()
# Publisher
await stream.publish({"type": "login", "userId": "u1"})
# Consumer (must specify group)
await stream.subscribe("analytics", lambda msg, meta: print(f"User {msg['userId']} performed {msg['type']}"))
# Delete topic
await stream.delete()
```



---

### Binary Payloads

All Nexo brokers (**Store, Queue, Stream, PubSub**) natively support raw binary data (`bytes`).    
Bypassing JSON serialization drastically reduces Latency, increases Throughput, and saves Bandwidth.

**Perfect for:** Video chunks, Images, Protobuf/MsgPack, Encrypted blobs.

```python
# Send 1MB raw bytes (30% smaller than JSON/Base64)
heavy_payload = b"\x00" * (1024 * 1024)

# 1. STREAM
await client.stream("cctv-archive").publish(heavy_payload)
# 2. PUBSUB
await client.pubsub("live-audio-call").publish(heavy_payload)
# 3. STORE
await client.store.map.set("user:avatar:1", heavy_payload)
# 4. QUEUE
await client.queue("pdf-processing").push(heavy_payload)
```

---

## License

MIT


## Links

- **Full Documentation:** [Nexo Docs](https://nexo-docs-hub.vercel.app/)
- **Docker Image:** [emanuelepifani/nexo](https://hub.docker.com/r/emanuelepifani/nexo)
- **PyPI:** [nexo-client](https://pypi.org/project/nexo-client/)

## Author

Built by **Emanuel Epifani**.

- [LinkedIn](https://www.linkedin.com/in/emanuel-epifani/)
- [GitHub](https://github.com/emanuel-epifani)
