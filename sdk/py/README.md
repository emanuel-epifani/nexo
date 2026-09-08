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
await client.store.map.set("user:1", {"name": "Max", "role": "admin"})
user: User | None = await client.store.map.get("user:1")
await client.store.map.delete("user:1")
```

### 2. QUEUE

Provision durable resources from deployment or administrative code:

```python
result = await client.queue.create(
    "emails",
    visibility_timeout_ms=30_000,
    max_deliveries=5,
)
print(result.status, result.definition.config)
```

Application code retrieves the existing resource and fails fast when it is missing:

```python
mail_queue: NexoQueue[Email] = await client.queue.get("emails")
await mail_queue.push({"to": "test@test.com"})

async def handle_email(message: Email, meta: QueueMessageMeta) -> None:
    print(meta["id"], message)

subscription = await mail_queue.subscribe(handle_email)
await subscription.stop()
```

### 3. PUB/SUB

Topics are routing addresses and do not require provisioning:

```python
alerts: NexoTopic[Alert] = client.pubsub.topic("system-alerts")

async def on_alert(message: Alert) -> None:
    print(message)

subscription = await alerts.subscribe(on_alert)
await alerts.publish({"level": "high"})
await subscription.stop()

all_alerts = client.pubsub.pattern("system-alerts/#")
await all_alerts.subscribe(lambda message, meta: print(meta["topic"], message))
```

### 4. STREAM

```python
result = await client.stream.create("user-events")
print(result.status, result.definition.config)

stream: NexoStream[UserEvent] = await client.stream.get("user-events")
await stream.publish({"type": "login", "userId": "u1"})

async def on_event(message: UserEvent, meta: StreamMessageMeta) -> None:
    print(meta["seq"], message)

await stream.group("analytics").subscribe(on_event)
```

> Callbacks for Queue, Pub/Sub, and Stream can be sync `def` or async `async def` — the SDK handles both.



---

### Binary Payloads

All Nexo brokers (**Store, Queue, Stream, PubSub**) natively support raw binary data (`bytes`).    
Bypassing JSON serialization drastically reduces Latency, increases Throughput, and saves Bandwidth.

**Perfect for:** Video chunks, Images, Protobuf/MsgPack, Encrypted blobs.

```python
heavy_payload = b"\x00" * (1024 * 1024)
stream: NexoStream[bytes] = await client.stream.get("cctv-archive")
queue: NexoQueue[bytes] = await client.queue.get("pdf-processing")
audio_topic: NexoTopic[bytes] = client.pubsub.topic("live-audio-call")

await stream.publish(heavy_payload)
await audio_topic.publish(heavy_payload)
await client.store.map.set("user:avatar:1", heavy_payload)
await queue.push(heavy_payload)
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
