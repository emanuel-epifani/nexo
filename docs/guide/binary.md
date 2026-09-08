# Binary Payloads

All Nexo brokers natively support raw binary data (`Buffer` in TypeScript, `bytes` in Python).
Bypassing JSON serialization drastically reduces latency, increases throughput, and saves bandwidth (~30% smaller payloads).

**Perfect for:** Video chunks, Images, Protobuf/MsgPack, Encrypted blobs.

## Usage

::: code-group

```typescript
const heavyPayload = Buffer.alloc(1024 * 1024); // 1MB raw buffer
const stream = await client.stream.get<Buffer>('cctv-archive');
const queue = await client.queue.get<Buffer>('pdf-processing');
const audioTopic = client.pubsub.topic<Buffer>('live-audio-call');

// Stream: Replayable Data (e.g. CCTV Recording)
await stream.publish(heavyPayload);

// PubSub: Ephemeral Live Data (e.g. VoIP)
await audioTopic.publish(heavyPayload);

// Store: Cache Images
await client.store.map.set('user:avatar:1', heavyPayload);

// Queue: Process Files
await queue.push(heavyPayload);
```

```python
heavy_payload = b"\x00" * (1024 * 1024)  # 1MB raw bytes
stream: NexoStream[bytes] = await client.stream.get("cctv-archive")
queue: NexoQueue[bytes] = await client.queue.get("pdf-processing")
audio_topic: NexoTopic[bytes] = client.pubsub.topic("live-audio-call")

# Stream: Replayable Data (e.g. CCTV Recording)
await stream.publish(heavy_payload)

# PubSub: Ephemeral Live Data (e.g. VoIP)
await audio_topic.publish(heavy_payload)

# Store: Cache Images
await client.store.map.set("user:avatar:1", heavy_payload)

# Queue: Process Files
await queue.push(heavy_payload)
```

:::
