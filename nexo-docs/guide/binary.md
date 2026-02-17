# Binary Payloads

All Nexo brokers natively support raw binary data (`Buffer`).
Bypassing JSON serialization drastically reduces latency, increases throughput, and saves bandwidth (~30% smaller payloads).

**Perfect for:** Video chunks, Images, Protobuf/MsgPack, Encrypted blobs.

## Usage

```typescript
const heavyPayload = Buffer.alloc(1024 * 1024); // 1MB raw buffer

// Stream: Replayable Data (e.g. CCTV Recording)
await client.stream('cctv-archive').publish(heavyPayload);

// PubSub: Ephemeral Live Data (e.g. VoIP)
await client.pubsub('live-audio-call').publish(heavyPayload);

// Store: Cache Images
await client.store.map.set('user:avatar:1', heavyPayload);

// Queue: Process Files
await client.queue('pdf-processing').push(heavyPayload);
```
