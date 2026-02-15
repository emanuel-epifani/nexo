import CodeBlock from "@/components/CodeBlock";

const BinaryDocs = () => {
  return (
    <div className="docs-prose">
      <h1>Binary Payloads</h1>
      <p>
        All Nexo brokers natively support raw binary data (<code>Buffer</code>). 
        Bypassing JSON serialization drastically reduces latency, increases throughput, and saves bandwidth (~30% smaller payloads).
      </p>
      <p>
        <strong>Perfect for:</strong> Video chunks, Images, Protobuf/MsgPack, Encrypted blobs.
      </p>

      <h2 id="usage">Usage</h2>
      <CodeBlock
        code={`const heavyPayload = Buffer.alloc(1024 * 1024); // 1MB raw buffer

// Stream: Replayable Data (e.g. CCTV Recording)
await client.stream('cctv-archive').publish(heavyPayload);

// PubSub: Ephemeral Live Data (e.g. VoIP)
await client.pubsub('live-audio-call').publish(heavyPayload);

// Store: Cache Images
await client.store.map.set('user:avatar:1', heavyPayload);

// Queue: Process Files
await client.queue('pdf-processing').push(heavyPayload);`}
      />
    </div>
  );
};

export default BinaryDocs;
