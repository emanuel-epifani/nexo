import CodeBlock from "@/components/CodeBlock";

const Architecture = () => {
  return (
    <div className="docs-prose">
      <h1>Architecture</h1>
      <p>
        Nexo runs as a <strong>single binary</strong> that exposes 4 distinct brokers and a built-in dashboard.
      </p>

      <ul>
        <li><strong>Zero Dependencies:</strong> No external databases, no JVM, no Erlang VM. Just one executable.</li>
        <li><strong>Thread-Isolated:</strong> Each broker runs on its own dedicated thread pool. Heavy processing on the Queue won't block Pub/Sub latency.</li>
        <li><strong>Unified Interface:</strong> A single TCP connection handles all protocols, reducing connection overhead.</li>
        <li><strong>Embedded Observability:</strong> The server hosts its own Web UI for instant visibility into every broker's state.</li>
      </ul>

      <h2 id="system-diagram">System Diagram</h2>

      <CodeBlock
        title="Architecture Overview"
        code={`                         ┌──────────────────────────────────────┐
                         │              NEXO SERVER             │
                         │                                      │       ┌──────────────┐
                         │   ┌──────────────────────────────┐   │──────▶│     RAM      │
                         │   │            STORE             │   │       │  (Volatile)  │
  ┌─────────────┐        │   └──────────────────────────────┘   │       └──────────────┘
  │   Client    │───────▶│   ┌──────────────────────────────┐   │
  │  (SDK/API)  │        │   │            PUBSUB            │   │
  └─────────────┘        │   └──────────────────────────────┘   │
                         │   ┌──────────────────────────────┐   │       ┌──────────────┐
                         │   │            QUEUE             │   │──────▶│     DISK     │
                         │   └──────────────────────────────┘   │       │  (Durable)   │
                         │   ┌──────────────────────────────┐   │──────▶└──────────────┘
                         │   │           STREAM             │   │
                         │   └──────────────────────────────┘   │
                         └───────────────┬──────────────────────┘
                                         │
                                         ▼
                                 ┌─────────────────┐
                                 │    Dashboard    │
                                 └─────────────────┘`}
      />

      <h2 id="the-four-brokers">The Four Brokers</h2>
      <p>Each broker is purpose-built to solve a specific architectural pattern:</p>
      <ul>
        <li><strong>Store</strong> replaces external caches (like Redis) for shared state.</li>
        <li><strong>Pub/Sub</strong> replaces message buses (like MQTT/Redis PubSub) for real-time volatility.</li>
        <li><strong>Queue</strong> replaces job queues (like RabbitMQ/SQS) for reliable background work.</li>
        <li><strong>Stream</strong> replaces event logs (like Kafka) for durable history.</li>
      </ul>
    </div>
  );
};

export default Architecture;
