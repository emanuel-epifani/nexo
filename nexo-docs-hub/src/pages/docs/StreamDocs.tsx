import CodeBlock from "@/components/CodeBlock";

const StreamDocs = () => {
  return (
    <div className="docs-prose">
      <h1>Stream</h1>
      <p>
        <strong>Append-only immutable log with offset tracking.</strong> The source of truth for your system's 
        history — Event Sourcing, audit trails, replaying historical data.
      </p>

      <h2 id="basic-usage">Basic Usage</h2>
      <CodeBlock
        code={`// Create stream
const stream = await client.stream<UserEvent>('user-events').create();

// Publish event
await stream.publish({ type: 'login', userId: 'u1' });

// Subscribe with consumer group
await stream.subscribe('analytics', (msg) => {
  console.log(\`User \${msg.userId} performed \${msg.type}\`);
});

// Delete stream
await stream.delete();`}
      />

      <h2 id="advanced-creation">Advanced Creation</h2>
      <CodeBlock
        code={`const orders = await client.stream<Order>('orders').create({
  // SCALING
  partitions: 4,  // Max concurrent consumers per group (default: 8)

  // PERSISTENCE
  persistence: 'file_sync',  // 'memory' | 'file_sync' | 'file_async'

  // RETENTION (Cleanup Policy)
  retention: {
    maxAgeMs: 86400000,    // 1 Day (Default: 7 Days)
    maxBytes: 536870912    // 512 MB (Default: 1 GB)
  },
});`}
      />

      <h2 id="consumer-patterns">Consumer Patterns</h2>
      <h3 id="scaling">Scaling (Load Balancing)</h3>
      <p>Same group name → automatic load balancing across consumers.</p>
      <CodeBlock
        code={`// K8s pods / microservice replicas
await orders.subscribe('workers', (order) => process(order));
await orders.subscribe('workers', (order) => process(order));`}
      />

      <h3 id="broadcast">Broadcast (Independent Domains)</h3>
      <p>Different groups → each gets a full copy of the stream.</p>
      <CodeBlock
        code={`await orders.subscribe('analytics', (order) => trackMetrics(order));
await orders.subscribe('audit-log', (order) => saveAudit(order));`}
      />

      <h2 id="features">Features</h2>
      <ul>
        <li><strong>Immutable History:</strong> Events are strictly appended and never modified.</li>
        <li><strong>Consumer Groups:</strong> Maintains separate read cursors for different consumers.</li>
        <li><strong>Replayability:</strong> Consumers can rewind their offset to re-process from any point.</li>
      </ul>

      <h2 id="performance">Performance</h2>
      <p>
        <strong>650k ops/sec</strong> with 1µs latency for persisted PUBLISH operations.
      </p>
    </div>
  );
};

export default StreamDocs;
