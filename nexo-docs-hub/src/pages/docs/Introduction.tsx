import CodeBlock from "@/components/CodeBlock";
import { Badge } from "@/components/ui/badge";

const Introduction = () => {
  return (
    <div className="docs-prose">
      <div className="flex items-center gap-3 mb-2">
        <Badge variant="outline" className="text-xs border-primary/30 text-primary">v0.1.1</Badge>
      </div>
      <h1>Introduction</h1>
      <p>
        <strong>Nexo</strong> is a high-performance, all-in-one message broker built in Rust. 
        It unifies <strong>Caching</strong>, <strong>Pub/Sub</strong>, <strong>Queues</strong>, and <strong>Streams</strong> into 
        a single binary with zero external dependencies.
      </p>

      <h2 id="the-problem">The Problem</h2>
      <p>
        Modern event-driven backends suffer from <strong>Infrastructure Fatigue</strong>. A typical stack requires 
        juggling multiple specialized services — one for caching, one for job queues, one for event streams, 
        one for real-time messaging — each with its own container, protocol, configuration, and SDK.
      </p>
      <p>
        The operational overhead is disproportionate to the actual problems being solved. Setting up a local 
        environment that mirrors production becomes a project in itself, and keeping environments in sync 
        is a constant source of friction.
      </p>

      <h2 id="the-solution">The Solution</h2>
      <p>
        Nexo offers a <strong>pragmatic trade-off</strong>: it sacrifices "infinite horizontal scale" for 
        <strong> operational simplicity</strong> and <strong>vertical performance</strong>. One TCP connection. 
        One binary. Four engines.
      </p>

      <ul>
        <li><strong>Unified:</strong> One TCP connection for Caching, Pub/Sub, Queues, and Streams.</li>
        <li><strong>Simple:</strong> Deploy a single binary. No clusters to manage. No JVMs to tune.</li>
        <li><strong>Fast:</strong> Built in Rust on top of Tokio for extreme throughput and incredibly low latency.</li>
        <li><strong>Observable:</strong> Built-in Web UI for local development. Inspect all engines in real-time.</li>
        <li><strong>Consistent:</strong> Same setup locally and in production. One Docker container, one endpoint.</li>
      </ul>

      <h2 id="performance">Performance</h2>
      <p>Benchmarks run on MacBook Pro M4 (Single Node):</p>
      <div className="overflow-x-auto mb-6">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b">
              <th className="text-left py-2 pr-4 font-semibold text-foreground">Engine</th>
              <th className="text-left py-2 pr-4 font-semibold text-foreground">Throughput</th>
              <th className="text-left py-2 pr-4 font-semibold text-foreground">Latency (p99)</th>
            </tr>
          </thead>
          <tbody className="text-muted-foreground">
            <tr className="border-b border-border/50">
              <td className="py-2 pr-4 font-medium text-foreground">Store</td>
              <td className="py-2 pr-4">4.5M ops/sec</td>
              <td className="py-2 pr-4">&lt; 1 µs</td>
            </tr>
            <tr className="border-b border-border/50">
              <td className="py-2 pr-4 font-medium text-foreground">PubSub</td>
              <td className="py-2 pr-4">3.8M msg/sec</td>
              <td className="py-2 pr-4">&lt; 1 µs</td>
            </tr>
            <tr className="border-b border-border/50">
              <td className="py-2 pr-4 font-medium text-foreground">Stream</td>
              <td className="py-2 pr-4">650k ops/sec</td>
              <td className="py-2 pr-4">1 µs</td>
            </tr>
            <tr>
              <td className="py-2 pr-4 font-medium text-foreground">Queue</td>
              <td className="py-2 pr-4">160k ops/sec</td>
              <td className="py-2 pr-4">3 µs</td>
            </tr>
          </tbody>
        </table>
      </div>

      <h2 id="quick-example">Quick Example</h2>
      <CodeBlock
        code={`import { NexoClient } from '@emanuelepifani/nexo-client';

const client = await NexoClient.connect({ host: 'localhost', port: 7654 });

// Store
await client.store.map.set("user:1", { name: "Max", role: "admin" });

// Pub/Sub
await client.pubsub('alerts').publish({ level: "high" });

// Queue
const q = await client.queue("emails").create();
await q.push({ to: "test@test.com" });

// Stream
const stream = await client.stream('events').create();
await stream.publish({ type: 'login', userId: 'u1' });`}
      />
    </div>
  );
};

export default Introduction;
