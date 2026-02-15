import CodeBlock from "@/components/CodeBlock";
import dashboardPreview from "@/assets/dashboard-preview.png";

const QuickStart = () => {
  return (
    <div className="docs-prose">
      <h1>Quick Start</h1>
      <p>Get Nexo running in under 30 seconds with Docker and the TypeScript SDK.</p>

      <h2 id="run-the-server">1. Run the Server</h2>
      <h3 id="ephemeral">Option A: Quick Start (Ephemeral)</h3>
      <p>Run in-memory for quick testing. Data persists across restarts but is lost if the container is removed.</p>
      <CodeBlock
        language="bash"
        code={`docker run -d -p 7654:7654 -p 8080:8080 nexobroker/nexo`}
      />
      <p>This exposes:</p>
      <ul>
        <li><strong>Port 7654 (TCP):</strong> Main server socket for SDK clients.</li>
        <li><strong>Port 8080 (HTTP):</strong> Built-in Web Dashboard — open <code>http://localhost:8080</code> to inspect all brokers in real-time. No extra tools needed.</li>
      </ul>
      <p>
        The Docker image is available on{" "}
        <a href="https://hub.docker.com/r/emanuelepifani/nexo" target="_blank" rel="noopener noreferrer" className="text-primary hover:underline font-medium">
          Docker Hub
        </a>.
      </p>

      <h3 id="production-mode">Option B: Production Mode (Persistent)</h3>
      <p>
        In production, <code>NEXO_ENV=prod</code> disables the Web Dashboard (only the TCP socket is exposed) 
        and you mount volumes to persist durable engine data.
      </p>

      <h4>Storage by design</h4>
      <ul>
        <li><strong>Store</strong> — Volatile by design. Pure in-memory key-value; no persistence needed.</li>
        <li><strong>Queue</strong> — Durable by design. Messages are persisted asynchronously by default (<code>file_async</code>), configurable to flush synchronously on every message (<code>file_sync</code>).</li>
        <li><strong>Stream</strong> — Durable by design. Same persistence modes as Queue. Ideal for event sourcing and audit trails.</li>
        <li><strong>Pub/Sub</strong> — Fire-and-forget by design. However, <strong>retained messages</strong> are persisted and require their own volume.</li>
      </ul>

      <p>
        Each durable engine maps to a separate volume, so you can attach different storage devices 
        (e.g. a fast NVMe SSD for queues, a large HDD for streams). If storage isolation isn't a concern, 
        you can point multiple engines to the same mount.
      </p>

      <CodeBlock
        language="bash"
        code={`docker run -d \\
  --name nexo \\
  -p 7654:7654 \\
  -v $(pwd)/nexo_queue:/storage/queue \\
  -v $(pwd)/nexo_stream:/storage/stream \\
  -v $(pwd)/nexo_pubsub:/storage/pubsub \\
  -e QUEUE_ROOT_PERSISTENCE_PATH=/storage/queue \\
  -e STREAM_ROOT_PERSISTENCE_PATH=/storage/stream \\
  -e PUBSUB_ROOT_PERSISTENCE_PATH=/storage/pubsub \\
  -e NEXO_ENV=prod \\
  nexobroker/nexo:latest`}
      />
      <p className="text-sm text-muted-foreground">
        Port 8080 is intentionally omitted — the Dashboard is disabled in production mode.
      </p>

      <h2 id="install-the-sdk">2. Install the SDK</h2>
      <CodeBlock language="bash" code={`npm install @emanuelepifani/nexo-client`} />
      <p>
        SDK package on{" "}
        <a href="https://www.npmjs.com/package/@emanuelepifani/nexo-client" target="_blank" rel="noopener noreferrer" className="text-primary hover:underline font-medium">
          npm
        </a>.
      </p>

      <h2 id="connect-and-use">3. Connect & Use</h2>
      <CodeBlock
        code={`import { NexoClient } from '@emanuelepifani/nexo-client';

// Connect once
const client = await NexoClient.connect({ host: 'localhost', port: 7654 });

// --- Store (Shared state) ---
await client.store.map.set("user:1", { name: "Max", role: "admin" });
const user = await client.store.map.get("user:1");

// --- Pub/Sub (Realtime events) ---
await client.pubsub('alerts').subscribe((msg) => console.log(msg));
await client.pubsub('alerts').publish({ level: "high" });

// --- Queue (Background jobs) ---
const mailQ = await client.queue("emails").create();
await mailQ.push({ to: "test@test.com" });
await mailQ.subscribe((msg) => console.log(msg));

// --- Stream (Event log) ---
const stream = await client.stream('user-events').create();
await stream.publish({ type: 'login', userId: 'u1' });
await stream.subscribe('analytics', (msg) => console.log(msg));`}
      />

      <h2 id="open-the-dashboard">4. Open the Dashboard</h2>
      <p>
        Navigate to <code>http://localhost:8080</code> to access the <strong>built-in Web Dashboard</strong>. 
        It's included in the container — no extra installations, no external monitoring tools. 
        Inspect stores, monitor queues, trace streams, and debug pub/sub topics in real-time.
      </p>
      <div className="mt-4 rounded-lg overflow-hidden border border-border shadow-lg">
        <img src={dashboardPreview} alt="Nexo built-in web dashboard" className="w-full" />
      </div>
    </div>
  );
};

export default QuickStart;
