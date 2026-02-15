import CodeBlock from "@/components/CodeBlock";

const StoreDocs = () => {
  return (
    <div className="docs-prose">
      <h1>Store</h1>
      <p>
        <strong>In-memory concurrent data structures.</strong> Ideal for high-velocity data that needs to be 
        instantly accessible across all your services — user sessions, API rate-limiting counters, temporary caching.
      </p>

      <h2 id="map">Map</h2>
      <p>The primary data structure — a distributed key-value map with per-key TTL and full type safety.</p>

      <h3 id="basic-usage">Basic Usage</h3>
      <CodeBlock
        code={`// Set a key
await client.store.map.set("user:1", { name: "Max", role: "admin" });

// Get a key (with type inference)
const user = await client.store.map.get<User>("user:1");

// Delete a key
await client.store.map.del("user:1");`}
      />

      <h3 id="features">Features</h3>
      <ul>
        <li><strong>Granular TTL:</strong> Set expiration per-key or globally. Ideal for temporary API caches and rate-limiting counters.</li>
        <li><strong>Type Safety:</strong> Full TypeScript generics support for type-safe reads.</li>
        <li><strong>Binary Support:</strong> Store raw <code>Buffer</code> data with zero JSON overhead.</li>
      </ul>

      <h3 id="architecture">Architecture</h3>
      <CodeBlock
        code={`┌──────────────┐     SET(key, val)      ┌──────────────────┐
│   Client A   │───────────────────────▶│    NEXO STORE    │
└──────────────┘                        │   (Shared RAM)   │
┌──────────────┐      GET(key)          │    [Map<K,V>]    │
│   Client B   │◀───────────────────────│                  │
└──────────────┘                        └──────────────────┘`}
      />

      <h2 id="set">Set <span className="text-xs bg-muted px-2 py-0.5 rounded-full text-muted-foreground ml-2">coming soon</span></h2>
      <p className="text-muted-foreground">Distributed unique-value collection. Stay tuned.</p>

      <h2 id="list">List <span className="text-xs bg-muted px-2 py-0.5 rounded-full text-muted-foreground ml-2">coming soon</span></h2>
      <p className="text-muted-foreground">Distributed ordered list. Stay tuned.</p>

      <h2 id="performance">Performance</h2>
      <p>
        <strong>4.5M ops/sec</strong> with sub-microsecond latency for SET operations.
      </p>
    </div>
  );
};

export default StoreDocs;
