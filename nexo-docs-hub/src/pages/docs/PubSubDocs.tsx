import CodeBlock from "@/components/CodeBlock";

const PubSubDocs = () => {
  return (
    <div className="docs-prose">
      <h1>Pub/Sub</h1>
      <p>
        <strong>Transient message bus with Topic-based routing.</strong> Designed for "fire-and-forget" scenarios 
        where low latency is critical — live chat, stock tickers, multi-service notifications.
      </p>

      <h2 id="basic-usage">Basic Usage</h2>
      <CodeBlock
        code={`// Define a topic
const alerts = client.pubsub<AlertMsg>("system-alerts");

// Subscribe
await alerts.subscribe((msg) => console.log(msg));

// Publish
await alerts.publish({ level: "high" });`}
      />

      <h2 id="wildcards">Wildcards</h2>
      <p>Nexo supports MQTT-style wildcard subscriptions:</p>

      <h3 id="single-level-wildcard">Single-Level Wildcard (+)</h3>
      <p>Matches exactly one segment.</p>
      <CodeBlock
        code={`// Matches: 'home/kitchen/light', 'home/garage/light'
const roomLights = client.pubsub<LightStatus>('home/+/light');
await roomLights.subscribe((status) => console.log('Light is:', status.state));`}
      />

      <h3 id="multi-level-wildcard">Multi-Level Wildcard (#)</h3>
      <p>Matches all remaining segments.</p>
      <CodeBlock
        code={`// Matches all topics under 'sensors/'
const allSensors = client.pubsub<SensorData>('sensors/#');
await allSensors.subscribe((data) => console.log('Sensor value:', data.value));`}
      />

      <h2 id="retained-messages">Retained Messages</h2>
      <p>Last value is stored and immediately sent to new subscribers.</p>
      <CodeBlock
        code={`await client.pubsub<string>('config/theme').publish('dark', { retain: true });`}
      />

      <h2 id="performance">Performance</h2>
      <p>
        <strong>3.8M msg/sec</strong> fan-out with sub-microsecond latency (1 Publisher → 1k Subscribers).
      </p>
    </div>
  );
};

export default PubSubDocs;
