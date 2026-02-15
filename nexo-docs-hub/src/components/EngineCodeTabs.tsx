import { useState } from "react";
import { Check, Copy, Database, Radio, ListOrdered, Activity } from "lucide-react";
import { Highlight, themes } from "prism-react-renderer";

const tabs = [
  {
    key: "store",
    label: "Store",
    icon: Database,
    code: `// Set key
await client.store.map.set("user:1", { name: "Max", role: "admin" });
// Get key
const user = await client.store.map.get<User>("user:1");
// Del key
await client.store.map.del("user:1");`,
  },
  {
    key: "pubsub",
    label: "Pub/Sub",
    icon: Radio,
    code: `// Define topic (auto-created on first publish)
const alerts = client.pubsub<AlertMsg>("system-alerts");
// Subscribe
await alerts.subscribe((msg) => console.log(msg));
// Publish
await alerts.publish({ level: "high" });`,
  },
  {
    key: "queue",
    label: "Queue",
    icon: ListOrdered,
    code: `// Create queue
const mailQ = await client.queue<MailJob>("emails").create();
// Push message
await mailQ.push({ to: "test@test.com" });
// Subscribe
await mailQ.subscribe((msg) => console.log(msg));
// Delete queue
await mailQ.delete();`,
  },
  {
    key: "stream",
    label: "Stream",
    icon: Activity,
    code: `// Create topic
const stream = await client.stream<UserEvent>('user-events').create();
// Publish
await stream.publish({ type: 'login', userId: 'u1' });
// Consume (must specify consumer group)
await stream.subscribe('analytics', (msg) => {
  console.log(\`User \${msg.userId} performed \${msg.type}\`);
});
// Delete topic
await stream.delete();`,
  },
];

const EngineCodeTabs = () => {
  const [activeTab, setActiveTab] = useState("store");
  const [copied, setCopied] = useState(false);

  const active = tabs.find((t) => t.key === activeTab)!;

  const handleCopy = () => {
    navigator.clipboard.writeText(active.code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="rounded-xl border border-border overflow-hidden bg-[hsl(220,20%,6%)] flex flex-col h-full">
      {/* Tab bar */}
      <div className="flex border-b border-border bg-[hsl(220,20%,8%)]">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`flex items-center gap-1.5 px-4 py-2.5 text-xs font-medium transition-colors border-b-2 ${
              activeTab === tab.key
                ? "border-primary text-primary"
                : "border-transparent text-muted-foreground hover:text-foreground"
            }`}
          >
            <tab.icon className="h-3.5 w-3.5" />
            {tab.label}
          </button>
        ))}
      </div>

      {/* Code area */}
      <div className="relative flex-1 group">
        <button
          onClick={handleCopy}
          className="absolute top-3 right-3 p-1.5 rounded-md opacity-0 group-hover:opacity-100 transition-opacity bg-[hsl(220,14%,18%)] hover:bg-[hsl(220,14%,24%)] z-10"
          aria-label="Copy code"
        >
          {copied ? (
            <Check className="h-3.5 w-3.5 text-primary" />
          ) : (
            <Copy className="h-3.5 w-3.5 text-muted-foreground" />
          )}
        </button>
        <Highlight
          theme={themes.nightOwl}
          code={active.code.trim()}
          language="typescript"
        >
          {({ style, tokens, getLineProps, getTokenProps }) => (
            <pre
              className="overflow-x-auto p-4 leading-relaxed text-sm h-full"
              style={{ ...style, margin: 0, background: "transparent" }}
            >
              {tokens.map((line, i) => (
                <div key={i} {...getLineProps({ line })}>
                  {line.map((token, key) => (
                    <span key={key} {...getTokenProps({ token })} />
                  ))}
                </div>
              ))}
            </pre>
          )}
        </Highlight>
      </div>
    </div>
  );
};

export default EngineCodeTabs;
