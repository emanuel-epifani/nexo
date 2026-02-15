import CodeBlock from "@/components/CodeBlock";
import { Package } from "lucide-react";

const sdks = [
  {
    language: "TypeScript / JavaScript",
    package: "@emanuelepifani/nexo-client",
    npmUrl: "https://www.npmjs.com/package/@emanuelepifani/nexo-client",
    install: "npm install @emanuelepifani/nexo-client",
    available: true,
  },
  {
    language: "Python",
    package: "nexo-client",
    available: false,
  },
];

const SDKs = () => {
  return (
    <div className="docs-prose">
      <h1>Client SDKs</h1>
      <p>
        Official client libraries for connecting to Nexo from your application.
        All SDKs provide the same unified API to access Store, Pub/Sub, Queue, and Stream brokers.
      </p>

      <h2 id="available">Available</h2>
      {sdks.filter(s => s.available).map((sdk) => (
        <div key={sdk.language} className="rounded-xl border border-border bg-card p-6 mb-4">
          <div className="flex items-center gap-3 mb-3">
            <Package className="h-5 w-5 text-primary" />
            <h3 className="text-lg font-semibold m-0">{sdk.language}</h3>
          </div>
          <p className="text-sm text-muted-foreground mb-3">
            <a href={sdk.npmUrl} target="_blank" rel="noopener noreferrer" className="text-primary hover:underline font-mono">
              {sdk.package}
            </a>
          </p>
          <CodeBlock language="bash" code={sdk.install!} />
        </div>
      ))}

      <h2 id="coming-soon">Coming Soon</h2>
      <div className="grid sm:grid-cols-1 gap-3">
        {sdks.filter(s => !s.available).map((sdk) => (
          <div key={sdk.language} className="rounded-xl border border-border bg-card p-4 opacity-50">
            <h4 className="text-sm font-semibold mb-1">{sdk.language}</h4>
            <p className="text-xs text-muted-foreground font-mono">{sdk.package}</p>
          </div>
        ))}
      </div>
    </div>
  );
};

export default SDKs;
