import { Link } from "react-router-dom";
import { ArrowRight, Database, Radio, ListOrdered, Activity, Zap, Github, Container, Monitor, Package } from "lucide-react";
import { Button } from "@/components/ui/button";
import DocsHeader from "@/components/DocsHeader";
import { motion } from "framer-motion";
import EngineCodeTabs from "@/components/EngineCodeTabs";
import dashboardPreview from "@/assets/dashboard-preview.png";

const engines = [
  {
    icon: Database,
    title: "Store",
    desc: "In-memory key-value with granular TTL for shared state across your services.",
    perf: "4.5M ops/sec",
    link: "/docs/store",
  },
  {
    icon: Radio,
    title: "Pub/Sub",
    desc: "Real-time messaging with native wildcard routing for complex event topologies.",
    perf: "3.8M msg/sec",
    link: "/docs/pubsub",
  },
  {
    icon: ListOrdered,
    title: "Queue",
    desc: "Reliable job processing with priority, delayed jobs, retry and Dead Letter Queues.",
    perf: "160k ops/sec",
    link: "/docs/queue",
  },
  {
    icon: Activity,
    title: "Stream",
    desc: "Persistent event log with Consumer Groups, partitions and offset tracking.",
    perf: "650k ops/sec",
    link: "/docs/stream",
  },
];

const Index = () => {
  return (
    <div className="min-h-screen bg-background">
      <DocsHeader />

      {/* Hero */}
      <section className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-b from-accent/50 to-transparent pointer-events-none" />
        <div className="max-w-6xl mx-auto px-6 pt-24 pb-20 relative">
          {/* Centered title + subtitle */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            className="text-center mb-12"
          >
            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full border border-border bg-card text-sm text-muted-foreground mb-6">
              <Zap className="h-3.5 w-3.5 text-primary" />
              Built in Rust · Zero Dependencies
            </div>
            <h1 className="text-3xl md:text-4xl font-extrabold tracking-tight mb-4 leading-none nexo-gradient-text">
              Nexo
            </h1>
            <p className="text-5xl md:text-6xl font-extrabold tracking-tight mb-6 leading-[1.1]">
              <span className="text-foreground">Your Entire Stack </span>
              <span className="nexo-gradient-text">in a Single Binary</span>
            </p>
            <p className="text-base text-muted-foreground leading-relaxed max-w-2xl mx-auto mb-8">
              <span className="text-foreground font-semibold">Store</span>, <span className="text-foreground font-semibold">Pub/Sub</span>, <span className="text-foreground font-semibold">Streams</span>, <span className="text-foreground font-semibold">Queues</span>, and a built-in dashboard for local debugging. Everything out of the box in a single high-performance Rust binary.
            </p>
            <p className="text-base text-muted-foreground leading-relaxed max-w-2xl mx-auto mb-8">
              No external dependencies, no complex setup. One SDK, one connection—just <code className="text-primary font-mono text-sm">docker run</code> and start coding.
            </p>
            <Button asChild size="lg" className="nexo-gradient border-0 text-primary-foreground">
              <Link to="/docs/quickstart">
                Get Started <ArrowRight className="ml-2 h-4 w-4" />
              </Link>
            </Button>
          </motion.div>
        </div>
      </section>

      {/* Section: Four Engines */}
      <section className="max-w-5xl mx-auto px-6 py-14 border-t border-border/50">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, delay: 0.2 }}
          className="text-center mb-8"
        >
          <h2 className="text-4xl font-bold mb-3">Isolated Engines. Zero Contention.</h2>
          <p className="text-muted-foreground text-sm max-w-2xl mx-auto leading-relaxed">
            Each engine runs on its own dedicated thread to ensure total resource isolation. This architecture guarantees that a heavy load on Queues or Streams will never increase your Pub/Sub latency or slow down your Store. You get the operational simplicity of a single binary with the rock-solid reliability of separate, dedicated services.
          </p>
        </motion.div>
        <div className="grid md:grid-cols-2 gap-4">
          {engines.map((engine, i) => (
            <motion.div
              key={engine.title}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.4, delay: 0.25 + i * 0.08 }}
            >
              <Link
                to={engine.link}
                className="group block p-6 rounded-xl border border-border bg-card hover:border-primary/30 hover:shadow-sm transition-all"
              >
                <div className="flex items-start justify-between mb-3">
                  <div className="h-10 w-10 rounded-lg bg-accent flex items-center justify-center">
                    <engine.icon className="h-5 w-5 text-accent-foreground" />
                  </div>
                  <span className="text-xs font-mono text-primary font-medium">{engine.perf}</span>
                </div>
                <h3 className="font-semibold text-lg mb-1 group-hover:text-primary transition-colors">
                  {engine.title}
                </h3>
                <p className="text-sm text-muted-foreground">{engine.desc}</p>
              </Link>
            </motion.div>
          ))}
        </div>
      </section>

      {/* Section: Unified SDK */}
      <section className="max-w-5xl mx-auto px-6 py-14 border-t border-border/50">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, delay: 0.4 }}
          className="text-center mb-8"
        >
          <h2 className="text-4xl font-bold mb-3">One SDK. Total Control.</h2>
          <p className="text-muted-foreground text-sm max-w-2xl mx-auto leading-relaxed">
            One unified client for all brokers. Full Type-safety with Generics. Zero boilerplate. Just clean, expressive APIs.
          </p>
        </motion.div>
        <div className="min-h-[320px]">
          <EngineCodeTabs />
        </div>
      </section>

      {/* Section: Dashboard */}
      <section className="max-w-5xl mx-auto px-6 py-14 border-t border-border/50">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, delay: 0.5 }}
          className="text-center mb-8"
        >
          <h2 className="text-4xl font-bold mb-3">Built-in Web Dashboard</h2>
          <p className="text-muted-foreground text-sm max-w-2xl mx-auto leading-relaxed">
            Run the container, open <code className="text-primary font-mono text-xs">localhost:8080</code> —
            a full real-time dashboard to inspect every engine, debug messages, and monitor queues and streams.
            No extra tools. Zero config.
          </p>
        </motion.div>
        <div className="rounded-xl overflow-hidden border border-border shadow-lg">
          <img src={dashboardPreview} alt="Nexo built-in web dashboard" className="w-full" />
        </div>
      </section>

      {/* Section: Performance */}
      <section className="max-w-5xl mx-auto px-6 py-14 border-t border-border/50">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, delay: 0.45 }}
          className="text-center mb-8"
        >
          <h2 className="text-4xl font-bold mb-3">Extreme Throughput. Sub-millisecond P99s.</h2>
          <p className="text-muted-foreground text-sm max-w-2xl mx-auto leading-relaxed">
            Handle millions of operations per second with uncompromising stability. High-speed primitives built to stay out of your CPU's way. The bottleneck ends here.
          </p>
        </motion.div>
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, delay: 0.5 }}
          className="rounded-xl border border-border overflow-hidden bg-card"
        >
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left px-6 py-3 font-semibold text-foreground">Engine</th>
                  <th className="text-left px-6 py-3 font-semibold text-foreground">Throughput</th>
                  <th className="text-left px-6 py-3 font-semibold text-foreground">Latency (p99)</th>
                </tr>
              </thead>
              <tbody>
                {[
                  { engine: "Store", throughput: "4.5M ops/sec", latency: "< 1 µs" },
                  { engine: "PubSub", throughput: "3.8M msg/sec", latency: "< 1 µs" },
                  { engine: "Stream", throughput: "650k ops/sec", latency: "1 µs" },
                  { engine: "Queue", throughput: "160k ops/sec", latency: "3 µs" },
                ].map((row) => (
                  <tr key={row.engine} className="border-b border-border/50 last:border-0">
                    <td className="px-6 py-3 font-medium text-foreground">{row.engine}</td>
                    <td className="px-6 py-3 font-mono text-primary">{row.throughput}</td>
                    <td className="px-6 py-3 font-mono text-muted-foreground">{row.latency}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="px-6 py-3 border-t border-border/50 text-xs text-muted-foreground">
            Benchmarks run on MacBook Pro M4 (Single Node)
          </div>
        </motion.div>
      </section>

      {/* Footer */}
      <footer className="border-t border-border py-8 text-center text-sm text-muted-foreground">
        <div className="flex items-center justify-center gap-4 flex-wrap">
          <span>Built by{" "}
            <a href="https://www.linkedin.com/in/emanuel-epifani/" target="_blank" rel="noopener noreferrer" className="text-foreground hover:text-primary transition-colors">
              Emanuel Epifani
            </a>
          </span>
          <span>·</span>
          <a href="https://github.com/emanuel-epifani/nexo" target="_blank" rel="noopener noreferrer" className="text-foreground hover:text-primary transition-colors inline-flex items-center gap-1">
            <Github className="h-3.5 w-3.5" /> GitHub
          </a>
          <span>·</span>
          <a href="https://hub.docker.com/r/emanuelepifani/nexo" target="_blank" rel="noopener noreferrer" className="text-foreground hover:text-primary transition-colors inline-flex items-center gap-1">
            <Container className="h-3.5 w-3.5" /> Docker Hub
          </a>
          <span>·</span>
          <Link to="/docs/sdks" className="text-foreground hover:text-primary transition-colors inline-flex items-center gap-1">
            <Package className="h-3.5 w-3.5" /> Client SDKs
          </Link>
          <span>·</span>
          <span>MIT License</span>
        </div>
      </footer>
    </div>
  );
};

export default Index;
