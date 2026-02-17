<script setup>
import { ref, computed, onMounted } from 'vue'
import { createHighlighter } from 'shiki'

const activeTab = ref('store')
const copied = ref(false)
const highlighter = ref(null)

// Initialize Shiki highlighter
onMounted(async () => {
  highlighter.value = await createHighlighter({
    themes: ['night-owl'],
    langs: ['typescript'],
  })
})

const tabs = [
  {
    key: 'store',
    label: 'Store',
    code: `// Set key
await client.store.map.set("user:1", { name: "Max", role: "admin" });
// Get key
const user = await client.store.map.get<User>("user:1");
// Del key
await client.store.map.del("user:1");`,
  },
  {
    key: 'pubsub',
    label: 'Pub/Sub',
    code: `// Define topic
const alerts = client.pubsub<AlertMsg>("system-alerts");
// Subscribe
await alerts.subscribe((msg) => console.log(msg));
// Publish
await alerts.publish({ level: "high" });`,
  },
  {
    key: 'queue',
    label: 'Queue',
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
    key: 'stream',
    label: 'Stream',
    code: `// Create topic
const stream = await client.stream<UserEvent>('user-events').create();
// Publish
await stream.publish({ type: 'login', userId: 'u1' });
// Consume
await stream.subscribe('analytics', (msg) => { console.log(\`User \${msg.userId} performed \${msg.type}\`); });
// Delete topic
await stream.delete();`,
  },
]

const engines = [
  {
    icon: 'database',
    title: 'Store',
    desc: 'In-memory key-value with granular TTL for shared state across your services.',
    perf: '4.5M ops/sec',
    link: '/guide/store',
  },
  {
    icon: 'radio',
    title: 'Pub/Sub',
    desc: 'Real-time messaging with native wildcard routing for complex event topologies.',
    perf: '3.8M msg/sec',
    link: '/guide/pubsub',
  },
  {
    icon: 'list-ordered',
    title: 'Queue',
    desc: 'Reliable job processing with priority, delayed jobs, retry and Dead Letter Queues.',
    perf: '160k ops/sec',
    link: '/guide/queue',
  },
  {
    icon: 'activity',
    title: 'Stream',
    desc: 'Persistent event log with Consumer Groups, partitions and offset tracking.',
    perf: '650k ops/sec',
    link: '/guide/stream',
  },
]

const benchmarks = [
  { engine: 'Store', throughput: '4.5M ops/sec', latency: '< 1 µs' },
  { engine: 'PubSub', throughput: '3.8M msg/sec', latency: '< 1 µs' },
  { engine: 'Stream', throughput: '650k ops/sec', latency: '1 µs' },
  { engine: 'Queue', throughput: '160k ops/sec', latency: '3 µs' },
]

const highlightedCode = computed(() => {
  const tab = tabs.find(t => t.key === activeTab.value)
  if (!tab || !highlighter.value) return ''
  return highlighter.value.codeToHtml(tab.code.trim(), {
    lang: 'typescript',
    theme: 'night-owl',
  })
})

const rawCode = computed(() => {
  const tab = tabs.find(t => t.key === activeTab.value)
  return tab?.code ?? ''
})

function handleCopy() {
  navigator.clipboard.writeText(rawCode.value)
  copied.value = true
  setTimeout(() => { copied.value = false }, 2000)
}
</script>

<template>
  <div class="nexo-landing">
    <!-- Hero -->
    <section class="hero-section">
      <div class="hero-glow" />
      <div class="hero-content">
        <div class="hero-badge">
          ⚡ High Performance · Zero Config
        </div>
        <h1 class="hero-title">
          <span class="hero-title-nexo">Nexo</span>
        </h1>
        <p class="hero-subtitle">
          <span class="hero-subtitle-white">Your Entire Stack </span>
          <span class="hero-subtitle-gradient">in a Single Binary</span>
        </p>
        <p class="hero-desc">
          <strong>Cache</strong>, <strong>Pub/Sub</strong>, <strong>Streams</strong>, <strong>Queues</strong>, and a built-in dashboard for local debugging. Everything out of the box in a single high-performance <strong>Rust</strong> binary.
        </p>
        <p class="hero-desc">
          No external dependencies, no complex setup. One SDK, one connection—just <code>docker run</code> and start coding.
        </p>
        <a href="/guide/quickstart" class="hero-cta">
          Get Started →
        </a>
      </div>
    </section>

    <!-- Engines -->
    <section class="landing-section">
      <div class="section-inner">
        <h2 class="section-title">Isolated Engines. Zero Contention.</h2>
        <p class="section-desc">
          Each engine runs on its own dedicated thread to ensure total resource isolation. 
          A heavy load on Queues or Streams will never increase your Pub/Sub latency or slow down your Store.
          You get the operational simplicity of a single binary with the rock-solid reliability of separate, dedicated services.
        </p>
        <div class="engines-grid">
          <a
            v-for="engine in engines"
            :key="engine.title"
            :href="engine.link"
            class="engine-card"
          >
            <div class="engine-card-top">
              <div class="engine-card-icon">
                <!-- Database (Store) -->
                <svg v-if="engine.icon === 'database'" xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>
                <!-- Radio (PubSub) -->
                <svg v-else-if="engine.icon === 'radio'" xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4.9 19.1C1 15.2 1 8.8 4.9 4.9"/><path d="M7.8 16.2c-2.3-2.3-2.3-6.1 0-8.4"/><path d="M16.2 7.8c2.3 2.3 2.3 6.1 0 8.4"/><path d="M19.1 4.9C23 8.8 23 15.1 19.1 19"/><circle cx="12" cy="12" r="2"/></svg>
                <!-- ListOrdered (Queue) -->
                <svg v-else-if="engine.icon === 'list-ordered'" xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="10" x2="21" y1="6" y2="6"/><line x1="10" x2="21" y1="12" y2="12"/><line x1="10" x2="21" y1="18" y2="18"/><path d="M4 6h1v4"/><path d="M4 10h2"/><path d="M6 18H4c0-1 2-2 2-3s-1-1.5-2-1"/></svg>
                <!-- Activity (Stream) -->
                <svg v-else-if="engine.icon === 'activity'" xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2"/></svg>
              </div>
              <span class="engine-card-perf">{{ engine.perf }}</span>
            </div>
            <h3 class="engine-card-title">{{ engine.title }}</h3>
            <p class="engine-card-desc">{{ engine.desc }}</p>
          </a>
        </div>
      </div>
    </section>

    <!-- Code Tabs -->
    <section class="landing-section">
      <div class="section-inner">
        <h2 class="section-title">One SDK. Total Control.</h2>
        <p class="section-desc">
          One unified client for all brokers. Full Type-safety with Generics. Zero boilerplate. Just clean, expressive APIs.
        </p>
        <div class="code-tabs-container">
          <div class="code-tabs-bar">
            <button
              v-for="tab in tabs"
              :key="tab.key"
              :class="['code-tab', { active: activeTab === tab.key }]"
              @click="activeTab = tab.key"
            >
              {{ tab.label }}
            </button>
          </div>
          <div class="code-tabs-body">
            <button class="code-copy-btn" @click="handleCopy" :title="copied ? 'Copied!' : 'Copy code'">
              {{ copied ? '✓' : '⎘' }}
            </button>
            <div class="code-tabs-highlighted" v-html="highlightedCode"></div>
          </div>
        </div>
      </div>
    </section>

    <!-- Dashboard -->
    <section class="landing-section">
      <div class="section-inner">
        <h2 class="section-title">Built-in Web Dashboard</h2>
        <p class="section-desc">
          Run the container, open <code>localhost:8080</code> — a full real-time dashboard to inspect every engine, debug messages, and monitor queues and streams. No extra tools. Zero config.
        </p>
        <div class="dashboard-img-container">
          <img src="/dashboard-preview.png" alt="Nexo built-in web dashboard" />
        </div>
      </div>
    </section>

    <!-- Performance -->
    <section class="landing-section">
      <div class="section-inner">
        <h2 class="section-title">Extreme Throughput. Sub-millisecond P99s.</h2>
        <p class="section-desc">
          Handle millions of operations per second with uncompromising stability. High-speed primitives built to stay out of your CPU's way. The bottleneck ends here.
        </p>
        <div class="perf-card">
          <table class="perf-table-full">
            <thead>
              <tr>
                <th>Engine</th>
                <th>Throughput</th>
                <th>Latency (p99)</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in benchmarks" :key="row.engine">
                <td class="perf-engine">{{ row.engine }}</td>
                <td class="perf-throughput">{{ row.throughput }}</td>
                <td class="perf-latency">{{ row.latency }}</td>
              </tr>
            </tbody>
          </table>
          <div class="perf-footnote">Benchmarks run on MacBook Pro M4 (Single Node)</div>
        </div>
      </div>
    </section>

    <!-- Footer -->
    <footer class="landing-footer">
      <div class="footer-inner">
        <span>Built by
          <a href="https://www.linkedin.com/in/emanuel-epifani/" target="_blank" rel="noopener noreferrer">Emanuel Epifani</a>
        </span>
        <span class="footer-dot">·</span>
        <a href="https://github.com/emanuel-epifani/nexo" target="_blank" rel="noopener noreferrer">GitHub</a>
        <span class="footer-dot">·</span>
        <a href="https://hub.docker.com/r/emanuelepifani/nexo" target="_blank" rel="noopener noreferrer">Docker Hub</a>
        <span class="footer-dot">·</span>
        <a href="/reference/sdks">Client SDKs</a>
        <span class="footer-dot">·</span>
        <span>MIT License</span>
      </div>
    </footer>
  </div>
</template>
