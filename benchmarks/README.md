# Benchmarks

Cross-system, end-to-end benchmarks: each nexo broker vs an industry reference,
exercised through the real client SDKs over TCP — not in-process.

| scenario | nexo broker | reference | hot path under test |
|---|---|---|---|
| `store`   | `nexo.store`   | Redis                | `GET` under read-heavy mix (~80/20) — cache-style reads; p99 read latency is the KPI |
| `queue`   | `nexo.queue`   | RabbitMQ             | full `push → consume → ack` pipeline; sustained msgs/sec end-to-end |
| `stream`  | `nexo.stream`  | NATS JetStream       | confirmed `S_PUB` append (ingest rate); secondary: group `FETCH` drain |
| `pubsub`  | `nexo.pubsub`  | Mosquitto (MQTT)     | `PUB` routing+delivery: throughput, fan-out 1→N, pub→sub latency |

Why these hot paths: a job queue is judged on the whole enqueue→ack cycle, an event
stream on its ingest rate, a KV store on read p99, a pub/sub bus on delivery fan-out.
Each spec marks the primary workload with `"hot": true` (printed as `*`).

## Layout

```
benchmarks/
  docker-compose.yml    # nexo (built from repo) + redis + rabbitmq + nats(-js) + mosquitto
  scenarios/*.json      # shared workload specs — the single source of truth
  ts/                   # TypeScript harness (uses sdk/ts via source import + tsx)
  py/                   # Python harness (uses sdk/py, install editable)
  results/              # markdown reports: <scenario>.<ts|py>.md
```

The spec files define workload parameters (ops, workers, batch sizes, payload size,
durability profile). Each harness interprets the same spec for both systems, so a
`ts` run and a `py` run produce comparable tables — and each system's numbers come
from identical call patterns.

## Run

```bash
# 1. start the stack (nexo is built from the current working tree)
docker compose -f benchmarks/docker-compose.yml up -d --build

# 2a. TypeScript harness
cd benchmarks/ts && npm install
npm run bench:store          # or bench:queue | bench:stream | bench:pubsub
npm run bench -- store       # equivalent

# 2b. Python harness
cd benchmarks/py
uv pip install -e ../../sdk/py          # or: pip install -e ../../sdk/py
uv pip install -r requirements.txt      # or: pip install -r requirements.txt
python runner.py store
```

Endpoints are overridable via env: `NEXO_HOST`/`NEXO_PORT`, `REDIS_URL`,
`RABBITMQ_URL`, `NATS_URL`, `MQTT_HOST`/`MQTT_PORT` (TS uses `MQTT_URL`).

## Methodology & fairness rules

- **Wire-level only**: all calls go through TCP + the official client of each
  system. In-process numbers (`tests/stress_tests.rs`) are not comparable and are
  tracked separately.
- **Durability tiers, not parity**: nexo's push paths confirm at different
  points per broker, so queue reports RabbitMQ in **two explicit tiers**
  instead of one "equivalent" config: `rabbit-d` (durable queue + persistent
  msgs + publisher confirms + manual ack) and `rabbit-v` (exclusive transient
  queue + non-persistent msgs + no confirms + auto-ack). nexo.queue push is
  confirmed only after its transaction commits to the shared SQLite WAL
  (`synchronous=NORMAL`, batched commits) — crash-consistent at process level,
  with a residual power-loss window on un-fsynced commits: semantically just
  below `rabbit-d` (which fsyncs on confirm), clearly above `rabbit-v`.
  Read each nexo row against the tier it actually matches. MQTT uses QoS 1
  publish to match nexo's server-confirmed publish.
- **Same machine, same network**: all services run in one compose project; both
  sides pay the same docker bridge cost.
- **Isolated namespaces**: each workload uses fresh queue/stream/topic names so a
  drain test never reads a previous workload's messages.
- **Latency semantics**: `avg/p50/p95/p99/max` are per-operation ms. For `pipeline`
  workloads latency = enqueue→delivery (timestamp embedded in payload, producer and
  consumer share the same process clock). For `*_batch` workloads latency is per
  batch call (noted in the report).

## Adding a scenario

1. Add `scenarios/<name>.json` (`reference` names the impl key: `redis`,
   `rabbitmq`, `jetstream`, `mqtt`).
2. Implement each `op` for `nexo` and the reference in `ts/src/scenarios/<name>.ts`
   and `py/scenarios/<name>.py` (`export async function run(spec)` / `async def run(spec)`).
3. If the reference needs a new service, add it to `docker-compose.yml` and the dep
   to `ts/package.json` / `py/requirements.txt`.
