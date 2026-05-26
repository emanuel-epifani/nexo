# Deployment

## Data Persistence

Nexo stores all data under `./data/` by default:

```
data/
├── queues/     ← Queue messages (SQLite WAL)
├── streams/    ← Stream segments (append-only files)
└── pubsub/     ← Pub/Sub retained messages (JSON files)
```

In Docker, this directory lives **inside the container** — meaning data is **lost when the container is removed**. To persist data across restarts, mount a Docker volume:

```bash
# Simple: single volume for all brokers
docker run -d \
  -p 7654:7654 \
  -v nexo-data:/app/data \
  emanuelepifani/nexo

# Or bind to a host directory
docker run -d \
  -p 7654:7654 \
  -v /mnt/ssd/nexo:/app/data \
  emanuelepifani/nexo
```

### Separate Disks (Advanced)

If your brokers have different I/O profiles (e.g. Streams write heavily, Queues need low latency), you can map each to a different disk using environment variables:

```bash
docker run -d \
  -p 7654:7654 \
  -e QUEUE_ROOT_PERSISTENCE_PATH=/data/queues \
  -e STREAM_ROOT_PERSISTENCE_PATH=/data/streams \
  -e PUBSUB_ROOT_PERSISTENCE_PATH=/data/pubsub \
  -v /mnt/fast-ssd:/data/queues \
  -v /mnt/large-hdd:/data/streams \
  -v /mnt/fast-ssd:/data/pubsub \
  emanuelepifani/nexo
```

For most deployments, a single volume is sufficient.

## Dashboard

Nexo ships with a built-in debug dashboard, but it only starts when the server is launched in **dev mode**.

- `nexo serve` — TCP only, dashboard **OFF**. This is the production default.
- `nexo dev` — TCP + dashboard on port `8080`.

```bash
# Development (dashboard ON, port 8080 exposed)
docker run -p 7654:7654 -p 8080:8080 emanuelepifani/nexo dev

# Production (dashboard OFF, default — no subcommand needed)
docker run -p 7654:7654 emanuelepifani/nexo
```

::: warning
The dashboard exposes internal state (messages, queues, topics) and is intended for local development and debugging only. Never run `nexo dev` in production.
:::

## Max Payload Size

Nexo enforces a maximum payload size per frame to prevent memory exhaustion from oversized or malicious requests. Any frame exceeding this limit is rejected at the protocol level before allocating memory.

The default limit is **10 MB**. To increase it (e.g. for large stream messages or queue payloads):

```bash
docker run -p 7654:7654 -e MAX_PAYLOAD_SIZE=52428800 emanuelepifani/nexo  # 50MB
```

## Environment Variables

| Variable | Default | Description |
|:---|:---|:---|
| `SERVER_HOST` | `127.0.0.1` | Bind address (set to `0.0.0.0` to expose on all interfaces, e.g. in Docker) |
| `SERVER_SOCKET_TCP_PORT` | `7654` | Client TCP socket port |
| `SERVER_DASHBOARD_HTTP_PORT` | `8080` | Dashboard HTTP port |
| `NEXO_LOG` | `error` | Log level (`error`, `warn`, `info`, `debug`, `trace`) |
| `MAX_PAYLOAD_SIZE` | `10485760` | Max frame payload in bytes (10 MB) |
| `QUEUE_ROOT_PERSISTENCE_PATH` | `./data/queues` | Queue data directory |
| `STREAM_ROOT_PERSISTENCE_PATH` | `./data/streams` | Stream data directory |
| `PUBSUB_ROOT_PERSISTENCE_PATH` | `./data/pubsub` | Pub/Sub data directory |
