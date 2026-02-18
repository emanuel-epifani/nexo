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

Nexo includes a built-in debug dashboard accessible on port `8080`. It is **automatically disabled** when `NEXO_ENV=prod`.

```bash
# Development (dashboard enabled by default)
docker run -p 7654:7654 -p 8080:8080 emanuelepifani/nexo

# Production (dashboard disabled)
docker run -p 7654:7654 -e NEXO_ENV=prod emanuelepifani/nexo
```

::: warning
The dashboard exposes internal state (messages, queues, topics) and is intended for debugging only. Do not expose port `8080` publicly in production.
:::

## Environment Variables

| Variable | Default | Description |
|:---|:---|:---|
| `NEXO_ENV` | `dev` | Set to `prod` to disable dashboard |
| `SERVER_HOST` | `0.0.0.0` | Bind address |
| `SERVER_PORT` | `7654` | Client connection port |
| `DASHBOARD_PORT` | `8080` | Dashboard HTTP port |
| `NEXO_LOG` | `error` | Log level (`error`, `warn`, `info`, `debug`, `trace`) |
| `QUEUE_ROOT_PERSISTENCE_PATH` | `./data/queues` | Queue data directory |
| `STREAM_ROOT_PERSISTENCE_PATH` | `./data/streams` | Stream data directory |
| `PUBSUB_ROOT_PERSISTENCE_PATH` | `./data/pubsub` | Pub/Sub data directory |
