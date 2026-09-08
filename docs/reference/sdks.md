# SDKs

Official libraries for connecting to Nexo from your application.
All SDKs expose the same unified API across Store, Pub/Sub, Queue, and Stream brokers.

## API Model

`NexoClient` exposes one facade per broker: `client.store`, `client.queue`, `client.stream`, and `client.pubsub`.

Queue and Stream are durable resources. Administrative code provisions them with `create()`, inspects them with `describe()`, and removes them with `delete()`. Application code calls fail-fast `get()` once and keeps the returned runtime-only handle. `create()` returns a provisioning status plus the complete effective server configuration; it never returns the operational handle.

Pub/Sub topics are routing addresses rather than provisioned resources. Use `client.pubsub.topic(name)` for concrete publish/subscribe addresses and `client.pubsub.pattern(pattern)` for wildcard subscriptions. Every `subscribe()` call returns an independently owned `Subscription`; stop it through that handle rather than through the topic.

SDKs map stable wire error codes to typed exceptions, including resource-not-found and configuration-conflict errors. Connection and timeout failures are never converted into a missing-resource result.

## Versioning

Nexo is released as **a single product**: the Docker image and every SDK move to the next version together. You do not need to check compatibility tables or guess which SDK works with which image — matching versions are guaranteed to work.

This means:

- One version number covers the Docker image, the TypeScript SDK, the Python SDK, and any future SDK.
- A new SDK enters at the current Docker image version, not from `v0.1.0`.
- When the Docker image bumps, all SDKs bump at the same time.

### Example release flow

| Release | Docker image | TypeScript SDK | Python SDK | Note |
| --- | --- | --- | --- | --- |
| Initial | `v0.3.0` | `v0.3.0` | — | TypeScript SDK ships with the image. |
| New SDK | `v0.5.0` | `v0.5.0` | `v0.5.0` | Python SDK joins at the current image version. |
| Patch | `v0.5.1` | `v0.5.1` | `v0.5.1` | All artifacts bump together. |

If your Docker image is at `v0.5.1`, install SDK `v0.5.1` — same version, guaranteed compatibility.

## Available SDKs

Pick the SDK that matches your running Docker image version.

### TypeScript

- Package: [`nexo-client`](https://www.npmjs.com/package/@emanuelepifani/nexo-client)

```bash
npm install @emanuelepifani/nexo-client
```

### Python

- Package: [`nexo-client`](https://pypi.org/project/nexo-client)

```bash
pip install nexo-client
```

Or with uv:

```bash
uv add nexo-client
```
