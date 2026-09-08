# Quick Start

Get Nexo running in under 30 seconds with Docker and the TypeScript SDK.

## 1. Run the Server


The Docker image is available on [Docker Hub](https://hub.docker.com/r/emanuelepifani/nexo).

```bash
docker run -d -p 7654:7654 emanuelepifani/nexo
```

This exposes:

- **Port 7654 (TCP):** Client TCP socket for SDK connections.


## 2. Install the SDK

SDK package on [npm](https://www.npmjs.com/package/@emanuelepifani/nexo-client).

```bash
npm install @emanuelepifani/nexo-client
```


## 3. Provision Durable Resources

Queue and Stream are durable resources. Create them from a deployment script, init job, or administrative process before starting application instances.

::: code-group

```typescript
import { NexoClient } from '@emanuelepifani/nexo-client';

const client = await NexoClient.connect({ host: 'localhost', port: 7654 });

const queueResult = await client.queue.create('emails');
const streamResult = await client.stream.create('user-events');

console.log(queueResult.status, queueResult.definition.config);
console.log(streamResult.status, streamResult.definition.config);
```

```python
from nexo import NexoClient

client = await NexoClient.connect(host="localhost", port=7654)

queue_result = await client.queue.create("emails")
stream_result = await client.stream.create("user-events")

print(queue_result.status, queue_result.definition.config)
print(stream_result.status, stream_result.definition.config)
```

:::

## 4. Connect & Use

Application code retrieves durable resources with `get()`, which fails immediately if deployment has not provisioned them. Pub/Sub topics are routing addresses and do not require provisioning.

::: code-group

```typescript
const client = await NexoClient.connect({ host: 'localhost', port: 7654 });

await client.store.map.set('user:1', { name: 'Max', role: 'admin' });
const user = await client.store.map.get('user:1');

const alerts = client.pubsub.topic<Alert>('alerts');
await alerts.subscribe(async (message) => console.log(message));
await alerts.publish({ level: 'high' });

const mailQueue = await client.queue.get<Email>('emails');
await mailQueue.push({ to: 'test@test.com' });
await mailQueue.subscribe(async (message) => console.log(message));

const stream = await client.stream.get<UserEvent>('user-events');
await stream.publish({ type: 'login', userId: 'u1' });
await stream.group('analytics').subscribe(async (message) => console.log(message));
```

```python
from nexo import NexoClient, NexoQueue, NexoStream, NexoTopic

client = await NexoClient.connect(host="localhost", port=7654)

await client.store.map.set("user:1", {"name": "Max", "role": "admin"})
user: User | None = await client.store.map.get("user:1")

async def on_alert(message: Alert) -> None:
    print(message)

alerts: NexoTopic[Alert] = client.pubsub.topic("alerts")
await alerts.subscribe(on_alert)
await alerts.publish({"level": "high"})

async def handle_email(message: Email) -> None:
    print(message)

mail_queue: NexoQueue[Email] = await client.queue.get("emails")
await mail_queue.push({"to": "test@test.com"})
await mail_queue.subscribe(handle_email)

async def on_event(message: UserEvent) -> None:
    print(message)

stream: NexoStream[UserEvent] = await client.stream.get("user-events")
await stream.publish({"type": "login", "userId": "u1"})
await stream.group("analytics").subscribe(on_event)
```

:::

