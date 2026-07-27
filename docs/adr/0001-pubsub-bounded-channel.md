# ADR-0001: PubSub Bounded Per-Subscriber Channel with Slow-Consumer Disconnect

## Stato

Proposto — in attesa di implementazione.

## Contesto

Il broker PubSub di Nexo è push-based con fan-out: un `publish()` invia un messaggio a tutti i subscriber di un topic. Attualmente ogni subscriber ha un canale intermedio `tokio::sync::mpsc::UnboundedSender` tra `PubSubManager` e il bridge task che scrive sul socket TCP outbound.

```text
publisher → PubSubManager.publish() ──► UnboundedSender ──► bridge task
                                                    │
                                                    ▼
                                          outbound_tx.send().await
                                                    │
                                                   TCP
```

Il bridge task, a sua volta, inserisce i messaggi nel bounded channel `outbound_tx` verso il TCP writer. Se il TCP writer è lento, il bridge si blocca su `outbound_tx.send().await`; di conseguenza smette di fare `recv()` sul canale unbounded, che continua a crescere in RAM finché il producer è più veloce del consumer. Questo può portare a OOM.

Questo è lo stesso problema risolto per il broker Queue con l'introduzione di un bounded channel per la persistenza.

## Problema

- Canale PubSub per subscriber è **unbounded**.
- Non c'è backpressure sul publisher.
- Non c'è limite di memoria per subscriber lenti.
- Un singolo slow consumer può far crescere la RAM del server fino a OOM.

## Vincoli

- `publish()` è sincrono (`fn`, non `async fn`). Non può fare `.await`.
- Il socket outbound è bounded e condiviso con tutti i tipi di traffico.
- Il modello è fan-out: un publisher a molti subscriber.
- Non vogliamo accoppiare i subscriber tra loro (head-of-line blocking).

## Soluzione proposta

Passare a un **bounded channel per subscriber** e **disconnettere il client** quando il buffer è pieno. Questo è lo stesso approccio usato da Redis Pub/Sub, NATS Core e MQTT (QoS 0 / bounded queue).

### Architettura target

```text
publisher → PubSubManager.publish() ──► [Sender, capacity N] ──► bridge task
                                                      │
                                              try_send(): Full?
                                                      │
                                          Sì → disconnect(client_id)
                                                      │
                                              drop(push_tx)
                                                      │
                                          bridge task termina
                                                      │
                                          connection.rs select! → break
                                                      │
                                          socket chiuso, cleanup
```

### Cambiamenti file per file

#### 1. `src/brokers/pub-sub/config.rs`

Aggiungere `push_channel_capacity` a `PubSubConfig`, con default e variabile d'ambiente.

```rust
pub struct PubSubConfig {
    pub persistence_path: String,
    pub retained_flush_ms: u64,
    pub cleanup_interval_seconds: u64,
    pub default_retained_ttl_seconds: u32,
    pub push_channel_capacity: usize,  // NUOVO
}
```

Default: `1024` (ogni subscriber può avere fino a 1024 messaggi in attesa).
Env: `PUBSUB_PUSH_CHANNEL_CAPACITY`.

#### 2. `src/brokers/pub-sub/domain/types.rs`

Cambiare `ClientInfo` da `UnboundedSender` a bounded `Sender`.

```diff
pub(crate) struct ClientInfo {
-    pub sender: mpsc::UnboundedSender<Arc<PubSubMessage>>,
+    pub sender: mpsc::Sender<Arc<PubSubMessage>>,
    pub subscriptions: HashSet<String>,
}
```

#### 3. `src/brokers/pub-sub/manager.rs`

- `connect()` accetta un `mpsc::Sender` invece di `mpsc::UnboundedSender`.
- `publish()` usa `try_send` e marca come zombie su `TrySendError::Full`.
- `subscribe()` gestisce `Full` anche per i retained messages.

```rust
pub fn connect(&self, client_id: &str, sender: mpsc::Sender<Arc<PubSubMessage>>) {
    // ...
}

pub fn publish(&self, ...) -> Result<usize, String> {
    // ...
    for client_id in matched {
        if let Some(info) = self.clients.get(client_id.as_ref()) {
            match info.sender.try_send(msg.clone()) {
                Ok(()) => sent_count += 1,
                Err(mpsc::error::TrySendError::Full(_)) => zombies.push(client_id),
                Err(mpsc::error::TrySendError::Closed(_)) => zombies.push(client_id),
            }
        } else {
            zombies.push(client_id);
        }
    }
    // ...
}
```

#### 4. `src/transport/tcp/connection.rs`

- Sostituire `mpsc::unbounded_channel()` con `mpsc::channel(server_config.pubsub_push_channel_capacity)`.
- Aggiungere `bridge_handle` al `tokio::select!` principale, così quando `push_tx` viene droppato la connessione termina e il socket TCP viene chiuso.

```rust
let (push_tx, mut push_rx) = mpsc::channel::<Arc<PubSubMessage>>(
    server_config.pubsub_push_channel_capacity,
);

// nel main loop
tokio::select! {
    Some(frame) = inbound_rx.recv() => { ... }
    socket_result = &mut socket_task => { ... }
    _ = request_set.join_next(), if !request_set.is_empty() => {}
    // NUOVO
    _ = &mut bridge_handle => {
        break;
    }
}
```

Quando `PubSubManager.disconnect()` droppa `push_tx`, il bridge esce e `connection.rs` interrompe il loop principale, facendo cleanup completo.

## Backpressure e disconnessione

- `publish()` rimane sincrono e **non si blocca**.
- Se il buffer di un subscriber è pieno, quel subscriber viene disconnesso.
- Gli altri subscriber non vengono rallentati.
- Il publisher può continuare a pubblicare.
- RAM limitata a `capacity × numero_subscriber`.

## Test deterministici

Aggiungere in `tests/pubsub_tests.rs` un test come:

```rust
#[tokio::test]
async fn slow_pubsub_subscriber_gets_disconnected() {
    let manager = PubSubManager::new(Arc::new(PubSubConfig::default()));

    // Crea subscriber con buffer piccolo
    let (tx, mut rx) = mpsc::channel(2);
    manager.connect("slow", tx);
    manager.subscribe("slow", "topic").unwrap();

    // Riempi il buffer + overflow
    for i in 0..4 {
        manager.publish("topic", Bytes::from(format!("{}", i)), false, false, None).unwrap();
    }

    // Il subscriber slow deve essere stato rimosso
    assert!(!manager.exists("slow"));
}
```

## Trade-off

| Vantaggio | Svantaggio |
|---|---|
| RAM limitata e stabile sotto carico | Subscriber lento perde messaggi in transito |
| Publisher mai bloccato | Cliente disconnesso, richiede riconnessione |
| Isolamento tra subscriber | Nessuna garanzia di delivery a slow consumer |
| Semantica allineata a Redis/NATS/MQTT | Richiede client con auto-reconnect |

## Alternative scartate

### Async `publish()` con `join_all`

Rendere `publish()` async e usare `sender.send(msg).await` per ogni subscriber in parallelo.

**Scartata** perché accoppia tutti i subscriber: un lento rallenta il publisher per tutti. Causa head-of-line blocking implicito e apre a DoS (un client lento su un topic popolare rallenta tutti i publisher su quel topic).

### Drop senza disconnect

Scartare i messaggi invece di disconnettere.

**Scartata** perché il client rimarrebbe connesso senza sapere di aver perso messaggi. La disconnessione è un segnale esplicito al client che deve rallentare o riconnettersi.

## Riferimenti

- Redis Pub/Sub: `client-output-buffer-limit pubsub <hard> <soft> <timeout>`
- NATS Core: bounded per-subscriber output buffer, slow consumer disconnect
- MQTT 3.1.1: `max_queued_messages`, slow consumer handling per client
- Nexo ADR precedente: bounded channel nel broker Queue per la persistenza
