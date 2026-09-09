# PubSub Broker — Deep Analysis & Bug Report

Data: 2026-09-09
Scope: `src/brokers/pub-sub/` (Rust) + `sdk/ts` + `sdk/py` (e2e) + `tests/pubsub_tests.rs`

## Metodo

Analisi end-to-end del broker PubSub: routing (radix tree), retained messages
(TTL + persistenza SQLite), slow-consumer disconnect, dispatch push, matcher
wildcard lato SDK (TS `PatternTrie` / Python `_PatternMatcher`), lifecycle
subscribe/unsubscribe/reconnect.

Due bug logici confermati con test di probe, fixati e coperti da regression test
(Rust + entrambe le SDK e2e). Il resto dell'analisi non ha evidenziato altri bug
logici: i matcher wildcard (`+`/`#`), la dedup per client_id, il cleanup dei
nodi vuoti, la persistenza/restore dei retained e la gestione slow-consumer
sono risultati corretti.

---

## Bug 1 — `clearRetained` consegna un messaggio vuoto spurio ai subscriber correnti

### Gravità
Medio (comportamento controintuitivo, side-effect visibile all'utente).

### Sintesi
`clearRetained` è un'operazione di metadati (cancella il retained cached), ma il
percorso `publish(..., clear=true)` consegna comunque il payload (vuoto) a tutti
i subscriber attivi sul topic. Il subscriber riceve un messaggio "fantasma"
vuoto (Buffer vuoto / `b""`).

### Root cause
`PubSubManager::publish` esegue la cancellazione del retained e poi,
**incondizionatamente**, fa `match_subscribers` + `try_send` verso i subscriber
attivi. Non c'è ramo di early-return per `clear`.

```rust
// src/brokers/pub-sub/manager.rs — prima del fix
if clear || retain {
    let retained = if clear { None } else { Some(...) };
    root.set_retained(&parts, retained);          // OK: cancella retained
    self.retained_dirty.store(true, ...);
}
// ↓ nessun guard su `clear`: consegna comunque
let mut matched = Vec::new();
root.match_subscribers(&parts, &mut matched);
... try_send(msg) ...
```

Il comando `clearRetained` invia `FLAG_PUBSUB_PUB_CLEAR` con payload
`Buffer.alloc(0)`; il server lo forwarda ai subscriber come push PubSub con
payload 0 byte. L'SDK decodifica `DataType.RAW` + 0 byte → Buffer vuoto.

### Conferma (probe)
```
PROBE clear delivery to current subscriber: true
PROBE spurious payload len = 0
```

### Fix
Early-return dopo la cancellazione del retained quando `clear=true`: il clear è
un'operazione di metadati, non un messaggio.

```rust
// src/brokers/pub-sub/manager.rs — dopo il fix
if clear || retain { ... root.set_retained(...); }

// `clear` is a retained-metadata operation, not a message: it must not
// deliver a (empty) payload to current subscribers.
if clear {
    return Ok(0);
}
// ... solo qui parte la consegna ai subscriber
```

### Documentazione
`docs/guide/pubsub.md`: "To clear a retained message, use `clearRetained()` …
A later subscriber on that topic will not receive a retained value." Non menziona
consegna ai subscriber correnti → il fix allinea il comportamento alla doc.

### Copertura test
- Rust: `test_clear_retained_does_not_deliver_to_current_subscribers`
  (asserts: nessun messaggio dopo clear + retained non re-distribuito)
- TS: `should not deliver a spurious message to current subscribers on clearRetained`
- Python: `test_clear_retained_does_not_deliver_to_current_subscribers`
- Matrix: `pubsub_clear_no_spurious_delivery`

### Note
- MQTT standard consegna l'empty-payload anche ai subscriber correnti, ma nexo
  ha `clear` come flag distinto da `retain`; la semantica attesa (e documentata)
  è "solo cancella il retained".
- Il test Rust preesistente `test_clear_retained_with_empty_payload` non
  verificava la consegna spuria (creava solo un nuovo subscriber); continua a
  passare.

---

## Bug 2 — `unsubscribe` con pattern non validato rimuove una subscription diversa

### Gravità
Medio (corruzione silenziosa dello stato di routing; richiede client raw/buggato,
le SDK ufficiali non lo triggerano perché validano lato client).

### Sintesi
`unsubscribe("a/#/b")` (pattern invalido: `#` non in ultima posizione) rimuove
silenziosamente la subscription valida `a/#` dal radix tree, causando perdita
silente di messaggi per quel subscriber.

### Root cause
Due problemi congiunti:

1. `PubSubManager::unsubscribe` **non valida** il pattern (a differenza di
   `subscribe`) e chiama `remove_subscriber` **incondizionatamente**, anche se
   il pattern non era mai stato sottoscritto.

2. `Node::remove_subscriber` per il ramo `#` **ignora il resto del path** (tail):
   rimuove il client dall'`hash_child` del nodo corrente e ritorna, senza
   scendere nei segmenti successivi.

```rust
// src/brokers/pub-sub/domain/radix_tree.rs
match head.as_str() {
    "#" => {
        if let Some(mut hash_node) = self.hash_child.take() {
            hash_node.subscribers.remove(client);   // rimuove da a/#
            if !hash_node.is_empty() { self.hash_child = Some(hash_node); }
        }
        // ← tail (["b"]) ignorato
    }
    ...
}
```

Combinazione: `unsubscribe("a/#/b")` → `info.subscriptions.remove` è no-op (il
pattern non era registrato), ma `remove_subscriber(["a","#","b"])` rimuove il
client dal nodo `#` di `a` (cioè dalla subscription `a/#`). Risultato: il tree e
`info.subscriptions` diventano inconsistenti — `info.subscriptions` contiene
ancora `a/#` ma il tree non ha più il subscriber → messaggi persi silenziosamente.

### Conferma (probe)
```
PROBE unsubscribe-invalid publish count: Ok(0)
PROBE unsubscribe-invalid received after bogus unsub: false
```
Dopo `unsubscribe("probe/#/b")`, la subscription `probe/#` non riceve più nulla.

### Fix
`unsubscribe` tocca il routing tree solo se il pattern era effettivamente
sottoscritto dal client (`info.subscriptions.remove` ritorna `true`). Questo
rende `unsubscribe` idempotente e immune a pattern non validati/non sottoscritti,
preservando le subscription esistenti.

```rust
// src/brokers/pub-sub/manager.rs — dopo il fix
pub fn unsubscribe(&self, client_id: &str, pattern: &str) {
    let Some(mut info) = self.clients.get_mut(client_id) else { return; };
    let was_subscribed = info.subscriptions.remove(pattern);
    drop(info);

    // Only touch the routing tree for a pattern this client actually held.
    // Without this guard, an unsubscribe for a never-subscribed (or invalid)
    // pattern could silently remove a *different* subscription: e.g.
    // unsubscribing "a/#/b" strips the client from the "a/#" hash node because
    // remove_subscriber stops descending after "#".
    if !was_subscribed {
        return;
    }

    let parts: Vec<String> = pattern.split('/').map(|s| s.to_string()).collect();
    let mut root = self.tree.write();
    root.remove_subscriber(&parts, client_id);
}
```

### Copertura test
- Rust: `test_unsubscribe_invalid_pattern_does_not_remove_valid_subscription`
  (subscribe `a/#`, unsubscribe `a/#/b`, publish `a/x` → deve ancora arrivare)
- Rust: `test_unsubscribe_never_subscribed_is_noop`
  (unsubscribe di pattern mai sottoscritto non rompe subscription correlate)

### Note
- Le SDK ufficiali inviano sempre UNSUB per pattern effettivamente sottoscritti,
  quindi non triggerano il bug; è comunque un difetto di robustezza lato server
  (client raw/buggato).
- Il fix è minimale e non cambia la signature pubblica (`unsubscribe` resta
  infallible, ritorna `()`). Non è stata aggiunta validazione esplicita del
  pattern perché il guard `was_subscribed` è più robusto (copre anche pattern
  validi mai sottoscritti).

---

## Aree verificate (nessun bug trovato)

- **Matcher wildcard Rust** (`match_subscribers`): `#` matcha anche il livello
  padre (semantica MQTT), `+` matcha esattamente un livello, dedup per client_id
  previene consegne duplicate da pattern sovrapposti. ✔
- **Retained delivery on subscribe** (`collect_retained_for_pattern`):
  ricostruzione path corretta per `+`, `#`, letterali; expired non consegnati. ✔
- **TTL retained**: `is_expired` coerente tra `Instant` e fallback `unix`;
  `from_persisted` ricalcola correttamente il remaining; `load_all` filtra gli
  expired. ✔ (nota: granularità al secondo su restore — minor, non bug)
- **Persistenza/restore**: flush batch (DELETE+INSERT in tx), shutdown flush
  sincrono, restore dopo restart. ✔
- **Slow-consumer disconnect**: `try_send` non blocca, zombie disconnessi,
  cleanup subscription dal tree. ✔
- **Lock ordering**: `clients` (DashMap) e `tree` (RwLock) sempre acquisiti in
  ordine consistente (clients → tree), senza overlap → nessun deadlock. ✔
- **Matcher SDK TS** (`PatternTrie`) e **Python** (`_PatternMatcher`): match
  `+`/`#` (incluso `#` matcha zero livelli e `a/+/b/#`), pruning nodi vuoti su
  remove, dispatch exact+wildcard. ✔
- **Reconnect/resubscribe** entrambe le SDK. ✔

### Edge case notato (non bloccante, non fixato)
- `ttl=0` su retained → `expires_at = now` → immediatamente expired → retained
  mai consegnato e cleanup rimosso. Le SDK accettano `ttl=0` (validano solo
  `< 0`). Silenzioso e controintuitivo, ma marginale; non trattato in questo
  passaggio.
