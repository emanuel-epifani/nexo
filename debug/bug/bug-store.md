# Store — Bug nelle logiche end-to-end

> **Stato:** analisi e documentazione; nessuna correzione applicata.
> **Ambito:** broker store (KV map): `set`, `get`, `del`, `incr`, `clear_all`, `clear_with_prefix`, TTL.
> **Priorità:** un problema P1 di disponibilità (panic/DoS), un problema P2 di correttezza semantica, due problemi P3 minori.

Ogni bug è contenuto in una sezione inizialmente chiusa. Nei renderer che supportano l'attributo HTML `name` di `<details>`, aprire una sezione chiude automaticamente quella precedente. Negli altri renderer le sezioni possono essere aperte e chiuse individualmente.

## Come leggere le evidenze

- La suite Rust esistente è stata esaminata staticamente (`tests/store_tests.rs`, 339 righe, 20 test) e confrontata con l'implementazione (`src/brokers/store/domain/map.rs`, `src/brokers/store/tcp.rs`).
- Il panic da TTL eccessivo (STORE-01) è **riprodotto ed eseguito** con un programma Rust standalone su questa macchina (macOS Darwin 25.6.0): `Instant::now() + Duration::from_secs(u64::MAX)` panica con `"overflow when adding duration to instant"`.
- I bug STORE-02 e STORE-03 sono dedotti dall'analisi del codice e dalla correlazione tra `get` (scadenza controllata in lettura), il task di cleanup (intervallo default 60 s) e `clear_all`/`clear_with_prefix`/`del` (che operano sulla mappa grezza senza filtrare le entry scadute).
- Gli SDK TypeScript e Python sono stati verificati leggendo il codice (`sdk/ts/src/brokers/store.ts`, `sdk/py/src/nexo/brokers/store.py`) e il relativo strato di connection (`connection.ts`, `connection.py`).
- Le proposte di correzione e i test di regressione elencati nelle sezioni sono lavoro futuro, non modifiche già implementate.

### Termini essenziali

| Termine | Significato in questa analisi |
| --- | --- |
| Entry scaduta | Entry ancora presente nella `DashMap` il cui `expires_at` è nel passato, ma non ancora rimossa dal task di cleanup periodico. |
| `get` | Controlla `Instant::now() >= expires_at` e restituisce `None` se la entry è scaduta, **senza rimuoverla** dalla mappa. |
| Cleanup task | Task tokio spawnato in `Map::new` che esegue `retain` ogni `cleanup_interval_secs` (default 60 s) ed elimina fisicamente le entry scadute. |
| `clear_all` / `clear_with_prefix` | Operazioni di cancellazione che restituiscono il conteggio delle entry rimosse. |

**Distinzione importante:** un'entry scaduta è ancora fisicamente nella `DashMap` finché il cleanup task non la rimuove. `get` la tratta come inesistente, ma `clear_all`, `clear_with_prefix` e `del` la trattano come esistente. Questa inconsistenza è la causa di STORE-02 e STORE-03.

<details name="store-bugs">
<summary><strong>STORE-01 · P1 — Panic del server via TTL eccessivo: `Instant + Duration` overflow</strong></summary>

### Comportamento atteso

Un client che invoca `set(key, value, ttl)` con un TTL grande (ma valido come `u64`) deve ricevere un errore protocollare, non causare il panic del thread che gestisce la connessione.

### Scenario che attiva il bug

Un client (legittimo o malevolo) invia `OP_MAP_SET` con `FLAG_STORE_MAP_SET_HAS_TTL` e un valore di TTL sufficiently grande da far overfloware la somma `Instant::now() + Duration::from_secs(ttl)`.

La soglia esatta dipende dalla piattaforma e dall'istante corrente (vedi riproduzione), ma qualsiasi TTL nell'ordine di ~10^18 secondi o superiore provoca il panic. `u64::MAX` (≈ 5,8 × 10^11 anni) è ben oltre la soglia.

### Riproduzione minima

Programma standalone eseguito su macOS Darwin 25.6.0:

```rust
use std::time::{Duration, Instant};

fn main() {
    let ttl = u64::MAX;
    let result = std::panic::catch_unwind(|| {
        Instant::now() + Duration::from_secs(ttl)
    });
    println!("ttl=u64::MAX => {:?}", result.is_err());
}
```

```text
thread 'main' panicked at library/std/src/time.rs:430:33:
overflow when adding duration to instant
ttl=u64::MAX => true
```

Il panic occorre perché `impl Add<Duration> for Instant` chiama `checked_add` e fa `.expect("overflow when adding duration to instant")`.

Soglia misurata su questa macchina: panic a partire da `ttl ≈ 17_179_869_184_000_000_000` (~5,4 × 10^11 anni). Valori inferiori non overflowano.

### Causa nel server e propagazione negli SDK

1. Il client (TS o Python) invia `OP_MAP_SET` con `FLAG_STORE_MAP_SET_HAS_TTL` e `ttl` come `u64` BE.
2. `MapCmd::parse` legge `ttl` come `u64` senza validazione di range superiore.
3. `Map::set` esegue `Instant::now() + Duration::from_secs(secs)` senza `checked_add`.
4. L'overflow causa `panic!` nel thread della connessione TCP.
5. Il thread panic; la connessione viene chiusa; il client riceve un disconnect, non un errore protocollare.

Nessuno degli SDK valida un limite superiore del TTL:
- TypeScript (`store.ts:10`): `w.u64(options!.ttl!)` — controlla solo `0 ≤ v ≤ 2^64-1`.
- Python (`store.py:29`): `w.u64(ttl)` — controlla solo `0 ≤ v ≤ 2^64-1`.

### Impatto

- **DoS remoto**: un singolo comando `set` con TTL grande causa il panic del thread di connessione. Se il server gestisce ogni connessione in un task tokio separato, il panic di un task può (a seconda della configurazione di `tokio::spawn` e della gestione dei panic) propagarsi o lasciare la connessione muta.
- **Nessun rate limiting o validazione** protegge da questo vettore: il TTL è un `u64` non validato dal protocollo.
- Il bug è raggiungibile da qualsiasi client connesso, senza autenticazione aggiuntiva oltre alla connessione TCP.

### Perché i test vicini non bastano

- `test_ttl_zero_is_error` verifica solo `ttl = 0` (caso limite inferiore).
- `test_ttl_expiration` usa `ttl = 1`.
- Nessun test usa TTL superiori a `60`.
- Il fuzz test non è presente per il parsing del payload `OP_MAP_SET`.

### Direzione della correzione e regressioni necessarie

- Sostituire `Instant::now() + Duration::from_secs(secs)` con `Instant::now().checked_add(Duration::from_secs(secs))`, restituendo `Err("ttl too large")` su `None`.
- In alternativa (o in aggiunta), definire un limite superiore nel `protocol.json` (es. `STORE_MAX_TTL_SECS`) e validarlo in `Map::set` prima dell'addizione.
- **Rust**: test `set` con `ttl = u64::MAX` → atteso `Err`, non panic. Test `set` con `ttl` al limite (`i64::MAX as u64` o soglia definita) → `Ok`.
- **TS + Python**: test e2e `set(key, val, { ttl: Number.MAX_SAFE_INTEGER })` o valore grande → atteso errore gestito, non disconnect.

### Riferimenti

- [`src/brokers/store/domain/map.rs:57`](../../src/brokers/store/domain/map.rs#L57) — `Instant::now() + Duration::from_secs(secs)` senza `checked_add`.
- [`src/brokers/store/tcp.rs:58-62`](../../src/brokers/store/tcp.rs#L58) — parsing del TTL senza validazione di range.
- [`sdk/ts/src/brokers/store.ts:10`](../../sdk/ts/src/brokers/store.ts#L10) — `w.u64(options!.ttl!)` senza limite superiore.
- [`sdk/py/src/nexo/brokers/store.py:29`](../../sdk/py/src/nexo/brokers/store.py#L29) — `w.u64(ttl)` senza limite superiore.
- [`src/brokers/store/config.rs:9`](../../src/brokers/store/config.rs#L9) — `cleanup_interval_secs` default 60 (non correlato al bug, ma contesto).

</details>

<details name="store-bugs">
<summary><strong>STORE-02 · P2 — `clear_all` / `clear_with_prefix` conteggiano entry scadute non ancora rimosse</strong></summary>

### Comportamento atteso

Il conteggio restituito da `clear_all()` e `clear_with_prefix()` deve riflettere il numero di chiavi **vive** effettivamente rimosse. Una chiave scaduta (TTL elapsed) che `get` considera inesistente non deve essere conteggiata.

### Scenario che attiva il bug

Il cleanup task ha un intervallo di default di 60 secondi. Nella finestra tra la scadenza di una chiave e la prossima esecuzione del cleanup, la entry è ancora fisicamente nella `DashMap`:

- `get(key)` → `None` (scadenza controllata in lettura).
- `clear_all()` → conteggia la entry scaduta nel totale restituito.
- `clear_with_prefix(prefix)` → conteggia la entry scaduta se la chiave corrisponde al prefisso.

### Riproduzione minima (logica)

1. `set("k1", "v1", Some(1))` — TTL 1 secondo.
2. `set("k2", "v2", Some(1))` — TTL 1 secondo.
3. Attendere 1,1 s: entrambe le chiavi sono scadute.
4. `get("k1")` → `None`, `get("k2")` → `None` (corretto).
5. `clear_all()` → restituisce `2` (inatteso: le chiavi sono scadute, `get` le considera inesistenti).

```text
get("k1") dopo scadenza: None
get("k2") dopo scadenza: None
clear_all() atteso:        0  (o almeno non 2)
clear_all() ottenuto:      2
```

### Causa interna

```rust
// clear_all — conta TUTTE le entry, incluse quelle scadute
pub fn clear_all(&self) -> usize {
    let count = self.inner.len();   // <-- include entry scadute
    self.inner.clear();
    count
}

// clear_with_prefix — conta TUTTE le entry corrispondenti, incluse quelle scadute
pub fn clear_with_prefix(&self, prefix: &str) -> usize {
    let mut count = 0;
    self.inner.retain(|k, _| {
        if k.starts_with(prefix) {
            count += 1;              // <-- include entry scadute
            false
        } else {
            true
        }
    });
    count
}
```

`get` filtra le entry scadute (`Instant::now() >= expiry` → `None`), ma né `clear_all` né `clear_with_prefix` applicano lo stesso filtro prima di contare. Il cleanup task rimuove fisicamente le entry scadute solo ogni `cleanup_interval_secs` (default 60 s), quindi la finestra di inconsistenza può durare fino a 60 secondi.

### Impatto

- Il conteggio restituito ai client è incoerente con lo stato osservabile via `get`: un client che imposta 3 chiavi con TTL, le lascia scadere, poi chiama `clear_all` riceve `3` anche se `get` restituisce `None` per tutte.
- In un sistema di monitoraggio che usa `clear_all` per misurare le chiavi eliminate, il numero è gonfiato da entry già morte.
- Il problema è comune a entrambi gli SDK, poiché entrambi restituiscono direttamente il conteggio dal server.

### Perché i test vicini non bastano

- `test_clear_all_removes_everything` imposta chiavi senza TTL: nessuna entry scaduta.
- `test_clear_with_prefix_removes_only_matching` idem.
- Nessun test combina TTL + scadenza + `clear_all`/`clear_with_prefix`.

### Direzione della correzione e regressioni necessarie

- Filtrare le entry scadute durante il conteggio in `clear_all` e `clear_with_prefix`, coerentemente con `get`.
- In alternativa (complementare), fare in modo che `get` rimuova eager l'entry scaduta (`remove_if`), riducendo la finestra in cui le entry scadute gonfiano i conteggi. Questo non elimina completamente il problema (entry mai lette restano fino al cleanup), quindi il filtro nel conteggio rimane necessario.
- **Rust**: test `clear_all` dopo scadenza di tutte le chiavi → atteso `0`. Test `clear_with_prefix` dopo scadenza → atteso `0`.
- **TS + Python**: test e2e con TTL=1s, attesa scadenza, `clearAll` → atteso `0` (o almeno `≥ 0` e non il numero di chiavi scadute).

### Riferimenti

- [`src/brokers/store/domain/map.rs:80-84`](../../src/brokers/store/domain/map.rs#L80) — `clear_all` usa `self.inner.len()` senza filtrare.
- [`src/brokers/store/domain/map.rs:86-97`](../../src/brokers/store/domain/map.rs#L86) — `clear_with_prefix` conta senza filtrare.
- [`src/brokers/store/domain/map.rs:64-74`](../../src/brokers/store/domain/map.rs#L64) — `get` filtra le entry scadute.
- [`src/brokers/store/domain/map.rs:28-48`](../../src/brokers/store/domain/map.rs#L28) — cleanup task con intervallo `cleanup_interval_secs`.
- [`src/brokers/store/config.rs:9`](../../src/brokers/store/config.rs#L9) — default `cleanup_interval_secs = 60`.

</details>

<details name="store-bugs">
<summary><strong>STORE-03 · P3 — `del` restituisce `true` per chiavi scadute (inconsistenza API Rust)</strong></summary>

### Comportamento atteso

`del(key)` di una chiave scaduta (che `get` considera inesistente) deve restituire `false`, coerentemente con la semantica "la chiave non esiste".

### Scenario che attiva il bug

Una chiave con TTL è scaduta ma non ancora rimossa dal cleanup task. `del(key)` trova la entry nella `DashMap` e la rimuove, restituendo `true`.

### Riproduzione minima (logica)

1. `set("k", "v", Some(1))` — TTL 1 secondo.
2. Attendere 1,1 s: chiave scaduta.
3. `get("k")` → `None` (corretto).
4. `del("k")` → restituisce `true` (inatteso).

```text
get("k") dopo scadenza: None
del("k") atteso:        false
del("k") ottenuto:      true
```

### Causa interna

```rust
pub fn del(&self, key: &str) -> bool {
    self.inner.remove(key).is_some()   // <-- non controlla expires_at
}
```

`del` rimuove la entry dalla mappa senza verificare se è scaduta. A differenza di `get`, che controlla `expires_at`, `del` tratta ogni entry presente come esistente.

### Impatto

- **Non visibile ai client**: il handler TCP (`tcp.rs:126-129`) ignora il valore di ritorno di `del` e restituisce sempre `Response::Ok`. Il client non può distinguere "chiave esistente e rimossa" da "chiave inesistente".
- **Inconsistenza nell'API Rust**: chi usa `StoreManager` direttamente (test, integrazioni future) ottiene un risultato incoerente tra `get` e `del`.
- Contribuisce alla stessa classe di problema di STORE-02: le entry scadute sono trattate in modo incoerente dalle diverse operazioni.

### Perché i test vicini non bastano

- `test_basic_crud` verifica `del` su chiave esistente (non scaduta) e su chiave inesistente (mai creata).
- Nessun test verifica `del` su chiave scaduta ma non ancora cleanup-ata.

### Direzione della correzione e regressioni necessarie

- Opzione A: controllare `expires_at` in `del` e restituire `false` se la entry è scaduta (rimuovendola comunque per pulizia).
- Opzione B: se il handler TCP ignora il valore di ritorno, documentare che `del` è "best-effort" e non garantisce coerenza con `get`. Meno consigliato.
- **Rust**: test `del` su chiave scaduta → atteso `false`.

### Riferimenti

- [`src/brokers/store/domain/map.rs:76-78`](../../src/brokers/store/domain/map.rs#L76) — `del` senza controllo `expires_at`.
- [`src/brokers/store/tcp.rs:126-129`](../../src/brokers/store/tcp.rs#L126) — handler TCP che ignora il valore di ritorno.

</details>

<details name="store-bugs">
<summary><strong>STORE-04 · P3 — Codice morto nei percorsi di errore degli SDK (incr / clearAll / clearWithPrefix)</strong></summary>

### Comportamento atteso

I percorsi di errore nel codice SDK dovrebbero essere raggiungibili o rimossi. Un percorso che non può mai essere eseguito ma che produrrebbe un risultato errato se lo fosse è un difetto di manutenibilità.

### Scenario che attiva il bug

Gli strati di connection (TS e Python) intercettano `ResponseStatus.ERR` e **reiettano** la promise/future prima che la risposta raggiunga il codice del broker SDK. Pertanto, i rami `throw`/`raise` in `mapIncr`, `mapClearAll` e `mapClearPrefix` non sono mai eseguiti.

### Evidenza nel codice

**TypeScript** — `connection.ts:233-241`:
```typescript
if (res.status === ResponseStatus.ERR) {
    const error = decodeErrorPayload(res.data);
    // ...
    reject(error);
    return;   // <-- ERR non arriva mai al broker SDK
}
resolve({ status: res.status, cursor: new Cursor(res.data) });
```

**TypeScript** — `store.ts:24-30` (`mapIncr`):
```typescript
const res = await conn.send(StoreOpcode.MAP_INCR, w => w.string(key).i64(delta));
if (res.status === ResponseStatus.DATA) {
    return res.cursor.decodeAny() as number;
}
throw new Error(res.cursor.readString());   // <-- morto: ERR già reiettato, OK/NULL non restituiscono mai stringa valida
```

Il server restituisce sempre `DATA` (successo) o `ERR` (errore) per `incr`, `clear_all` e `clear_prefix`. Mai `OK` o `NULL`. Quindi `res.status` è sempre `DATA` quando il codice raggiunge questo punto. Il ramo `throw` è irraggiungibile.

Se mai fosse raggiunto (es. per un bug futuro nel server che restituisce `OK` con payload vuoto), `res.cursor.readString()` leggerebbe 4 byte di lunghezza da un buffer vuoto → `RangeError [ERR_OUT_OF_RANGE]`, non un messaggio di errore significativo.

**Python** — `connection.py:263-273`:
```python
if status == ResponseStatus.ERR:
    error = _decode_server_error(data)
    # ...
    raise error   # <-- ERR non arriva mai al broker SDK
return status, Cursor(data)
```

**Python** — `store.py:45-51` (`incr`):
```python
status, cursor = await self._conn.send(...)
if status == ResponseStatus.DATA:
    return cursor.decode_any()
raise Exception(cursor.read_string())   # <-- morto, stesso motivo
```

Stessa situazione per `clear_all` e `clear_with_prefix` in entrambi gli SDK.

### Impatto

- **Nessun impatto runtime**: il codice è irraggiungibile nel flusso normale.
- **Manutenibilità**: un futuro cambiamento del protocollo che introducesse `OK` o `NULL` per queste operazioni attiverebbe un percorso che produce un errore fuorviante (lettura da cursor vuoto) invece di un messaggio utile.
- **Falsa sensazione di gestione errori**: chi legge il codice SDK può credere che gli errori del server siano gestiti con `readString()`, mentre in realtà sono gestiti (correttamente) nello strato connection.

### Perché i test vicini non bastano

- I test `should error on non-integer value` (TS) e `test_incr_non_integer_errors` (Python) verificano che `incr` su valore non intero sollevi un'eccezione. L'eccezione proviene dallo strato connection (`decodeErrorPayload` / `_decode_server_error`), non dal ramo `throw`/`raise` del broker SDK. Il test non distingue le due sorgenti.

### Direzione della correzione e regressioni necessarie

- Rimuovere i rami `throw`/`raise` morti in `mapIncr`, `mapClearAll`, `mapClearPrefix` (TS e Python), oppure sostituirli con un fallback esplicito che lancia un errore di protocollo (`Unexpected response status`).
- In alternativa, se si vuole mantenere un fallback difensivo, usare un messaggio fisso (`new Error('unexpected response status')`) invece di `readString()` da un cursor potenzialmente vuoto.
- Nessun test di regressione necessario per la rimozione di codice morto; verificare che i test esistenti continuino a passare.

### Riferimenti

- [`sdk/ts/src/transport/tcp/connection.ts:233`](../../sdk/ts/src/transport/tcp/connection.ts#L233) — reiezione di `ERR` nello strato connection TS.
- [`sdk/ts/src/brokers/store.ts:29`](../../sdk/ts/src/brokers/store.ts#L29) — ramo `throw` morto in `mapIncr`.
- [`sdk/ts/src/brokers/store.ts:37`](../../sdk/ts/src/brokers/store.ts#L37) — ramo `throw` morto in `mapClearAll`.
- [`sdk/ts/src/brokers/store.ts:45`](../../sdk/ts/src/brokers/store.ts#L45) — ramo `throw` morto in `mapClearPrefix`.
- [`sdk/py/src/nexo/transport/tcp/connection.py:263`](../../sdk/py/src/nexo/transport/tcp/connection.py#L263) — reiezione di `ERR` nello strato connection Python.
- [`sdk/py/src/nexo/brokers/store.py:51`](../../sdk/py/src/nexo/brokers/store.py#L51) — ramo `raise` morto in `incr`.
- [`sdk/py/src/nexo/brokers/store.py:59`](../../sdk/py/src/nexo/brokers/store.py#L59) — ramo `raise` morto in `clear_all`.
- [`sdk/py/src/nexo/brokers/store.py:67`](../../sdk/py/src/nexo/brokers/store.py#L67) — ramo `raise` morto in `clear_with_prefix`.

</details>

## Osservazione progettuale (non bug)

<details name="store-bugs">
<summary><strong>STORE-N1 — `get` non rimuove eager le entry scadute; `incr` su entry scaduta azzera il TTL</strong></summary>

### `get` lazy expiration

`get` controlla `expires_at` e restituisce `None` se la entry è scaduta, ma **non rimuove** la entry dalla `DashMap`. La rimozione fisica avviene solo nel cleanup task periodico (default 60 s). Questo significa che le entry scadute occupano memoria per fino a 60 secondi dopo la scadenza.

Questo è un scelta progettuale (lazy expiration + background sweep), coerente con l'uso di `DashMap::retain` nel cleanup. Non è un bug, ma è la causa strutturale di STORE-02 e STORE-03: finché le entry scadute restano nella mappa, ogni operazione che non filtra `expires_at` (come `clear_all`, `clear_with_prefix`, `del`) le tratta come esistenti.

Un'alternativa sarebbe l'eager removal in `get` (`self.inner.remove_if(key, |_, e| e.expires_at.map_or(false, |exp| Instant::now() >= exp))`), che ridurrebbe la finestra di inconsistenza ma non l'eliminerebbe (entry mai lette restano fino al cleanup).

### `incr` su entry scaduta azzera il TTL

Quando `incr` incontra una entry scaduta (`Occupied` con `expires_at` nel passato), crea una nuova entry con `expires_at: None` (persistente):

```rust
if Instant::now() >= expiry {
    let new_val = delta;
    o.insert(Entry {
        value: encode_int(new_val),
        expires_at: None,   // <-- TTL azzerato, chiave diventa persistente
    });
    return Ok(encode_int(new_val));
}
```

Questo è **compatibile con Redis** (`INCR` su chiave scaduta/non esistente crea una nuova chiave persistente). Non è un bug, ma il test `test_incr_on_expired_key_starts_from_zero` verifica solo il valore risultante, non che il TTL sia stato azzerato. Se in futuro si decidesse di preservare il TTL originale (divergendo da Redis), il test non coglierebbe il cambiamento.

### Riferimenti

- [`src/brokers/store/domain/map.rs:64-74`](../../src/brokers/store/domain/map.rs#L64) — `get` con lazy expiration.
- [`src/brokers/store/domain/map.rs:118-128`](../../src/brokers/store/domain/map.rs#L118) — `incr` su entry scaduta con `expires_at: None`.
- [`tests/store_tests.rs:212-227`](../../tests/store_tests.rs#L212) — test che verifica solo il valore, non il TTL.

</details>
