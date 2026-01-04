use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use uuid::Uuid;
use crate::brokers::queues::QueueManager;
// ---------- Message ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub payload: Bytes,
    pub priority: u8,
    pub attempts: u32,
    pub created_at: u64,
    pub visible_at: u64,
    pub delayed_until: Option<u64>,
}

impl Message {
    pub fn new(payload: Bytes, priority: u8, delay_ms: Option<u64>) -> Self {
        let now = current_time_ms();
        let delayed_until = delay_ms.map(|d| now + d);

        Self {
            id: Uuid::new_v4(),
            payload,
            priority,
            attempts: 0,
            created_at: now,
            visible_at: 0,
            delayed_until,
        }
    }
}

// ---------- QueueConfig ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub visibility_timeout_ms: u64,
    pub max_retries: u32,
    pub ttl_ms: u64,
    pub default_delay_ms: u64,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            visibility_timeout_ms: 30000,
            max_retries: 5,
            ttl_ms: 604800000, // 7 days in ms
            default_delay_ms: 0,
        }
    }
}

impl QueueConfig {
    pub fn merge_defaults(&mut self) {
        let def = Self::default();
        if self.visibility_timeout_ms == 0 {
            self.visibility_timeout_ms = def.visibility_timeout_ms;
        }
        if self.max_retries == 0 {
            self.max_retries = def.max_retries;
        }
        if self.ttl_ms == 0 {
            self.ttl_ms = def.ttl_ms;
        }
    }
}

// ---------- InternalState ----------

struct InternalState {
    /// L'Archivio: contiene i dati reali di tutti i messaggi presenti nella coda
    registry: HashMap<Uuid, Message>,
    /// Messaggi pronti: stanno aspettando un consumatore (ordinati per Priorità)
    waiting_for_dispatch: BTreeMap<u8, VecDeque<Uuid>>,
    /// Messaggi programmati: stanno aspettando il momento giusto (ordinati per Tempo)
    waiting_for_time: BTreeMap<u64, Vec<Uuid>>,
    /// Messaggi inviati: stanno aspettando un ACK o il timeout (ordinati per Scadenza)
    waiting_for_ack: BTreeMap<u64, Vec<Uuid>>, //BTreeMap<"expired_date"", Vec<"uuid_message_of_registry"">>
    /// Indice TTL: Ordina i messaggi per data di scadenza assoluta (O(1) cleanup)
    waiting_for_ttl: BTreeMap<u64, Vec<Uuid>>,
    /// Consumatori "parcheggiati": stanno aspettando che arrivi un messaggio
    waiting_consumers: VecDeque<oneshot::Sender<Message>>,
}

impl InternalState {
    fn new() -> Self {
        Self {
            registry: HashMap::new(),
            waiting_for_dispatch: BTreeMap::new(),
            waiting_for_time: BTreeMap::new(),
            waiting_for_ack: BTreeMap::new(),
            waiting_for_ttl: BTreeMap::new(),
            waiting_consumers: VecDeque::new(),
        }
    }
}

// ---------- Queue ----------

pub struct Queue {
    pub name: String,
    pub config: QueueConfig,
    state: Mutex<InternalState>,
}

impl Queue {
    pub fn new(name: String, config: QueueConfig) -> Self {
        Self {
            name,
            config,
            state: Mutex::new(InternalState::new()),
        }
    }

    /// Estrae il prossimo ID pronto per la consegna rispettando le priorità e il TTL
    fn next_available_message_id(&self, state: &mut InternalState, now: u64) -> Option<Uuid> {
        while let Some(id) = self.peek_next_id(state) {
            // Se il messaggio non esiste più o è scaduto (TTL), lo scartiamo
            let is_valid = if let Some(msg) = state.registry.get(&id) {
                let age = now.saturating_sub(msg.created_at);
                if age > self.config.ttl_ms {
                    state.registry.remove(&id);
                    false
                } else {
                    true
                }
            } else {
                false
            };

            if is_valid {
                return Some(id);
            }
            // L'ID non era valido (scaduto o rimosso), continuiamo il loop (Lazy Cleanup)
        }
        None
    }

    /// Semplice helper per fare pop dal bucket a priorità più alta
    fn peek_next_id(&self, state: &mut InternalState) -> Option<Uuid> {
        for (_priority, bucket) in state.waiting_for_dispatch.iter_mut().rev() {
            if let Some(id) = bucket.pop_front() {
                return Some(id);
            }
        }
        None
    }

    /// Sposta un messaggio nello stato "In attesa di ACK" (Visibility Timeout)
    fn move_to_waiting_ack(&self, state: &mut InternalState, id: Uuid, now: u64) -> Option<Message> {
        let msg = state.registry.get_mut(&id)?;
        let timeout = now + self.config.visibility_timeout_ms;
        msg.visible_at = timeout;
        msg.attempts += 1;

        let msg_cloned = msg.clone();
        state.waiting_for_ack.entry(timeout).or_default().push(id);
        Some(msg_cloned)
    }

    /// Estrae in modo efficiente gli ID scaduti da un indice temporale
    fn extract_expired_ids(&self, index: &mut BTreeMap<u64, Vec<Uuid>>, now: u64) -> Vec<Uuid> {
        let ready_later = index.split_off(&(now + 1));
        let expired_map = std::mem::replace(index, ready_later);
        expired_map.into_values().flatten().collect()
    }

    /// Logica centrale di smistamento: consegna a un consumatore in attesa o mette in coda ready.
    fn dispatch(&self, state: &mut InternalState, id: Uuid, now: u64) {
        // Verifica TTL prima del dispatch
        if let Some(msg) = state.registry.get(&id) {
            let age = now.saturating_sub(msg.created_at);
            if age > self.config.ttl_ms {
                state.registry.remove(&id);
                return;
            }
        } else {
            return;
        }

        while let Some(consumer_tx) = state.waiting_consumers.pop_front() {
            if let Some(msg) = self.move_to_waiting_ack(state, id, now) {
                if consumer_tx.send(msg).is_ok() {
                    return;
                }
                // Se il consumatore si è disconnesso, ripristiniamo lo stato
                if let Some(m) = state.registry.get_mut(&id) {
                    m.attempts -= 1;
                    m.visible_at = 0;
                }
            }
        }

        // Nessun consumatore pronto, lo mettiamo nel bucket della priorità corrispondente
        if let Some(msg) = state.registry.get(&id) {
            state.waiting_for_dispatch.entry(msg.priority).or_default().push_back(id);
        }
    }

    pub fn push(&self, payload: Bytes, priority: u8, delay_ms: Option<u64>) {
        let mut state = self.state.lock();
        let effective_delay = delay_ms.or(if self.config.default_delay_ms > 0 { Some(self.config.default_delay_ms) } else { None });
        let msg = Message::new(payload, priority, effective_delay);
        let id = msg.id;
        let now = current_time_ms();

        state.registry.insert(id, msg.clone());

        // Indice TTL: registriamo quando questo messaggio morirà
        let ttl_expiry = msg.created_at + self.config.ttl_ms;
        state.waiting_for_ttl.entry(ttl_expiry).or_default().push(id);

        if let Some(delay_ts) = msg.delayed_until {
            state.waiting_for_time.entry(delay_ts).or_default().push(id);
        } else {
            self.dispatch(&mut state, id, now);
        }
    }

    pub fn pop(&self) -> Option<Message> {
        let mut state = self.state.lock();
        let now = current_time_ms();
        let id = self.next_available_message_id(&mut state, now)?;
        self.move_to_waiting_ack(&mut state, id, now)
    }

    pub fn ack(&self, id: Uuid) -> bool {
        let mut state = self.state.lock();
        if let Some(msg) = state.registry.remove(&id) {
            // Pulizia indice TTL per evitare memory leak su TTL lunghi
            let ttl_expiry = msg.created_at + self.config.ttl_ms;
            if let Some(bucket) = state.waiting_for_ttl.get_mut(&ttl_expiry) {
                bucket.retain(|&x| x != id);
                if bucket.is_empty() {
                    state.waiting_for_ttl.remove(&ttl_expiry);
                }
            }
            true
        } else {
            true // Idempotenza
        }
    }

    pub fn consume(&self) -> oneshot::Receiver<Message> {
        let mut state = self.state.lock();
        let (tx, rx) = oneshot::channel();
        let now = current_time_ms();

        if let Some(id) = self.next_available_message_id(&mut state, now) {
            if let Some(msg) = self.move_to_waiting_ack(&mut state, id, now) {
                let _ = tx.send(msg);
                return rx;
            }
        }

        state.waiting_consumers.push_back(tx);
        rx
    }

    pub fn start_reaper(self: Arc<Self>, manager: Arc<QueueManager>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                self.reprocess_expired_messages(&manager);
            }
        });
    }

    fn reprocess_expired_messages(&self, manager: &Arc<QueueManager>) {
        let mut state = self.state.lock();
        let now = current_time_ms();

        // 0. Cleanup dei messaggi scaduti (TTL)
        let expired_ttl_ids = self.extract_expired_ids(&mut state.waiting_for_ttl, now);
        for id in expired_ttl_ids {
            state.registry.remove(&id);
            // Non serve pulire gli altri indici (dispatch/ack/time) subito.
            // La Lazy Cleanup in next_available_message_id e gli altri check gestiranno i riferimenti "morti".
        }

        // 1. Messaggi programmati -> Pronti/Dispatch
        let expired_delayed = self.extract_expired_ids(&mut state.waiting_for_time, now);
        for id in expired_delayed {
            if state.registry.contains_key(&id) {
                self.dispatch(&mut state, id, now);
            }
        }

        // 2. Messaggi in attesa di ACK -> DLQ o Pronti/Dispatch
        let expired_in_flight = self.extract_expired_ids(&mut state.waiting_for_ack, now);
        for id in expired_in_flight {
            let (should_dlq, payload, priority) = match state.registry.get(&id) {
                Some(msg) if msg.attempts >= self.config.max_retries => (true, msg.payload.clone(), msg.priority),
                Some(_) => (false, Bytes::new(), 0),
                None => continue, // Già confermato (ACK) o scaduto (TTL)
            };

            if should_dlq {
                state.registry.remove(&id);
                let dlq_name = format!("{}_dlq", self.name);
                let manager_clone = Arc::clone(manager);
                tokio::spawn(async move {
                    let _ = manager_clone.push_internal(dlq_name, payload, priority, None);
                });
            } else {
                self.dispatch(&mut state, id, now);
            }
        }
    }
}

pub fn current_time_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}