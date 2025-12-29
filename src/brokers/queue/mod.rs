use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use parking_lot::Mutex;
use uuid::Uuid;
use bytes::Bytes;
use serde::{Serialize, Deserialize};
use tokio::sync::oneshot;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

mod queue_manager;
pub use queue_manager::QueueManager;

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

// ---------- QueueState ----------

pub struct QueueState {
    /// L'Archivio: contiene i dati reali di tutti i messaggi presenti nella coda
    pub registry: HashMap<Uuid, Message>,
    /// Messaggi pronti: stanno aspettando un consumatore (ordinati per Priorità)
    pub waiting_for_dispatch: BTreeMap<u8, VecDeque<Uuid>>,
    /// Messaggi programmati: stanno aspettando il momento giusto (ordinati per Tempo)
    pub waiting_for_time: BTreeMap<u64, Vec<Uuid>>,
    /// Messaggi inviati: stanno aspettando un ACK o il timeout (ordinati per Scadenza)
    pub waiting_for_ack: BTreeMap<u64, Vec<Uuid>>,
    /// Consumatori "parcheggiati": stanno aspettando che arrivi un messaggio
    pub waiting_consumers: VecDeque<oneshot::Sender<Message>>,
}

impl QueueState {
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
            waiting_for_dispatch: BTreeMap::new(),
            waiting_for_time: BTreeMap::new(),
            waiting_for_ack: BTreeMap::new(),
            waiting_consumers: VecDeque::new(),
        }
    }

    /// Estrae il prossimo ID pronto per la consegna rispettando le priorità
    pub fn next_available_message_id(&mut self) -> Option<Uuid> {
        for (_priority, bucket) in self.waiting_for_dispatch.iter_mut().rev() {
            if let Some(id) = bucket.pop_front() {
                return Some(id);
            }
        }
        None
    }

    /// Sposta un messaggio nello stato "In attesa di ACK" (Visibility Timeout)
    pub fn move_to_waiting_ack(&mut self, id: Uuid, now: u64) -> Option<Message> {
        let msg = self.registry.get_mut(&id)?;
        // TODO: Make visibility timeout configurable (currently 1s for tests)
        let timeout = now + 1000;
        msg.visible_at = timeout;
        msg.attempts += 1;
        
        let msg_cloned = msg.clone();
        self.waiting_for_ack.entry(timeout).or_default().push(id);
        Some(msg_cloned)
    }

    /// Estrae in modo efficiente gli ID scaduti da waiting_for_time o waiting_for_ack
    pub fn extract_expired_ids(index: &mut BTreeMap<u64, Vec<Uuid>>, now: u64) -> Vec<Uuid> {
        let ready_later = index.split_off(&(now + 1));
        let expired_map = std::mem::replace(index, ready_later);
        expired_map.into_values().flatten().collect()
    }
}

// ---------- Queue ----------

pub struct Queue {
    pub name: String,
    state: Mutex<QueueState>,
}

impl Queue {
    pub fn new(name: String) -> Self {
        Self {
            name,
            state: Mutex::new(QueueState::new()),
        }
    }

    /// Logica centrale di smistamento: consegna a un consumatore in attesa o mette in coda ready.
    fn dispatch(&self, state: &mut QueueState, id: Uuid, now: u64) {
        while let Some(consumer_tx) = state.waiting_consumers.pop_front() {
            if let Some(msg) = state.move_to_waiting_ack(id, now) {
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
        let msg = Message::new(payload, priority, delay_ms);
        let id = msg.id;
        let now = current_time_ms();

        state.registry.insert(id, msg.clone());

        if let Some(delay_ts) = msg.delayed_until {
            state.waiting_for_time.entry(delay_ts).or_default().push(id);
        } else {
            self.dispatch(&mut state, id, now);
        }
    }

    pub fn pop(&self) -> Option<Message> {
        let mut state = self.state.lock();
        let now = current_time_ms();
        let id = state.next_available_message_id()?;
        state.move_to_waiting_ack(id, now)
    }

    pub fn ack(&self, id: Uuid) -> bool {
        let mut state = self.state.lock();
        state.registry.remove(&id);
        true // Idempotenza
    }

    pub fn consume(&self) -> oneshot::Receiver<Message> {
        let mut state = self.state.lock();
        let (tx, rx) = oneshot::channel();
        let now = current_time_ms();

        if let Some(id) = state.next_available_message_id() {
            if let Some(msg) = state.move_to_waiting_ack(id, now) {
                let _ = tx.send(msg);
                return rx;
            }
        }
        
        state.waiting_consumers.push_back(tx);
        rx
    }

    pub fn start_reaper(self: Arc<Self>, manager: Arc<QueueManager>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                self.reprocess_expired_messages(&manager);
            }
        });
    }

    fn reprocess_expired_messages(&self, manager: &Arc<QueueManager>) {
        let mut state = self.state.lock();
        let now = current_time_ms();

        // 1. Messaggi programmati -> Pronti/Dispatch
        let expired_delayed = QueueState::extract_expired_ids(&mut state.waiting_for_time, now);
        for id in expired_delayed {
            self.dispatch(&mut state, id, now);
        }

        // 2. Messaggi in attesa di ACK -> DLQ o Pronti/Dispatch
        let expired_in_flight = QueueState::extract_expired_ids(&mut state.waiting_for_ack, now);
        for id in expired_in_flight {
            let (should_dlq, payload, priority) = match state.registry.get(&id) {
                Some(msg) if msg.attempts >= 5 => (true, msg.payload.clone(), msg.priority),
                Some(_) => (false, Bytes::new(), 0),
                None => continue, // Già confermato (ACK)
            };

            if should_dlq {
                state.registry.remove(&id);
                let dlq_name = format!("{}_dlq", self.name);
                let manager_clone = Arc::clone(manager);
                tokio::spawn(async move {
                    manager_clone.push(dlq_name, payload, priority, None);
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

