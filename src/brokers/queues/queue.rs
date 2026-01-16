use std::collections::{BTreeMap, HashMap, VecDeque, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Notify};
use uuid::Uuid;
use crate::brokers::queues::QueueManager;
use crate::dashboard::models::queues::{QueueSummary, MessageSummary};
use chrono::{DateTime, Utc};
use hashlink::LinkedHashSet;

// ---------- MessageState ----------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MessageState {
    Ready,                  // In waiting_for_dispatch
    Scheduled(u64),         // In waiting_for_time (timestamp)
    InFlight(u64),          // In waiting_for_ack (timestamp scadenza)
}

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
    pub state: MessageState,
}

impl Message {
    pub fn new(payload: Bytes, priority: u8, delay_ms: Option<u64>) -> Self {
        let now = current_time_ms();
        
        let state = if let Some(delay) = delay_ms {
            MessageState::Scheduled(now + delay)
        } else {
            MessageState::Ready
        };

        Self {
            id: Uuid::new_v4(),
            payload,
            priority,
            attempts: 0,
            created_at: now,
            visible_at: 0,
            delayed_until: delay_ms.map(|d| now + d),
            state,
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
    /// L'Archivio: contiene i dati reali e lo STATO di tutti i messaggi
    registry: HashMap<Uuid, Message>,
    /// Messaggi pronti: Ordinata per Priorità -> Per servire prima i VIP
    /// LinkedHashSet x garantire FIFO dentro stessa priorità + O(1) removal
    waiting_for_dispatch: BTreeMap<u8, LinkedHashSet<Uuid>>,
    /// Messaggi programmati: Ordinata per Tempo Start -> Per attivare i messaggi al momento giusto
    waiting_for_time: BTreeMap<u64, LinkedHashSet<Uuid>>,
    /// Messaggi inviati: Ordinata per Tempo Scadenza -> Per pulire timout o rimuovere gli acked dai consumer
    /// LinkedHashSet x garantire O(1) removal after ack
    waiting_for_ack: BTreeMap<u64, LinkedHashSet<Uuid>>,
    /// Consumatori batch in attesa
    batch_waiters: VecDeque<Arc<Notify>>,
}

impl InternalState {
    fn new() -> Self {
        Self {
            registry: HashMap::new(),
            waiting_for_dispatch: BTreeMap::new(),
            waiting_for_time: BTreeMap::new(),
            waiting_for_ack: BTreeMap::new(),
            batch_waiters: VecDeque::new(),
        }
    }

    /// Il cuore del sistema: sposta un messaggio da uno stato all'altro in modo atomico e pulito.
    /// Ritorna true se la transizione è avvenuta, false se il messaggio non esiste.
    fn transition_to(&mut self, id: Uuid, new_state: MessageState) -> bool {
        // 1. Recupera il messaggio (se non esiste, esci)
        let msg = match self.registry.get_mut(&id) {
            Some(m) => m,
            None => return false,
        };

        let old_state = msg.state.clone();
        
        // Se lo stato non cambia, non fare nulla (idempotenza)
        if old_state == new_state {
            return true;
        }

        // 2. FASE DI PULIZIA (Remove from Old Index)
        match old_state {
            MessageState::Ready => {
                if let Some(queue) = self.waiting_for_dispatch.get_mut(&msg.priority) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_dispatch.remove(&msg.priority);
                    }
                }
            },
            MessageState::Scheduled(ts) => {
                if let Some(queue) = self.waiting_for_time.get_mut(&ts) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_time.remove(&ts);
                    }
                }
            },
            MessageState::InFlight(ts) => {
                if let Some(queue) = self.waiting_for_ack.get_mut(&ts) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_ack.remove(&ts);
                    }
                }
            },
        }

        // 3. FASE DI AGGIORNAMENTO (Update Registry)
        msg.state = new_state.clone();

        // 4. FASE DI INSERIMENTO (Add to New Index)
        match new_state {
            MessageState::Ready => {
                self.waiting_for_dispatch
                    .entry(msg.priority)
                    .or_default()
                    .insert(id);
                
                // Notifica eventuali consumer in attesa
                if let Some(waiter) = self.batch_waiters.pop_front() {
                    waiter.notify_one();
                }
            },
            MessageState::Scheduled(ts) => {
                self.waiting_for_time
                    .entry(ts)
                    .or_default()
                    .insert(id);
            },
            MessageState::InFlight(ts) => {
                self.waiting_for_ack
                    .entry(ts)
                    .or_default()
                    .insert(id);
            },
        }

        true
    }

    /// Rimuove completamente un messaggio dal sistema (es. ACK o Cancel)
    fn delete_message(&mut self, id: Uuid) -> bool {
        let msg = match self.registry.remove(&id) {
            Some(m) => m,
            None => return false,
        };

        match msg.state {
            MessageState::Ready => {
                if let Some(queue) = self.waiting_for_dispatch.get_mut(&msg.priority) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_dispatch.remove(&msg.priority);
                    }
                }
            },
            MessageState::Scheduled(ts) => {
                if let Some(queue) = self.waiting_for_time.get_mut(&ts) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_time.remove(&ts);
                    }
                }
            },
            MessageState::InFlight(ts) => {
                if let Some(queue) = self.waiting_for_ack.get_mut(&ts) {
                    queue.remove(&id);
                    if queue.is_empty() {
                        self.waiting_for_ack.remove(&ts);
                    }
                }
            },
        }
        true
    }
}

// ---------- Queue ----------

pub struct Queue {
    pub name: String,
    pub config: QueueConfig,
    state: Mutex<InternalState>,
    scheduler_notify: Notify, // Per svegliare "The Pulse"
}

fn format_time(ts: u64) -> String {
    let d = UNIX_EPOCH + Duration::from_millis(ts);
    let datetime = DateTime::<Utc>::from(d);
    datetime.to_rfc3339()
}

impl Queue {
    pub fn new(name: String, config: QueueConfig) -> Self {
        Self {
            name,
            config,
            state: Mutex::new(InternalState::new()),
            scheduler_notify: Notify::new(),
        }
    }

    pub fn push(&self, payload: Bytes, priority: u8, delay_ms: Option<u64>) {
        let mut state = self.state.lock();
        let effective_delay = delay_ms.or(if self.config.default_delay_ms > 0 { Some(self.config.default_delay_ms) } else { None });
        
        let msg = Message::new(payload, priority, effective_delay);
        let id = msg.id;
        let initial_state = msg.state.clone();
        
        // Inseriamo nel registry (ancora senza indici)
        state.registry.insert(id, msg);

        match initial_state {
            MessageState::Ready => {
                state.waiting_for_dispatch.entry(priority).or_default().insert(id);
                // Notifica consumer
                if let Some(waiter) = state.batch_waiters.pop_front() {
                    waiter.notify_one();
                }
            },
            MessageState::Scheduled(ts) => {
                state.waiting_for_time.entry(ts).or_default().insert(id);
                
                // CHECK PER SCHEDULER: Se questo è il primo evento temporale, sveglia il thread!
                let is_earliest = state.waiting_for_time.keys().next().map(|&t| t == ts).unwrap_or(false);
                if is_earliest {
                    self.scheduler_notify.notify_one();
                }
            },
            MessageState::InFlight(_) => {
                // Impossibile per un nuovo messaggio
            }
        }
    }

    pub fn pop(&self) -> Option<Message> {
        let mut state = self.state.lock();
        let now = current_time_ms();
        
        // 1. Trova il prossimo ID dalla coda Ready
        // Iteriamo le priorità al contrario (High -> Low)
        let next_id = state.waiting_for_dispatch.iter().rev().find_map(|(_, queue)| queue.front().cloned())?;

        // 2. Calcola scadenza
        let timeout = now + self.config.visibility_timeout_ms;
        
        // 3. Esegui transizione (Ready -> InFlight)
        state.transition_to(next_id, MessageState::InFlight(timeout));
        
        // 4. CHECK PER SCHEDULER: Se questo timeout è prima di tutti gli altri ACK, sveglia!
        // (Nota: dobbiamo controllare se è il primo in waiting_for_ack)
        let is_earliest = state.waiting_for_ack.keys().next().map(|&t| t == timeout).unwrap_or(false);
        if is_earliest {
            self.scheduler_notify.notify_one();
        }

        // 5. Ritorna il messaggio aggiornato
        let msg = state.registry.get_mut(&next_id)?;
        msg.visible_at = timeout;
        msg.attempts += 1;
        Some(msg.clone())
    }

    pub fn ack(&self, id: Uuid) -> bool {
        let mut state = self.state.lock();
        // Cancellazione O(1) pura e immediata
        state.delete_message(id)
    }

    /// Batch Consume (identico a prima, ma usa la nuova logica interna)
    pub async fn consume_batch(&self, max: usize, wait_ms: u64) -> Vec<Message> {
        // FASE 1
        {
            let mut state = self.state.lock();
            let result = self.take_messages_internal(&mut state, max);
            if !result.is_empty() {
                return result;
            }
        }
        
        // FASE 2
        let notify = Arc::new(Notify::new());
        {
            let mut state = self.state.lock();
            state.batch_waiters.push_back(notify.clone());
        }

        let _ = tokio::time::timeout(Duration::from_millis(wait_ms), notify.notified()).await;

        // FASE 3
        let mut state = self.state.lock();
        self.take_messages_internal(&mut state, max)
    }

    fn take_messages_internal(&self, state: &mut InternalState, max: usize) -> Vec<Message> {
        let mut result = Vec::with_capacity(max);
        let now = current_time_ms();
        
        while result.len() < max {
            // Trova ID
            let next_id = match state.waiting_for_dispatch.iter().rev().find_map(|(_, queue)| queue.front().cloned()) {
                Some(id) => id,
                None => break,
            };

            // Sposta
            let timeout = now + self.config.visibility_timeout_ms;
            state.transition_to(next_id, MessageState::InFlight(timeout));
            
            // Aggiorna e aggiungi
            if let Some(msg) = state.registry.get_mut(&next_id) {
                msg.visible_at = timeout;
                msg.attempts += 1;
                result.push(msg.clone());
            }

            // Nota: qui potremmo ottimizzare il notify scheduler facendolo una volta sola alla fine,
            // ma per ora va bene così (è raro fare batch enormi che cambiano il 'min' molte volte).
            let is_earliest = state.waiting_for_ack.keys().next().map(|&t| t == timeout).unwrap_or(false);
            if is_earliest {
                self.scheduler_notify.notify_one();
            }
        }
        result
    }

    // ==========================================
    // THE PULSE (Unified Background Task)
    // ==========================================

    pub fn start_tasks(self: Arc<Self>, manager: Arc<QueueManager>) {
        tokio::spawn(async move {
            self.run_pulse_loop(manager).await;
        });
    }

    async fn run_pulse_loop(&self, manager: Arc<QueueManager>) {
        loop {
            // 1. Calcola il prossimo evento (Next Wake Up)
            let next_wake_up = {
                let state = self.state.lock();
                
                let next_scheduled = state.waiting_for_time.keys().next().cloned();
                let next_ack = state.waiting_for_ack.keys().next().cloned();
                
                match (next_scheduled, next_ack) {
                    (Some(a), Some(b)) => Some(std::cmp::min(a, b)),
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    (None, None) => None,
                }
            };

            let now = current_time_ms();

            match next_wake_up {
                Some(ts) => {
                    if ts <= now {
                        // È ORA! Processa subito
                        self.process_expired_events(&manager);
                    } else {
                        // DORMI fino a ts OPPURE finché qualcuno non fa notify
                        let duration = Duration::from_millis(ts - now);
                        tokio::select! {
                            _ = tokio::time::sleep(duration) => {
                                // Timeout scaduto: è ora di processare
                                self.process_expired_events(&manager);
                            }
                            _ = self.scheduler_notify.notified() => {
                                // Qualcuno ha inserito un messaggio che scade PRIMA!
                                // Ricomincia il loop e ricalcola il nuovo sleep.
                                continue;
                            }
                        }
                    }
                },
                None => {
                    // Coda vuota, dormi per sempre finché non arriva un notify
                    self.scheduler_notify.notified().await;
                }
            }
        }
    }

    fn process_expired_events(&self, manager: &Arc<QueueManager>) {
         let mut dlq_payloads = Vec::new();
         {
             let mut state = self.state.lock();
             let now = current_time_ms();
             
             let mut ids_to_ready = Vec::new();
             let mut ids_to_dlq = Vec::new();

             // Scheduled
             for (&ts, ids) in state.waiting_for_time.iter() {
                 if ts <= now { ids_to_ready.extend(ids.iter().cloned()); } else { break; }
             }
             
             // Ack
             for (&ts, ids) in state.waiting_for_ack.iter() {
                 if ts <= now {
                     for &id in ids {
                         if let Some(msg) = state.registry.get(&id) {
                             if msg.attempts >= self.config.max_retries {
                                 ids_to_dlq.push(id);
                             } else {
                                 ids_to_ready.push(id);
                             }
                         }
                     }
                 } else { break; }
             }

             for id in ids_to_ready {
                 state.transition_to(id, MessageState::Ready);
             }
             
             for id in ids_to_dlq {
                 if let Some(msg) = state.registry.get(&id) {
                     dlq_payloads.push((msg.payload.clone(), msg.priority));
                 }
                 state.delete_message(id);
             }
         } // Lock dropped

         if !dlq_payloads.is_empty() {
             let dlq_name = format!("{}_dlq", self.name);
             let manager_clone = Arc::clone(manager);
             tokio::spawn(async move {
                 for (payload, priority) in dlq_payloads {
                     let _ = manager_clone.push_internal(dlq_name.clone(), payload, priority, None);
                 }
             });
         }
    }
    
    // Snapshot
    pub fn get_snapshot(&self) -> QueueSummary {
        let state = self.state.lock();
        
        let pending_count: usize = state.waiting_for_dispatch.values().map(|q| q.len()).sum();
        let inflight_count: usize = state.waiting_for_ack.values().map(|v| v.len()).sum();
        let scheduled_count: usize = state.waiting_for_time.values().map(|v| v.len()).sum();
        
        let mut messages = Vec::new();
        
        // Iteriamo direttamente sul registry per avere lo stato pulito
        
        // Pending
        for (priority, queue) in &state.waiting_for_dispatch {
            for id in queue {
                if let Some(msg) = state.registry.get(id) {
                    messages.push(MessageSummary {
                        id: msg.id,
                        payload_preview: String::from_utf8_lossy(&msg.payload).to_string(),
                        state: "Pending".to_string(),
                        priority: *priority,
                        attempts: msg.attempts,
                        next_delivery_at: None,
                    });
                }
            }
        }

        // InFlight
        for (expiry, list) in &state.waiting_for_ack {
            for id in list {
                 if let Some(msg) = state.registry.get(id) {
                     messages.push(MessageSummary {
                        id: msg.id,
                        payload_preview: String::from_utf8_lossy(&msg.payload).to_string(),
                        state: "InFlight".to_string(),
                        priority: msg.priority,
                        attempts: msg.attempts,
                        next_delivery_at: Some(format_time(*expiry)),
                    });
                }
            }
        }

        // Scheduled
        for (time, list) in &state.waiting_for_time {
             for id in list {
                 if let Some(msg) = state.registry.get(id) {
                     messages.push(MessageSummary {
                        id: msg.id,
                        payload_preview: String::from_utf8_lossy(&msg.payload).to_string(),
                        state: "Scheduled".to_string(),
                        priority: msg.priority,
                        attempts: msg.attempts,
                        next_delivery_at: Some(format_time(*time)),
                    });
                }
            }
        }

        QueueSummary {
            name: self.name.clone(),
            pending_count,
            inflight_count,
            scheduled_count,
            consumers_waiting: state.batch_waiters.len(),
            messages,
        }
    }
}

pub fn current_time_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

// ==========================================
// TESTS
// ==========================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn create_queue() -> Arc<Queue> {
        let config = QueueConfig {
            visibility_timeout_ms: 100, // Short timeout for testing
            max_retries: 3,
            ttl_ms: 10000,
            default_delay_ms: 0,
        };
        Arc::new(Queue::new("test_queue".to_string(), config))
    }

    #[tokio::test]
    async fn test_basic_push_pop_ack() {
        let queue = create_queue();
        let payload = Bytes::from("test_payload");

        // Push
        queue.push(payload.clone(), 0, None);
        
        // Verifica stato: 1 Pending
        let snap = queue.get_snapshot();
        assert_eq!(snap.pending_count, 1);
        assert_eq!(snap.inflight_count, 0);

        // Pop
        let msg = queue.pop().expect("Should return message");
        assert_eq!(msg.payload, payload);
        
        // Verifica stato: 0 Pending, 1 InFlight
        let snap = queue.get_snapshot();
        assert_eq!(snap.pending_count, 0);
        assert_eq!(snap.inflight_count, 1);

        // Ack
        let ack_result = queue.ack(msg.id);
        assert!(ack_result);

        // Verifica stato: 0 Pending, 0 InFlight
        let snap = queue.get_snapshot();
        assert_eq!(snap.pending_count, 0);
        assert_eq!(snap.inflight_count, 0);
    }

    #[tokio::test]
    async fn test_scheduler_timeout_retry() {
        let queue = create_queue();
        let manager = Arc::new(QueueManager::new()); // Mockish manager

        // Push
        queue.push(Bytes::from("retry_me"), 0, None);
        
        // Pop (lo mette in InFlight con timeout 100ms)
        let msg = queue.pop().unwrap();
        
        // Aspetta che scada il timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Esegui ciclo scheduler manualmente
        queue.process_expired_events(&manager);

        // Verifica: deve essere tornato in Pending (Retry)
        let snap = queue.get_snapshot();
        assert_eq!(snap.pending_count, 1, "Message should be back in pending");
        assert_eq!(snap.inflight_count, 0, "Message should not be in inflight");
        
        // Pop di nuovo (Retry 1)
        let msg2 = queue.pop().unwrap();
        assert_eq!(msg2.id, msg.id);
        assert_eq!(msg2.attempts, 2);
    }

    #[tokio::test]
    async fn test_scheduler_delay() {
        let queue = create_queue();
        let manager = Arc::new(QueueManager::new());

        // Push con delay 100ms
        queue.push(Bytes::from("delayed"), 0, Some(100));

        let snap = queue.get_snapshot();
        assert_eq!(snap.scheduled_count, 1);
        assert_eq!(snap.pending_count, 0);

        // Esegui scheduler prima del tempo -> Niente succede
        queue.process_expired_events(&manager);
        let snap = queue.get_snapshot();
        assert_eq!(snap.scheduled_count, 1);

        // Aspetta
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Esegui scheduler -> Deve passare a Pending
        queue.process_expired_events(&manager);
        let snap = queue.get_snapshot();
        assert_eq!(snap.scheduled_count, 0);
        assert_eq!(snap.pending_count, 1);
    }

    #[tokio::test]
    async fn test_priority_fifo() {
        let queue = create_queue();
        
        // Push mixed priorities
        queue.push(Bytes::from("low"), 0, None);
        queue.push(Bytes::from("high_1"), 10, None);
        queue.push(Bytes::from("high_2"), 10, None);
        queue.push(Bytes::from("mid"), 5, None);

        // Expect: High_1, High_2, Mid, Low
        let msg1 = queue.pop().unwrap();
        assert_eq!(msg1.priority, 10);
        assert_eq!(msg1.payload, Bytes::from("high_1")); // FIFO within priority

        let msg2 = queue.pop().unwrap();
        assert_eq!(msg2.priority, 10);
        assert_eq!(msg2.payload, Bytes::from("high_2"));

        let msg3 = queue.pop().unwrap();
        assert_eq!(msg3.priority, 5);
        assert_eq!(msg3.payload, Bytes::from("mid"));

        let msg4 = queue.pop().unwrap();
        assert_eq!(msg4.priority, 0);
        assert_eq!(msg4.payload, Bytes::from("low"));
    }

    #[tokio::test]
    async fn test_dlq_movement() {
        let mut config = QueueConfig::default();
        config.visibility_timeout_ms = 50;
        config.max_retries = 2; // 2 attempts allowed (0 and 1). After 2nd failure -> DLQ
        let queue = Arc::new(Queue::new("main_queue".to_string(), config));
        
        let manager = Arc::new(QueueManager::new());
        manager.set_self(manager.clone()); // Setup self ref for DLQ creation
        
        // Push
        queue.push(Bytes::from("toxic"), 0, None);

        // Attempt 1
        let msg = queue.pop().unwrap(); 
        assert_eq!(msg.attempts, 1);
        // Fail (Timeout)
        tokio::time::sleep(Duration::from_millis(60)).await;
        queue.process_expired_events(&manager);
        
        // Attempt 2
        let msg = queue.pop().unwrap(); 
        assert_eq!(msg.attempts, 2);
        // Fail (Timeout) -> Should go to DLQ now
        tokio::time::sleep(Duration::from_millis(60)).await;
        queue.process_expired_events(&manager);

        // Should be empty in main queue
        let snap = queue.get_snapshot();
        assert_eq!(snap.pending_count, 0);
        assert_eq!(snap.inflight_count, 0);

        // Wait for async spawn DLQ push
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Verify DLQ exists and has message
        let dlq = manager.get("main_queue_dlq").expect("DLQ should exist");
        let snap_dlq = dlq.get_snapshot();
        assert_eq!(snap_dlq.pending_count, 1);
        
        let dlq_msg = dlq.pop().unwrap();
        assert_eq!(dlq_msg.payload, Bytes::from("toxic"));
    }

    #[tokio::test]
    async fn test_delete_message_mid_flight() {
        let queue = create_queue();
        
        // Push & Pop (InFlight)
        queue.push(Bytes::from("todelete"), 0, None);
        let msg = queue.pop().unwrap();
        
        // Delete manually (simulates Cancel or Ack)
        let deleted = queue.ack(msg.id); // ack calls delete_message
        assert!(deleted);

        // Verify it's gone
        let snap = queue.get_snapshot();
        assert_eq!(snap.inflight_count, 0);
        assert_eq!(snap.pending_count, 0);
        
        // Double delete should return false
        assert!(!queue.ack(msg.id));
    }
}
