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

#[derive(Debug, Clone, Serialize, Deserialize)]

// ---------- Message ----------
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

// ---------- QueueInner ----------

pub struct QueueInner {
    pub registry: HashMap<Uuid, Message>,
    pub ready: BTreeMap<u8, VecDeque<Uuid>>,
    pub delayed: BTreeMap<u64, Vec<Uuid>>,
    pub in_flight: BTreeMap<u64, Vec<Uuid>>,
    pub waiting_consumers: VecDeque<oneshot::Sender<Message>>,
}

impl QueueInner {
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
            ready: BTreeMap::new(),
            delayed: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            waiting_consumers: VecDeque::new(),
        }
    }

    pub fn pop_ready_id(&mut self) -> Option<Uuid> {
        for (_priority, bucket) in self.ready.iter_mut().rev() {
            if let Some(id) = bucket.pop_front() {
                return Some(id);
            }
        }
        None
    }
}

// ---------- Queue ----------

pub struct Queue {
    pub name: String,
    inner: Mutex<QueueInner>,
}

impl Queue {
    pub fn new(name: String) -> Self {
        Self {
            name,
            inner: Mutex::new(QueueInner::new()),
        }
    }

    pub fn push(&self, payload: Bytes, priority: u8, delay_ms: Option<u64>) {
        let mut inner = self.inner.lock();
        let mut msg = Message::new(payload, priority, delay_ms);
        let now = current_time_ms();

        if msg.delayed_until.is_none() {
            while let Some(consumer_tx) = inner.waiting_consumers.pop_front() {
                if consumer_tx.send(msg.clone()).is_ok() {
                    let timeout = now + 30000;
                    msg.visible_at = timeout;
                    msg.attempts += 1;
                    inner.registry.insert(msg.id, msg.clone());
                    inner.in_flight.entry(timeout).or_default().push(msg.id);
                    return;
                }
            }
        }

        inner.registry.insert(msg.id, msg.clone());
        if let Some(delay_timestamp) = msg.delayed_until {
            inner.delayed.entry(delay_timestamp).or_default().push(msg.id);
        } else {
            inner.ready.entry(msg.priority).or_insert_with(VecDeque::new).push_back(msg.id);
        }
    }

    pub fn pop(&self) -> Option<Message> {
        let mut inner = self.inner.lock();
        let now = current_time_ms();

        if let Some(id) = inner.pop_ready_id() {
            if let Some(msg) = inner.registry.get_mut(&id) {
                let timeout = now + 30000;
                msg.visible_at = timeout;
                msg.attempts += 1;
                let msg_cloned = msg.clone();
                inner.in_flight.entry(timeout).or_default().push(id);
                return Some(msg_cloned);
            }
        }
        None
    }

    pub fn ack(&self, id: Uuid) -> bool {
        let mut inner = self.inner.lock();
        if let Some(msg) = inner.registry.remove(&id) {
            let timeout = msg.visible_at;
            if let Some(list) = inner.in_flight.get_mut(&timeout) {
                list.retain(|&x| x != id);
                if list.is_empty() {
                    inner.in_flight.remove(&timeout);
                }
            }
            return true;
        }
        false
    }

    pub fn consume(&self) -> oneshot::Receiver<Message> {
        let mut inner = self.inner.lock();
        let (tx, rx) = oneshot::channel();
        
        if let Some(msg) = self.pop_internal(&mut inner) {
            let _ = tx.send(msg);
        } else {
            inner.waiting_consumers.push_back(tx);
        }
        rx
    }

    fn pop_internal(&self, inner: &mut QueueInner) -> Option<Message> {
        let now = current_time_ms();
        if let Some(id) = inner.pop_ready_id() {
            if let Some(msg) = inner.registry.get_mut(&id) {
                let timeout = now + 30000;
                msg.visible_at = timeout;
                msg.attempts += 1;
                let msg_cloned = msg.clone();
                inner.in_flight.entry(timeout).or_default().push(id);
                return Some(msg_cloned);
            }
        }
        None
    }

    pub fn start_reaper(self: Arc<Self>, manager: Arc<QueueManager>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                self.reap(&manager);
            }
        });
    }

    fn reap(&self, manager: &Arc<QueueManager>) {
        let mut inner = self.inner.lock();
        let now = current_time_ms();

        let mut to_ready = Vec::new();
        let mut to_remove_delayed = Vec::new();
        for (&ts, ids) in inner.delayed.iter() {
            if ts <= now {
                to_ready.extend(ids.clone());
                to_remove_delayed.push(ts);
            } else {
                break;
            }
        }
        for ts in to_remove_delayed {
            inner.delayed.remove(&ts);
        }
        for id in to_ready {
            self.move_to_ready(&mut inner, id);
        }

        let mut to_process_if = Vec::new();
        let mut to_remove_if = Vec::new();
        for (&ts, ids) in inner.in_flight.iter() {
            if ts <= now {
                to_process_if.extend(ids.clone());
                to_remove_if.push(ts);
            } else {
                break;
            }
        }
        for ts in to_remove_if {
            inner.in_flight.remove(&ts);
        }

        for id in to_process_if {
            if let Some(msg) = inner.registry.get_mut(&id) {
                if msg.attempts >= 5 {
                    let dlq_name = format!("{}_dlq", self.name);
                    let msg_to_move = msg.clone();
                    inner.registry.remove(&id);
                    let manager_clone = Arc::clone(&manager);
                    tokio::spawn(async move {
                        manager_clone.push(dlq_name, msg_to_move.payload, msg_to_move.priority, None);
                    });
                } else {
                    self.move_to_ready(&mut inner, id);
                }
            }
        }
    }

    fn move_to_ready(&self, inner: &mut QueueInner, id: Uuid) {
        if let Some(msg) = inner.registry.get(&id) {
            while let Some(consumer_tx) = inner.waiting_consumers.pop_front() {
                if consumer_tx.send(msg.clone()).is_ok() {
                    let timeout = current_time_ms() + 30000;
                    inner.in_flight.entry(timeout).or_default().push(id);
                    return;
                }
            }
            inner.ready.entry(msg.priority).or_default().push_back(id);
        }
    }
}

pub fn current_time_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}
