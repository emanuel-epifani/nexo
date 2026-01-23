export interface QueueBrokerSnapshot {
  active_queues: QueueSummary[];
  dlq_queues: QueueSummary[];
}

export interface QueueSummary {
  name: string;
  pending: MessageSummary[];
  inflight: MessageSummary[];
  scheduled: MessageSummary[];
}

export interface MessageSummary {
  id: string; // UUID
  payload: string;
  state: string; // "Pending", "InFlight", "Scheduled"
  priority: number; // u8
  attempts: number; // u32
  next_delivery_at: string | null;
}
