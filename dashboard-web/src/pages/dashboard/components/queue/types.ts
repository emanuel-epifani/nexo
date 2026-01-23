export interface QueueBrokerSnapshot {
  queues: QueueSummary[];
}

export interface QueueSummary {
  name: string;
  pending_count: number;
  inflight_count: number;
  scheduled_count: number;
  messages: MessageSummary[];
}

export interface MessageSummary {
  id: string; // UUID
  payload_preview: string;
  state: string; // "Pending", "InFlight", "Scheduled"
  priority: number; // u8
  attempts: number; // u32
  next_delivery_at: string | null;
}
