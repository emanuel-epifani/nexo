export interface QueueBrokerSnapshot {
  active_queues: QueueSummary[];
  dlq_queues: QueueSummary[];
}

export interface QueueSummary {
  name: string;
  pending: MessageSummary[];
  inflight: MessageSummary[];
  scheduled: ScheduledMessageSummary[];
}

export interface MessageSummary {
  id: string; // UUID
  payload: any;
  state: string; // "Pending", "InFlight", "Scheduled"
  priority: number; // u8
  attempts: number; // u32
}

export interface ScheduledMessageSummary extends MessageSummary {
  next_delivery_at: string;
}
