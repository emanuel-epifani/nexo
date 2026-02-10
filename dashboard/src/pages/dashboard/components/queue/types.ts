export type QueueBrokerSnapshot = QueueSummary[];

export interface QueueSummary {
  name: string;
  pending: MessageSummary[];
  inflight: MessageSummary[];
  scheduled: ScheduledMessageSummary[];
  dlq: DlqMessageSummary[];
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

export interface DlqMessageSummary extends MessageSummary {
  failure_reason: string;
  created_at: number;
}
