export type QueueBrokerSnapshot = QueueSummary[];

export interface QueueSummary {
  name: string;
  pending: number;
  inflight: number;
  scheduled: number;
  dlq: number;
}

export interface PaginatedMessages {
  messages: MessageSummary[];
  total: number;
}

export interface PaginatedDlqMessages {
  messages: DlqMessageSummary[];
  total: number;
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

export interface DlqMessageSummary {
  id: string; // UUID
  payload: any;
  attempts: number; // u32
  failure_reason: string;
  created_at: number;
}
