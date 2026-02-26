export interface StreamBrokerSnapshot {
  topics: TopicSummary[];
}

export interface TopicSummary {
  name: string;
  last_seq: number;
  groups: ConsumerGroupSummary[];
}

export interface ConsumerGroupSummary {
  id: string;
  ack_floor: number;
  pending_count: number;
}

export interface MessagePreview {
  seq: number;
  timestamp: string;
  payload: unknown;
}

export interface StreamMessages {
  messages: MessagePreview[];
  from_seq: number;
  limit: number;
  last_seq: number;
}
