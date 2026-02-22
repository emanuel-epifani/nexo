export interface StreamBrokerSnapshot {
  topics: TopicSummary[];
}

export interface TopicSummary {
  name: string;
  partitions: PartitionInfo[];
}

export interface PartitionInfo {
  id: number;
  groups: ConsumerGroupSummary[];
  last_offset: number;
}

export interface ConsumerGroupSummary {
  id: string;
  committed_offset: number;
}

export interface MessagePreview {
  offset: number;
  timestamp: string;
  payload: unknown;
}

export interface StreamMessages {
  messages: MessagePreview[];
  from_offset: number;
  limit: number;
  last_offset: number;
}
