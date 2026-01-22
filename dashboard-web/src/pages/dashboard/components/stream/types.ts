export interface StreamBrokerSnapshot {
  total_topics: number;
  total_active_groups: number;
  topics: TopicSummary[];
}

export interface TopicSummary {
  name: string;
  partitions: PartitionInfo[];
}

export interface PartitionInfo {
  id: number; // u32
  messages: MessagePreview[];
  current_consumers: string[];
  last_offset: number; // u64
}

export interface MessagePreview {
  offset: number; // u64
  timestamp: string;
  payload_preview: string;
}
