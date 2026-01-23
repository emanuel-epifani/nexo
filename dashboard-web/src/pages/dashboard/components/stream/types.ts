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
  groups: ConsumerGroupSummary[];
  last_offset: number; // u64 (High Watermark)
}

export interface ConsumerGroupSummary {
  id: string;
  committed_offset: number;
}

export interface MessagePreview {
  offset: number; // u64
  timestamp: string;
  payload: any;
}
