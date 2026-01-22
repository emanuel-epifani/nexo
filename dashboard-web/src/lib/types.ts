export interface SystemSnapshot {
  brokers: BrokersSnapshot;
}

export interface BrokersSnapshot {
  store: StoreBrokerSnapshot;
  queue: QueueBrokerSnapshot;
  pubsub: PubSubBrokerSnapshot;
  stream: StreamBrokerSnapshot;
}

// --- Queue Broker ---
export interface QueueBrokerSnapshot {
  queues: QueueSummary[];
}

export interface QueueSummary {
  name: string;
  pending_count: number;
  inflight_count: number;
  scheduled_count: number;
  consumers_waiting: number;
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

// --- Stream Broker ---
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

// --- PubSub Broker ---
export interface PubSubBrokerSnapshot {
  active_clients: number;
  topic_tree: TopicNodeSnapshot;
  wildcard_subscriptions: WildcardSubscription[];
}

export interface WildcardSubscription {
  pattern: string;
  client_id: string;
}

export interface TopicNodeSnapshot {
  name: string;
  full_path: string;
  subscribers: number;
  retained_value: string | null;
  children: TopicNodeSnapshot[];
}

// --- Store Broker ---
export interface StoreBrokerSnapshot {
  total_keys: number;
  expiring_keys: number;
  map: MapStructure;
}

export interface MapStructure {
  keys: KeyDetail[];
}

export interface KeyDetail {
  key: string;
  value_preview: string;
  expires_at: string | null; // ISO8601
}
