// --- STREAM BROKER ---

export interface MemberDetail {
    client_id: string;
    partitions_assigned: number[];
}

export interface GroupSummary {
    name: string;
    pending_messages: number;
    connected_clients: number;
    members: MemberDetail[];
}

export interface TopicSummary {
    name: string;
    partitions_count: number;
    retention_ms: number;
    total_messages: number;
    consumer_groups: GroupSummary[];
}

export interface StreamBrokerSnapshot {
    total_topics: number;
    total_active_groups: number;
    topics: TopicSummary[];
}

// --- QUEUE BROKER ---

export interface QueueSummary {
    name: string;
    pending_count: number;
    inflight_count: number;
    scheduled_count: number;
    consumers_waiting: number;
}

export interface QueueBrokerSnapshot {
    queues: QueueSummary[];
}

// --- STORE BROKER ---

export interface KeyDetail {
    key: string;
    value_preview: string;
    created_at: string | null;
    expires_at: string | null;
}

export interface StoreBrokerSnapshot {
    total_keys: number;
    expiring_keys: number;
    keys: KeyDetail[];
}

// --- PUB/SUB BROKER ---

export interface TopicNodeSnapshot {
    name: string;
    subscribers: number;
    retained_msg: boolean;
    children: TopicNodeSnapshot[];
}

export interface PubSubBrokerSnapshot {
    active_clients: number;
    topic_tree: TopicNodeSnapshot;
}

// --- SYSTEM ---

export interface BrokersSnapshot {
    stream: StreamBrokerSnapshot;
    queue: QueueBrokerSnapshot;
    store: StoreBrokerSnapshot;
    pubsub: PubSubBrokerSnapshot;
}

export interface SystemSnapshot {
    uptime_seconds: number;
    server_time: string;
    brokers: BrokersSnapshot;
}
