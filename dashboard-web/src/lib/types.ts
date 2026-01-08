// --- STREAM BROKER ---

export interface MemberSummary {
    client_id: string;
    current_offset: number;
    lag: number;
}

export interface GroupSummary {
    name: string;
    members: MemberSummary[];
}

export interface TopicSummary {
    name: string;
    total_messages: number;
    consumer_groups: GroupSummary[];
}

export interface StreamBrokerSnapshot {
    total_topics: number;
    total_active_groups: number;
    topics: TopicSummary[];
}

// --- QUEUE BROKER ---

export interface MessageSummary {
    id: string;
    payload_preview: string;
    state: string;
    priority: number;
    attempts: number;
    next_delivery_at: string | null;
}

export interface QueueSummary {
    name: string;
    pending_count: number;
    inflight_count: number;
    scheduled_count: number;
    consumers_waiting: number;
    messages: MessageSummary[];
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

export interface PubSubBrokerSnapshot {
    active_clients: number;
    topic_tree: TopicNodeSnapshot;
    wildcard_subscriptions: WildcardSubscription[];
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
