export interface PubSubBrokerSnapshot {
  active_clients: number;
  total_topics: number;
  topics: TopicSnapshot[];
  wildcards: WildcardSubscriptions;
}

export interface WildcardSubscriptions {
  multi_level: WildcardSubscription[];
  single_level: WildcardSubscription[];
}

export interface WildcardSubscription {
  pattern: string;
  client_id: string;
}

export interface TopicSnapshot {
  full_path: string;
  subscribers: number;
  retained_value: any | null;
}
