export interface PubSubBrokerSnapshot {
  active_clients: number;
  topics: TopicSnapshot[];
  wildcard_subscriptions: WildcardSubscription[];
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
