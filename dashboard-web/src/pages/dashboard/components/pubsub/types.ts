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
