export interface StoreBrokerSnapshot {
  keys: KeyDetail[];
}

export interface KeyDetail {
  key: string;
  value: string;
  expires_at: string; // ISO8601
}
