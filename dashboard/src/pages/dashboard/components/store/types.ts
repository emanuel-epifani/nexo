export interface StoreBrokerSnapshot {
  keys: KeyDetail[];
}

export interface KeyDetail {
  key: string;
  value: any;
  expires_at: string; // ISO8601
}
