export interface StoreBrokerSnapshot {
  keys: KeyDetail[];
}

export interface KeyDetail {
  key: string;
  value: any;
  exp_at: string; // ISO8601
}
