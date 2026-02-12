export interface StoreBrokerSnapshot {
  keys: KeyDetail[];
  total: number;
  offset: number;
  limit: number;
}

export interface KeyDetail {
  key: string;
  value: any;
  exp_at: string; // ISO8601
}
