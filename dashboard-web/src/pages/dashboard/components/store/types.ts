export interface StoreBrokerSnapshot {
  total_keys: number;
  map: MapStructure;
}

export interface MapStructure {
  keys: KeyDetail[];
}

export interface KeyDetail {
  key: string;
  value: string;
  expires_at: string; // ISO8601
}
