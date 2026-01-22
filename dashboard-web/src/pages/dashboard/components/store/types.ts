export interface StoreBrokerSnapshot {
  total_keys: number;
  expiring_keys: number;
  map: MapStructure;
}

export interface MapStructure {
  keys: KeyDetail[];
}

export interface KeyDetail {
  key: string;
  value_preview: string;
  expires_at: string | null; // ISO8601
}
