export function formatDashboardValue(value: unknown): string {
  return typeof value === 'string'
    ? value
    : JSON.stringify(value, null, 2)
}

export function getDashboardValueSize(value: unknown): number {
  return new Blob([
    typeof value === 'string' ? value : JSON.stringify(value)
  ]).size
}

export function getDashboardValueKind(value: unknown): string {
  return typeof value === 'string' && value.startsWith('0x')
    ? 'BINARY DATA'
    : 'UTF-8 TEXT'
}
