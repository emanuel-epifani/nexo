export const ProvisionOutcome = {
  CREATED: 'created',
  UNCHANGED: 'unchanged',
} as const;

export type ProvisionOutcome = typeof ProvisionOutcome[keyof typeof ProvisionOutcome];

export interface ProvisionResult<T> {
  status: ProvisionOutcome;
  definition: T;
}
