export class Subscription {
  private stopped = false;
  private completed = false;
  private stopPromise: Promise<void> | null = null;
  private readonly stopFn: () => Promise<void>;
  private readonly activeFn?: () => boolean;
  private resolveClosed!: () => void;
  private readonly hasCompletion: boolean;
  private readonly errorFn?: () => unknown;
  private completionError: unknown = null;

  public readonly closed: Promise<void>;

  constructor(
    stopFn: () => Promise<void>,
    activeFn?: () => boolean,
    completion?: Promise<unknown>,
    errorFn?: () => unknown,
  ) {
    this.stopFn = stopFn;
    this.activeFn = activeFn;
    this.errorFn = errorFn;
    this.hasCompletion = completion !== undefined;
    this.closed = new Promise(resolve => {
      this.resolveClosed = resolve;
    });
    if (completion) {
      void completion.then(
        () => this.complete(),
        error => {
          this.completionError = error;
          this.complete();
        },
      );
    }
  }

  get active(): boolean {
    if (this.stopped || this.completed) return false;
    return this.activeFn ? this.activeFn() : true;
  }

  get error(): unknown {
    return this.errorFn?.() ?? this.completionError;
  }

  stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;

    this.stopped = true;
    this.stopPromise = Promise.resolve()
      .then(() => this.stopFn())
      .finally(() => {
        if (!this.hasCompletion) this.complete();
      });
    void this.stopPromise.catch(() => undefined);
    return this.stopPromise;
  }

  private complete(): void {
    if (this.completed) return;
    this.completed = true;
    this.resolveClosed();
  }
}
