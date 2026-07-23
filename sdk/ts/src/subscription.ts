export class Subscription {
  private _stopped = false;
  private _stopFn: () => Promise<void>;
  private _activeFn?: () => boolean;

  constructor(
    stopFn: () => Promise<void>,
    activeFn?: () => boolean,
  ) {
    this._stopFn = stopFn;
    this._activeFn = activeFn;
  }

  get active(): boolean {
    if (this._stopped) return false;
    if (this._activeFn) return this._activeFn();
    return !this._stopped;
  }

  async stop(): Promise<void> {
    if (this._stopped) return;
    this._stopped = true;
    await this._stopFn();
  }
}
