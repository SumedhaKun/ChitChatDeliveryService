export class TtlDedupe {
  private readonly entries = new Map<string, number>();

  public constructor(
    private readonly ttlMs: number,
    private readonly maxEntries: number,
  ) {}

  public has(key: string, now = Date.now()): boolean {
    this.prune(now);
    const expiresAt = this.entries.get(key);
    return expiresAt !== undefined && expiresAt > now;
  }

  public add(key: string, now = Date.now()): void {
    this.prune(now);
    this.entries.delete(key);
    this.entries.set(key, now + this.ttlMs);
    while (this.entries.size > this.maxEntries) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined) break;
      this.entries.delete(oldest);
    }
  }

  private prune(now: number): void {
    for (const [key, expiresAt] of this.entries) {
      if (expiresAt > now) continue;
      this.entries.delete(key);
    }
  }
}
