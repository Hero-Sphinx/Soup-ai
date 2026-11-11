import pg from "pg";

export const CHANNEL = "soup";

type Listener = () => void;

/**
 * One dedicated LISTEN connection per app instance, fanned out in-process.
 *
 * A notification is only a wake-up ("run X changed", "key Y revoked"). It
 * carries no data, so losing one can delay a stream but never corrupt it:
 * every stream also re-reads the database on a timer.
 */
export class Hub {
  private topics = new Map<string, Set<Listener>>();
  private client: pg.Client | null = null;
  private stopped = false;
  private retry: NodeJS.Timeout | null = null;
  /** For tests: the backend pid of the LISTEN connection. */
  listenerPid: number | null = null;

  constructor(
    private databaseUrl: string,
    private onError: (msg: string) => void,
  ) {}

  async start(): Promise<void> {
    await this.connect();
  }

  private async connect(): Promise<void> {
    if (this.stopped) return;
    const client = new pg.Client({ connectionString: this.databaseUrl });
    const lost = () => {
      if (this.client !== client) return;
      this.client = null;
      this.listenerPid = null;
      client.end().catch(() => {});
      if (!this.stopped) {
        this.onError("listen connection lost; streams fall back to polling until it is back");
        this.retry = setTimeout(() => void this.connect().catch(() => lostAgain()), 1000);
      }
    };
    const lostAgain = () => {
      if (!this.stopped) this.retry = setTimeout(() => void this.connect().catch(() => lostAgain()), 1000);
    };
    client.on("error", lost);
    client.on("end", lost);
    client.on("notification", (msg) => {
      if (msg.payload) this.emit(msg.payload);
    });
    await client.connect();
    await client.query(`LISTEN ${CHANNEL}`);
    const pid = await client.query("SELECT pg_backend_pid() AS pid");
    this.client = client;
    this.listenerPid = pid.rows[0].pid;
  }

  private emit(topic: string): void {
    const set = this.topics.get(topic);
    if (set) for (const fn of [...set]) fn();
  }

  subscribe(topic: string, fn: Listener): () => void {
    let set = this.topics.get(topic);
    if (!set) this.topics.set(topic, (set = new Set()));
    set.add(fn);
    return () => {
      set.delete(fn);
      if (set.size === 0) this.topics.delete(topic);
    };
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.retry) clearTimeout(this.retry);
    const c = this.client;
    this.client = null;
    if (c) await c.end().catch(() => {});
  }
}
