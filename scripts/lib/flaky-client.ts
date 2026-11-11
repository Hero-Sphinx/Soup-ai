// A metrics client on a bad network. Used by the tests, the demo simulator
// and the load test, so "unreliable" means the same thing everywhere.

export interface MetricEvent {
  key: string;
  step: number;
  value: number;
  ts?: string;
}

/** Small seeded PRNG so a failing run can be replayed with the same seed. */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function makeEvents(keys: string[], steps: number, rng: () => number, firstStep = 0): MetricEvent[] {
  const out: MetricEvent[] = [];
  for (let s = firstStep; s < firstStep + steps; s++) {
    for (const key of keys) out.push({ key, step: s, value: Math.round(rng() * 1e9) / 1e6 });
  }
  return out;
}

export interface ChaosOptions {
  baseUrl: string;
  apiKey: string;
  runId: string;
  events: MetricEvent[];
  rng: () => number;
  maxBatch?: number;
  concurrency?: number;
  /** Random delay before each batch, to spread the run over time. */
  paceMs?: number;
  /** Size of the window events are shuffled in (out-of-order delivery). */
  shuffleWindow?: number;
  /** Fraction of events held back and delivered only at the very end. */
  lateFraction?: number;
  /** Request "lost" before it is sent: the client waits, then retries. */
  pLostRequest?: number;
  /** Response lost: server stored the batch, client never hears, resends. */
  pLostResponse?: number;
  /** Request aborted a few ms in: the server may or may not have stored it. */
  pAbort?: number;
  /** An already-acknowledged batch is sent again later, mixed and reversed. */
  pStaleResend?: number;
}

export interface ChaosStats {
  events: number;
  batches: number;
  requests: number;
  lostRequests: number;
  lostResponses: number;
  aborted: number;
  staleResends: number;
  lateEvents: number;
  accepted: number;
  duplicates: number;
  conflicts: number;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function sendUnreliably(o: ChaosOptions): Promise<ChaosStats> {
  const rng = o.rng;
  const maxBatch = o.maxBatch ?? 40;
  const window = o.shuffleWindow ?? 150;
  const stats: ChaosStats = {
    events: o.events.length,
    batches: 0,
    requests: 0,
    lostRequests: 0,
    lostResponses: 0,
    aborted: 0,
    staleResends: 0,
    lateEvents: 0,
    accepted: 0,
    duplicates: 0,
    conflicts: 0,
  };

  // 1. late events: pulled out and appended at the end
  const onTime: MetricEvent[] = [];
  const late: MetricEvent[] = [];
  for (const e of o.events) (rng() < (o.lateFraction ?? 0.05) ? late : onTime).push(e);
  stats.lateEvents = late.length;

  // 2. out of order: shuffle inside sliding windows
  for (let start = 0; start < onTime.length; start += window) {
    const end = Math.min(start + window, onTime.length);
    for (let i = end - 1; i > start; i--) {
      const j = start + Math.floor(rng() * (i - start + 1));
      [onTime[i], onTime[j]] = [onTime[j]!, onTime[i]!];
    }
  }
  const ordered = [...onTime, ...late];

  // 3. batches of random size
  const queue: MetricEvent[][] = [];
  for (let i = 0; i < ordered.length; ) {
    const n = 1 + Math.floor(rng() * maxBatch);
    queue.push(ordered.slice(i, i + n));
    i += n;
  }
  stats.batches = queue.length;

  const post = async (batch: MetricEvent[], signal?: AbortSignal) => {
    stats.requests++;
    const res = await fetch(`${o.baseUrl}/runs/${o.runId}/metrics`, {
      method: "POST",
      headers: { authorization: `Bearer ${o.apiKey}`, "content-type": "application/json" },
      body: JSON.stringify({ events: batch }),
      signal,
    });
    const body = (await res.json()) as { accepted: number; duplicates: number; conflicts: unknown[] };
    if (res.status !== 200) throw Object.assign(new Error(`ingest ${res.status}`), { status: res.status });
    return body;
  };

  /** Keeps sending until the server acknowledges: at-least-once delivery. */
  const deliver = async (batch: MetricEvent[]) => {
    for (let attempt = 0; ; attempt++) {
      try {
        const ack = await post(batch);
        // The client-side loss check: every event must be accounted for.
        if (ack.accepted + ack.duplicates + ack.conflicts.length !== batch.length) {
          throw new Error("ack does not account for every event in the batch");
        }
        stats.accepted += ack.accepted;
        stats.duplicates += ack.duplicates;
        stats.conflicts += ack.conflicts.length;
        return;
      } catch (err) {
        const status = (err as { status?: number }).status;
        if ((status && status < 500) || attempt >= 8) throw err;
        await sleep(20 * (attempt + 1));
      }
    }
  };

  const worker = async () => {
    for (;;) {
      const batch = queue.shift();
      if (!batch) return;
      if (o.paceMs) await sleep(rng() * o.paceMs);
      const roll = rng();
      const pA = o.pLostRequest ?? 0.08;
      const pB = pA + (o.pLostResponse ?? 0.08);
      const pC = pB + (o.pAbort ?? 0.08);
      if (roll < pA) {
        stats.lostRequests++;
        await sleep(5 + rng() * 20);
      } else if (roll < pB) {
        stats.lostResponses++;
        await post(batch).catch(() => {}); // stored, but the client "never hears back"
      } else if (roll < pC) {
        stats.aborted++;
        const ctrl = new AbortController();
        setTimeout(() => ctrl.abort(), rng() * 12);
        await post(batch, ctrl.signal).catch(() => {});
      }
      await deliver(batch);
      if (rng() < (o.pStaleResend ?? 0.1)) {
        stats.staleResends++;
        queue.push([...batch].reverse().filter(() => rng() < 0.7).concat(batch[0]!));
      }
    }
  };

  await Promise.all(Array.from({ length: o.concurrency ?? 4 }, worker));
  return stats;
}
