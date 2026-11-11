import { z } from "zod";
import { withTx } from "./db.js";
import { CHANNEL } from "./notify.js";
import { HttpError, type Ctx } from "./http.js";

export const EventSchema = z
  .object({
    key: z.string().regex(/^[A-Za-z0-9_.\/-]{1,64}$/),
    step: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
    value: z.number().finite(),
    ts: z.string().datetime({ offset: true }).optional(),
  })
  .strict();

export type MetricEvent = z.infer<typeof EventSchema>;

export const batchSchema = (maxBatch: number) =>
  z.object({ events: z.array(EventSchema).min(1).max(maxBatch) }).strict();

export interface Conflict {
  key: string;
  step: number;
  stored: number;
  rejected: number;
}

export interface IngestResult {
  /** New points written by this request. */
  accepted: number;
  /** Points already stored with the same value (a harmless resend). */
  duplicates: number;
  /** Points already stored with a DIFFERENT value. First write wins. */
  conflicts: Conflict[];
  /** Head of the run's log after this request. */
  last_seq: number;
}

const idOf = (key: string, step: number) => `${key}\n${step}`;

/**
 * One batch = one transaction. Either every new point is stored and gets its
 * seq, or nothing changes.
 *
 * The `FOR UPDATE` on the run row serializes writers per run. That is what
 * makes seq trustworthy: batch N+1 cannot take its numbers until batch N has
 * committed, so the set of visible seqs is always exactly 1..last_seq and a
 * reader doing `seq > cursor` can never skip a row that commits late.
 */
export async function ingestBatch(
  ctx: Ctx,
  projectId: string,
  runId: string,
  events: MetricEvent[],
): Promise<IngestResult> {
  return withTx(ctx.pool, async (tx) => {
    const run = await tx.query("SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2 FOR UPDATE", [
      runId,
      projectId,
    ]);
    if (run.rowCount !== 1) throw new HttpError(404, "not found");
    const lastSeq: number = run.rows[0].last_seq;

    const existing = await tx.query(
      `SELECT m.key, m.step, m.value
         FROM metrics m
         JOIN UNNEST($2::text[], $3::bigint[]) AS u(key, step) ON m.key = u.key AND m.step = u.step
        WHERE m.run_id = $1`,
      [runId, events.map((e) => e.key), events.map((e) => e.step)],
    );
    const known = new Map<string, number>();
    for (const r of existing.rows) known.set(idOf(r.key, r.step), r.value);

    const fresh: MetricEvent[] = [];
    const conflicts: Conflict[] = [];
    let duplicates = 0;
    for (const e of events) {
      const id = idOf(e.key, e.step);
      const stored = known.get(id);
      if (stored === undefined) {
        fresh.push(e);
        known.set(id, e.value); // a repeat later in this same batch is judged against it
      } else if (stored === e.value) {
        duplicates++;
      } else {
        conflicts.push({ key: e.key, step: e.step, stored, rejected: e.value });
      }
    }

    if (fresh.length > 0) {
      await tx.query(
        `INSERT INTO metrics (run_id, key, step, value, client_ts, seq)
         SELECT $1, u.key, u.step, u.value, u.ts, $6::bigint + u.ord
           FROM UNNEST($2::text[], $3::bigint[], $4::float8[], $5::timestamptz[])
                WITH ORDINALITY AS u(key, step, value, ts, ord)`,
        [
          runId,
          fresh.map((e) => e.key),
          fresh.map((e) => e.step),
          fresh.map((e) => e.value),
          fresh.map((e) => e.ts ?? null),
          lastSeq,
        ],
      );
      await ctx.faults.afterInsert?.();
      await tx.query("UPDATE runs SET last_seq = $2, updated_at = now() WHERE id = $1", [
        runId,
        lastSeq + fresh.length,
      ]);
      // Delivered by Postgres only if and when this transaction commits.
      await tx.query("SELECT pg_notify($1, $2)", [CHANNEL, `run:${runId}`]);
    }

    if (conflicts.length > 0) {
      await tx.query(
        `INSERT INTO metric_conflicts (run_id, key, step, stored_value, rejected_value)
         SELECT $1, * FROM UNNEST($2::text[], $3::bigint[], $4::float8[], $5::float8[])`,
        [
          runId,
          conflicts.map((c) => c.key),
          conflicts.map((c) => c.step),
          conflicts.map((c) => c.stored),
          conflicts.map((c) => c.rejected),
        ],
      );
    }

    return { accepted: fresh.length, duplicates, conflicts, last_seq: lastSeq + fresh.length };
  });
}
