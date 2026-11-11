import type { ServerResponse } from "node:http";
import type { FastifyReply } from "fastify";
import { HttpError, type Ctx, type Principal } from "./http.js";

interface Conn {
  raw: ServerResponse;
  projectId: string;
}

export class StreamRegistry {
  private open = new Set<Conn>();
  stats = { opened: 0, closedStalled: 0, closedRevoked: 0, maxBufferedBytes: 0 };

  add(c: Conn) {
    this.open.add(c);
    this.stats.opened++;
  }
  remove(c: Conn) {
    this.open.delete(c);
  }
  get size() {
    return this.open.size;
  }
  countForProject(projectId: string) {
    let n = 0;
    for (const c of this.open) if (c.projectId === projectId) n++;
    return n;
  }
  /** Hard-drops every open stream (shutdown, and "the network died" in tests). */
  killAll(): number {
    const n = this.open.size;
    for (const c of [...this.open]) c.raw.destroy();
    return n;
  }
}

export const SECURITY_HEADERS: Record<string, string> = {
  "cache-control": "no-store",
  "x-content-type-options": "nosniff",
  "referrer-policy": "no-referrer",
  "content-security-policy": "default-src 'self'; frame-ancestors 'none'",
};

export function parseCursor(raw: unknown): number {
  if (raw === undefined || raw === "") return 0;
  if (typeof raw !== "string" || !/^\d{1,15}$/.test(raw)) throw new HttpError(400, "invalid cursor");
  return Number(raw);
}

/**
 * A stream is a tailing reader of the run's log in Postgres, not a queue.
 *
 * It keeps one number (cursor = last seq written to this socket) and loops:
 * read rows with seq > cursor, write them, advance. First connect, live
 * updates and catch-up after a disconnect are all this same loop; only the
 * starting cursor differs. Because nothing is buffered per client beyond one
 * page, a slow client cannot overflow anything: we simply stop reading from
 * the database until its socket drains, and cut it off if it stalls for too
 * long. It then resumes from its own cursor.
 */
export async function serveStream(
  ctx: Ctx,
  principal: Principal,
  runId: string,
  startCursor: number,
  reply: FastifyReply,
): Promise<void> {
  const { pool, hub, config, streams } = ctx;

  const run = await pool.query("SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2", [
    runId,
    principal.projectId,
  ]);
  if (run.rowCount !== 1) throw new HttpError(404, "not found");
  if (streams.countForProject(principal.projectId) >= config.maxStreamsPerProject) {
    throw new HttpError(429, "too many streams");
  }

  reply.hijack();
  const raw = reply.raw;
  raw.writeHead(200, {
    ...SECURITY_HEADERS,
    "content-type": "text/event-stream; charset=utf-8",
    connection: "keep-alive",
    "x-accel-buffering": "no",
  });

  const conn: Conn = { raw, projectId: principal.projectId };
  streams.add(conn);

  let closed = false;
  let revoked = false;
  let dirty = true;
  let wake: (() => void) | null = null;
  const poke = () => {
    dirty = true;
    wake?.();
  };
  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(t);
        wake = null;
        resolve();
      };
      const t = setTimeout(done, ms);
      wake = done;
    });

  raw.on("close", () => {
    closed = true;
    poke();
  });
  const unsubRun = hub.subscribe(`run:${runId}`, poke);
  const unsubKey = hub.subscribe(`key:${principal.keyId}`, () => {
    revoked = true;
    poke();
  });

  let reason = "client closed";

  /** Resolves once the kernel accepted the bytes; kills a client that stalls. */
  const write = async (chunk: string) => {
    // The socket may have closed while we were awaiting the database. Its
    // 'close' event has then already fired and will not fire again, so
    // waiting for it here would hang this loop (and leak the stream) forever.
    if (closed || raw.destroyed) return;
    if (raw.write(chunk)) return;
    streams.stats.maxBufferedBytes = Math.max(streams.stats.maxBufferedBytes, raw.writableLength);
    await new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(t);
        raw.off("drain", done);
        raw.off("close", done);
        resolve();
      };
      const t = setTimeout(() => {
        streams.stats.closedStalled++;
        reason = "stalled";
        raw.destroy();
        done();
      }, config.streamStallMs);
      raw.on("drain", done);
      raw.on("close", done);
    });
  };

  let cursor = startCursor;
  let lastKeyCheck = Date.now();
  try {
    if (cursor > run.rows[0].last_seq) {
      // The client is ahead of the log (e.g. database restored from backup).
      // Its state cannot be trusted: tell it to start over.
      cursor = 0;
      await write(`event: reset\ndata: {}\n\n`);
    }
    while (!closed) {
      if (!dirty) await sleep(config.streamPollMs);
      if (closed) break;
      dirty = false;

      // A stream opened with a key that was later rotated must not live on.
      if (!revoked && Date.now() - lastKeyCheck >= config.streamPollMs) {
        lastKeyCheck = Date.now();
        const k = await pool.query("SELECT 1 FROM api_keys WHERE id = $1 AND revoked_at IS NULL", [
          principal.keyId,
        ]);
        if (k.rowCount !== 1) revoked = true;
      }
      if (revoked) {
        streams.stats.closedRevoked++;
        reason = "key revoked";
        await write(`event: revoked\ndata: {}\n\n`);
        break;
      }

      for (;;) {
        const page = await pool.query(
          "SELECT key, step, value, seq FROM metrics WHERE run_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
          [runId, cursor, config.streamPage],
        );
        if (page.rows.length === 0) break;
        cursor = page.rows[page.rows.length - 1].seq;
        await write(`id: ${cursor}\nevent: points\ndata: ${JSON.stringify(page.rows)}\n\n`);
        if (closed || page.rows.length < config.streamPage) break;
      }
      // "You now hold everything up to seq N." Doubles as the heartbeat.
      if (!closed) await write(`event: sync\ndata: {"seq":${cursor}}\n\n`);
    }
  } catch (err) {
    reason = "error";
    ctx.log({ level: "error", msg: "stream failed", run_id: runId, error: String((err as Error)?.message ?? err) });
  } finally {
    unsubRun();
    unsubKey();
    streams.remove(conn);
    if (!raw.destroyed) raw.end();
    ctx.log({
      msg: "stream closed",
      project_id: principal.projectId,
      key_id: principal.keyId,
      run_id: runId,
      from_seq: startCursor,
      to_seq: cursor,
      reason,
    });
  }
}
