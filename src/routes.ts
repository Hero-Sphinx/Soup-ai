import { monitorEventLoopDelay } from "node:perf_hooks";
import type { FastifyInstance } from "fastify";
import { z } from "zod";
import { withTx } from "./db.js";
import { adminRoute, HttpError, newApiKey, parseId, parseWith, projectRoute, publicRoute, type Ctx } from "./http.js";
import { batchSchema, ingestBatch } from "./ingest.js";
import { CHANNEL } from "./notify.js";
import { parseCursor, serveStream } from "./stream.js";

const NameBody = z.object({ name: z.string().trim().min(1).max(100) }).strict();
const loopDelay = monitorEventLoopDelay({ resolution: 10 });
loopDelay.enable();

const idParam = (req: { params: unknown }) => parseId((req.params as { id?: unknown }).id);

export function registerRoutes(app: FastifyInstance, ctx: Ctx): void {
  const { pool } = ctx;

  publicRoute(app, ctx, "GET", "/health", async () => {
    await pool.query("SELECT 1");
    return { ok: true };
  });

  // ---- admin: the only place a project comes into existence ----
  adminRoute(app, ctx, "POST", "/admin/projects", async (_p, req, reply) => {
    const { name } = parseWith(NameBody, req.body);
    const { key, hash } = newApiKey();
    const project = await withTx(pool, async (tx) => {
      const p = await tx.query("INSERT INTO projects (name) VALUES ($1) RETURNING id, name", [name]);
      await tx.query("INSERT INTO api_keys (project_id, key_hash) VALUES ($1, $2)", [p.rows[0].id, hash]);
      return p.rows[0];
    });
    reply.code(201);
    // The key is shown exactly once; only its hash is stored.
    return { project, api_key: key };
  });

  // Operational numbers only (no tenant data): what you would graph and alert on.
  adminRoute(app, ctx, "GET", "/admin/stats", async () => ({
    streams: { open: ctx.streams.size, ...ctx.streams.stats },
    db_pool: { total: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount },
    event_loop_delay_ms: { p50: loopDelay.percentile(50) / 1e6, p99: loopDelay.percentile(99) / 1e6, max: loopDelay.max / 1e6 },
    cpu_seconds: (process.cpuUsage().user + process.cpuUsage().system) / 1e6,
    rss_mb: Math.round(process.memoryUsage().rss / 1e6),
  }));

  // ---- project scope: everything below is filtered by principal.projectId ----
  projectRoute(app, ctx, "GET", "/project", async (p) => {
    const r = await pool.query("SELECT id, name FROM projects WHERE id = $1", [p.projectId]);
    return r.rows[0];
  });

  projectRoute(app, ctx, "POST", "/keys/rotate", async (p) => {
    const { key, hash } = newApiKey();
    await withTx(pool, async (tx) => {
      const created = await tx.query("INSERT INTO api_keys (project_id, key_hash) VALUES ($1, $2) RETURNING id", [
        p.projectId,
        hash,
      ]);
      // No grace period: every other key of this project dies in this
      // transaction, and open streams using them are told to close.
      const revoked = await tx.query(
        "UPDATE api_keys SET revoked_at = now() WHERE project_id = $1 AND id <> $2 AND revoked_at IS NULL RETURNING id",
        [p.projectId, created.rows[0].id],
      );
      for (const row of revoked.rows) {
        await tx.query("SELECT pg_notify($1, $2)", [CHANNEL, `key:${row.id}`]);
      }
    });
    return { api_key: key };
  });

  projectRoute(app, ctx, "GET", "/runs", async (p) => {
    const r = await pool.query(
      `SELECT id, name, last_seq, created_at, updated_at
         FROM runs WHERE project_id = $1 ORDER BY created_at DESC, id LIMIT 200`,
      [p.projectId],
    );
    return { runs: r.rows };
  });

  projectRoute(app, ctx, "POST", "/runs", async (p, req, reply) => {
    const { name } = parseWith(NameBody, req.body);
    const r = await pool.query(
      "INSERT INTO runs (project_id, name) VALUES ($1, $2) RETURNING id, name, last_seq, created_at, updated_at",
      [p.projectId, name],
    );
    reply.code(201);
    return r.rows[0];
  });

  projectRoute(app, ctx, "GET", "/runs/:id", async (p, req) => {
    const r = await pool.query(
      "SELECT id, name, last_seq, created_at, updated_at FROM runs WHERE id = $1 AND project_id = $2",
      [idParam(req), p.projectId],
    );
    if (r.rowCount !== 1) throw new HttpError(404, "not found");
    return r.rows[0];
  });

  projectRoute(app, ctx, "POST", "/runs/:id/metrics", async (p, req) => {
    const runId = idParam(req);
    const { events } = parseWith(batchSchema(ctx.config.maxBatch), req.body);
    return ingestBatch(ctx, p.projectId, runId, events);
  });

  const PageQuery = z
    .object({
      after_seq: z.string().regex(/^\d{1,15}$/).default("0"),
      limit: z.string().regex(/^\d{1,4}$/).default("1000"),
    })
    .strict();

  projectRoute(app, ctx, "GET", "/runs/:id/metrics", async (p, req) => {
    const runId = idParam(req);
    const q = parseWith(PageQuery, req.query);
    const limit = Math.min(Math.max(Number(q.limit), 1), 5000);
    // One statement = one snapshot: last_seq and the page cannot disagree.
    const r = await pool.query(
      `SELECT r.last_seq,
              COALESCE((SELECT json_agg(p) FROM (
                 SELECT key, step, value, seq FROM metrics
                  WHERE run_id = r.id AND seq > $3 ORDER BY seq LIMIT $4) p), '[]'::json) AS points
         FROM runs r WHERE r.id = $1 AND r.project_id = $2`,
      [runId, p.projectId, Number(q.after_seq), limit],
    );
    if (r.rowCount !== 1) throw new HttpError(404, "not found");
    return { last_seq: r.rows[0].last_seq, points: r.rows[0].points };
  });

  projectRoute(app, ctx, "GET", "/runs/:id/stream", async (p, req, reply) => {
    const runId = idParam(req);
    const fromHeader = req.headers["last-event-id"];
    const fromQuery = (req.query as { after_seq?: unknown }).after_seq;
    const cursor = parseCursor(fromHeader !== undefined ? fromHeader : fromQuery);
    await serveStream(ctx, p, runId, cursor, reply);
  });
}
