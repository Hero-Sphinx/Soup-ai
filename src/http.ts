import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import type { FastifyInstance, FastifyReply, FastifyRequest } from "fastify";
import type { z } from "zod";
import type { Config } from "./config.js";
import type { Pool } from "./db.js";
import type { Hub } from "./notify.js";
import type { StreamRegistry } from "./stream.js";

/** Test-only fault injection points. Empty in production. */
export interface Faults {
  /** Called inside the ingest transaction after rows are inserted, before commit. */
  afterInsert?: () => void | Promise<void>;
}

export interface Ctx {
  config: Config;
  pool: Pool;
  hub: Hub;
  log: (entry: Record<string, unknown>) => void;
  streams: StreamRegistry;
  faults: Faults;
  /** Every registered route and how it is authenticated (read by the architecture test). */
  routes: { method: string; url: string; auth: "project" | "admin" | "public" }[];
}

export class HttpError extends Error {
  constructor(
    public status: number,
    message: string,
    public details?: unknown,
  ) {
    super(message);
  }
}

/** The caller, derived only from the API key. Never from the URL or body. */
export interface Principal {
  projectId: string;
  keyId: string;
}

/** Routes reachable without credentials. Reviewed list; keep it tiny. */
export const PUBLIC_ROUTES = ["GET /health"];

const sha256 = (s: string) => createHash("sha256").update(s).digest();

export function newApiKey(): { key: string; hash: string } {
  const key = "sk_" + randomBytes(32).toString("base64url");
  return { key, hash: sha256(key).toString("hex") };
}

const BEARER = /^Bearer (sk_[A-Za-z0-9_-]{20,100})$/;

async function authenticate(ctx: Ctx, req: FastifyRequest): Promise<Principal> {
  const m = BEARER.exec(req.headers.authorization ?? "");
  if (!m) throw new HttpError(401, "unauthorized");
  // No cache in front of this lookup on purpose: a revoked key must stop
  // working on the very next request.
  const r = await ctx.pool.query(
    "SELECT id, project_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
    [sha256(m[1]!).toString("hex")],
  );
  if (r.rowCount !== 1) throw new HttpError(401, "unauthorized");
  return { projectId: r.rows[0].project_id, keyId: r.rows[0].id };
}

function requireAdmin(ctx: Ctx, req: FastifyRequest): void {
  const given = sha256(req.headers.authorization ?? "");
  const want = sha256("Bearer " + ctx.config.adminToken);
  if (!timingSafeEqual(given, want)) throw new HttpError(401, "unauthorized");
}

type Method = "GET" | "POST";
type Handler<P> = (principal: P, req: FastifyRequest, reply: FastifyReply) => Promise<unknown>;

/** The ONLY ways to register a route. Each one states its auth mode. */
export function projectRoute(app: FastifyInstance, ctx: Ctx, method: Method, url: string, handler: Handler<Principal>) {
  ctx.routes.push({ method, url, auth: "project" });
  app.route({
    method,
    url,
    handler: async (req, reply) => {
      const principal = await authenticate(ctx, req);
      (req as RequestWithPrincipal).principal = principal;
      return handler(principal, req, reply);
    },
  });
}

export function adminRoute(app: FastifyInstance, ctx: Ctx, method: Method, url: string, handler: Handler<null>) {
  ctx.routes.push({ method, url, auth: "admin" });
  app.route({
    method,
    url,
    handler: async (req, reply) => {
      requireAdmin(ctx, req);
      return handler(null, req, reply);
    },
  });
}

export function publicRoute(app: FastifyInstance, ctx: Ctx, method: Method, url: string, handler: Handler<null>) {
  ctx.routes.push({ method, url, auth: "public" });
  app.route({ method, url, handler: (req, reply) => handler(null, req, reply) });
}

export type RequestWithPrincipal = FastifyRequest & { principal?: Principal };

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/**
 * A malformed id gets the same 404 as an unknown id or another project's id,
 * and never reaches Postgres (whose uuid parse error would otherwise surface).
 */
export function parseId(raw: unknown): string {
  if (typeof raw !== "string" || !UUID.test(raw)) throw new HttpError(404, "not found");
  return raw;
}

export function parseWith<T extends z.ZodTypeAny>(schema: T, data: unknown): z.infer<T> {
  const r = schema.safeParse(data);
  if (!r.success) {
    // Only describes the caller's own input: path + reason, no stored data.
    const details = r.error.issues.slice(0, 10).map((i) => ({ path: i.path.join("."), message: i.message }));
    throw new HttpError(400, "invalid request", details);
  }
  return r.data;
}
