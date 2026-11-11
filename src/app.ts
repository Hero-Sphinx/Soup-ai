import { fileURLToPath } from "node:url";
import fastifyStatic from "@fastify/static";
import Fastify, { type FastifyInstance } from "fastify";
import type { Config } from "./config.js";
import { applySchema, createPool } from "./db.js";
import { HttpError, type Ctx, type Faults, type RequestWithPrincipal } from "./http.js";
import { Hub } from "./notify.js";
import { registerRoutes } from "./routes.js";
import { SECURITY_HEADERS, StreamRegistry } from "./stream.js";

export interface BuiltApp {
  app: FastifyInstance;
  ctx: Ctx;
  close: () => Promise<void>;
}

const CLIENT_ERRORS: Record<number, string> = {
  400: "bad request",
  404: "not found",
  413: "payload too large",
  415: "unsupported media type",
};

const UUID = /^[0-9a-f-]{36}$/;

export async function buildApp(opts: {
  config: Config;
  /** Where structured log lines go. Tests capture them to prove no secret is logged. */
  logSink?: (line: string) => void;
  faults?: Faults;
}): Promise<BuiltApp> {
  const { config } = opts;
  const sink = opts.logSink ?? ((line: string) => process.stdout.write(line + "\n"));
  const log = (entry: Record<string, unknown>) => sink(JSON.stringify({ t: new Date().toISOString(), ...entry }));

  const pool = createPool(config.databaseUrl);
  await applySchema(pool);
  const hub = new Hub(config.databaseUrl, (msg) => log({ level: "warn", msg }));
  await hub.start();

  const ctx: Ctx = { config, pool, hub, log, streams: new StreamRegistry(), faults: opts.faults ?? {}, routes: [] };

  const app = Fastify({ logger: false, bodyLimit: config.bodyLimit, exposeHeadRoutes: false });

  // JSON only. Fastify also parses text/plain by default; one input format
  // means one validation path.
  app.removeContentTypeParser("text/plain");

  app.addHook("onRequest", async (_req, reply) => {
    reply.headers(SECURITY_HEADERS);
  });

  // Access log. Deliberately logs the route PATTERN, never the raw URL,
  // headers or body: an API key can therefore not end up here.
  app.addHook("onResponse", async (req, reply) => {
    const principal = (req as RequestWithPrincipal).principal;
    const id = (req.params as { id?: string } | undefined)?.id;
    log({
      msg: "request",
      method: req.method,
      route: req.routeOptions?.url ?? "unmatched",
      status: reply.statusCode,
      ms: Math.round(reply.elapsedTime),
      project_id: principal?.projectId,
      key_id: principal?.keyId,
      run_id: principal && id && UUID.test(id) ? id : undefined,
    });
  });

  app.setErrorHandler((err: unknown, req, reply) => {
    if (err instanceof HttpError) {
      return reply.code(err.status).send(err.details ? { error: err.message, details: err.details } : { error: err.message });
    }
    const status = (err as { statusCode?: number }).statusCode;
    if (typeof status === "number" && status >= 400 && status < 500) {
      // Framework-level client errors (bad JSON, body too large...): fixed text only.
      return reply.code(status).send({ error: CLIENT_ERRORS[status] ?? "bad request" });
    }
    // Details stay in the server log; the caller gets nothing to learn from.
    const e = err as { message?: string; code?: string };
    log({ level: "error", msg: "unhandled", route: req.routeOptions?.url, error: e?.message, code: e?.code });
    return reply.code(500).send({ error: "internal error" });
  });

  app.setNotFoundHandler((_req, reply) => reply.code(404).send({ error: "not found" }));

  registerRoutes(app, ctx);

  await app.register(fastifyStatic, {
    root: fileURLToPath(new URL("../public", import.meta.url)),
    cacheControl: false,
  });

  return {
    app,
    ctx,
    close: async () => {
      ctx.streams.killAll();
      await app.close();
      await hub.stop();
      await pool.end();
    },
  };
}
