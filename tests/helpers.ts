import pg from "pg";
import { buildApp, type BuiltApp } from "../src/app.js";
import { loadConfig, type Config } from "../src/config.js";
import type { Faults } from "../src/http.js";
import { openStream } from "../public/sse.js";
import { applyPoints, createState, resetState, sortPoints } from "../public/state.js";

export const ADMIN_TOKEN = "test-admin-token";

export interface Reply {
  status: number;
  text: string;
  json: any;
  headers: Headers;
}

export interface TestApp {
  base: string;
  built: BuiltApp;
  /** Every structured log line the server wrote. */
  logs: string[];
  /** Direct database access: the tests' independent source of truth. */
  db: pg.Pool;
  call: (key: string | null, method: string, path: string, body?: unknown, headers?: Record<string, string>) => Promise<Reply>;
  project: (name: string) => Promise<{ projectId: string; key: string }>;
  run: (key: string, name: string) => Promise<string>;
  close: () => Promise<void>;
}

export async function startApp(overrides: Partial<Config> = {}, faults: Faults = {}): Promise<TestApp> {
  const logs: string[] = [];
  const config = loadConfig({ adminToken: ADMIN_TOKEN, ...overrides });
  const built = await buildApp({ config, logSink: (l) => logs.push(l), faults });
  await built.app.listen({ port: 0, host: "127.0.0.1" });
  const addr = built.app.server.address() as { port: number };
  const base = `http://127.0.0.1:${addr.port}`;
  const db = new pg.Pool({ connectionString: config.databaseUrl, max: 4 });

  const call: TestApp["call"] = async (key, method, path, body, headers = {}) => {
    const res = await fetch(base + path, {
      method,
      headers: {
        ...(key ? { authorization: `Bearer ${key}` } : {}),
        ...(body !== undefined ? { "content-type": "application/json" } : {}),
        ...headers,
      },
      body: body === undefined ? undefined : typeof body === "string" ? body : JSON.stringify(body),
    });
    const text = await res.text();
    let json: any;
    try {
      json = JSON.parse(text);
    } catch {
      json = undefined;
    }
    return { status: res.status, text, json, headers: res.headers };
  };

  return {
    base,
    built,
    logs,
    db,
    call,
    project: async (name) => {
      const r = await call(null, "POST", "/admin/projects", { name }, { authorization: `Bearer ${ADMIN_TOKEN}` });
      if (r.status !== 201) throw new Error(`create project failed: ${r.status} ${r.text}`);
      return { projectId: r.json.project.id, key: r.json.api_key };
    },
    run: async (key, name) => {
      const r = await call(key, "POST", "/runs", { name });
      if (r.status !== 201) throw new Error(`create run failed: ${r.status} ${r.text}`);
      return r.json.id;
    },
    close: async () => {
      await db.end();
      await built.close();
    },
  };
}

export async function waitUntil(cond: () => boolean | Promise<boolean>, timeoutMs: number, label: string) {
  const start = Date.now();
  while (!(await cond())) {
    if (Date.now() - start > timeoutMs) throw new Error(`timed out after ${timeoutMs}ms waiting for: ${label}`);
    await new Promise((r) => setTimeout(r, 15));
  }
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** What is actually stored, read straight from Postgres (not through the API). */
export async function dbPoints(db: pg.Pool, runId: string) {
  const r = await db.query("SELECT key, step, value FROM metrics WHERE run_id = $1", [runId]);
  return sortPoints(r.rows.map((x) => ({ key: x.key as string, step: Number(x.step), value: x.value as number })));
}

export async function dbHead(db: pg.Pool, runId: string) {
  const r = await db.query(
    `SELECT r.last_seq, count(m.*)::int AS n, COALESCE(min(m.seq), 0) AS lo, COALESCE(max(m.seq), 0) AS hi
       FROM runs r LEFT JOIN metrics m ON m.run_id = r.id WHERE r.id = $1 GROUP BY r.id`,
    [runId],
  );
  return {
    lastSeq: Number(r.rows[0].last_seq),
    count: Number(r.rows[0].n),
    minSeq: Number(r.rows[0].lo),
    maxSeq: Number(r.rows[0].hi),
  };
}

/** A stream consumer running the SAME client code as the web page, in Node. */
export function consume(base: string, apiKey: string, runId: string, state = createState()) {
  const statuses: string[] = [];
  let syncedSeq = -1;
  let connects = 0;
  const handle = openStream({
    url: `${base}/runs/${runId}/stream`,
    apiKey,
    getCursor: () => state.lastSeq,
    onPoints: (points: any[]) => applyPoints(state, points),
    onSync: (seq: number) => {
      if (seq !== state.lastSeq) return false;
      syncedSeq = seq;
      return true;
    },
    onReset: () => resetState(state),
    onStatus: (s: string) => {
      statuses.push(s);
      if (s === "open") connects++;
      else syncedSeq = -1;
    },
    retryMinMs: 10,
    retryMaxMs: 60,
    idleTimeoutMs: 5000,
  });
  return {
    state,
    statuses,
    handle,
    get connects() {
      return connects;
    },
    get status() {
      return statuses[statuses.length - 1];
    },
    waitSynced: (seq: number, timeoutMs = 15000) =>
      waitUntil(() => syncedSeq === seq && state.lastSeq === seq, timeoutMs, `stream synced at seq ${seq}`),
    close: () => handle.close(),
  };
}

export interface Frame {
  event: string;
  id?: string;
  data: string;
}

/** Raw SSE read without the client library: frames up to and including the first sync. */
export async function readFrames(base: string, apiKey: string, runId: string, lastEventId?: string): Promise<Frame[]> {
  const ctrl = new AbortController();
  const res = await fetch(`${base}/runs/${runId}/stream`, {
    headers: { authorization: `Bearer ${apiKey}`, ...(lastEventId !== undefined ? { "last-event-id": lastEventId } : {}) },
    signal: ctrl.signal,
  });
  if (res.status !== 200) throw new Error(`stream ${res.status}`);
  const frames: Frame[] = [];
  const reader = res.body!.getReader();
  const dec = new TextDecoder();
  let buf = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return frames;
      buf += dec.decode(value, { stream: true });
      let end;
      while ((end = buf.indexOf("\n\n")) >= 0) {
        const f: Frame = { event: "message", data: "" };
        for (const line of buf.slice(0, end).split("\n")) {
          if (line.startsWith("event:")) f.event = line.slice(6).trim();
          else if (line.startsWith("id:")) f.id = line.slice(3).trim();
          else if (line.startsWith("data:")) f.data += line.slice(5).trim();
        }
        buf = buf.slice(end + 2);
        frames.push(f);
        if (f.event === "sync" || f.event === "revoked") return frames;
      }
    }
  } finally {
    ctrl.abort();
  }
}
