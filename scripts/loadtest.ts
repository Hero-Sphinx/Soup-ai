// Evidence for the verdict: RUNS concurrent runs, each producing EPS events
// per second, each watched by one live stream. Measures ingest latency,
// end-to-end latency (client timestamp -> point received on the stream) and
// then checks every viewer against the database.
//
//   RUNS=50 EPS=10 SECONDS=30 npm run loadtest
// The server runs in its OWN process (spawned here, or BASE_URL if given), so
// the load generator's CPU is not billed to it.
import { spawn } from "node:child_process";
import pg from "pg";
import { openStream } from "../public/sse.js";
import { applyPoints, createState, toPoints, sortPoints } from "../public/state.js";

const RUNS = Number(process.env.RUNS ?? 50);
const EPS = Number(process.env.EPS ?? 10);
const SECONDS = Number(process.env.SECONDS ?? 30);
const BATCH_MS = Number(process.env.BATCH_MS ?? 200); // client flush interval
const PROJECTS = 5;

const adminToken = process.env.ADMIN_TOKEN ?? "loadtest-admin-token";
const databaseUrl = process.env.DATABASE_URL!;
let base = process.env.BASE_URL ?? "";
let server: ReturnType<typeof spawn> | null = null;
if (!base) {
  const port = 18080;
  // one plain node process (no shell, no wrapper), so kill() really stops it
  server = spawn(process.execPath, ["--import", "tsx", "src/server.ts"], {
    stdio: ["ignore", "ignore", "inherit"],
    env: { ...process.env, PORT: String(port), ADMIN_TOKEN: adminToken },
  });
  base = `http://127.0.0.1:${port}`;
  for (let i = 0; ; i++) {
    if (await fetch(`${base}/health`).then((r) => r.ok, () => false)) break;
    if (i > 100) throw new Error("server did not start");
    await new Promise((r) => setTimeout(r, 200));
  }
}
const db = new pg.Pool({ connectionString: databaseUrl, max: 2 });
const stats = async () =>
  (await (await fetch(`${base}/admin/stats`, { headers: { authorization: `Bearer ${adminToken}` } })).json()) as any;

const post = async (path: string, token: string, body: unknown) => {
  const res = await fetch(base + path, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: res.status, json: (await res.json()) as any };
};

const pct = (xs: number[], p: number) => (xs.length ? xs[Math.min(xs.length - 1, Math.floor((p / 100) * xs.length))]! : NaN);
const summary = (xs: number[]) => {
  xs.sort((a, b) => a - b);
  return `p50 ${pct(xs, 50).toFixed(1)}  p95 ${pct(xs, 95).toFixed(1)}  p99 ${pct(xs, 99).toFixed(1)}  max ${(xs.at(-1) ?? NaN).toFixed(1)} ms`;
};

const keys: string[] = [];
for (let i = 0; i < PROJECTS; i++) {
  keys.push((await post("/admin/projects", adminToken, { name: `load-${i}` })).json.api_key);
}

interface Sim {
  runId: string;
  key: string;
  sent: { key: string; step: number; value: number }[];
  state: ReturnType<typeof createState>;
  close: () => void;
}
const sims: Sim[] = [];
const ingestMs: number[] = [];
const e2eMs: number[] = [];
const sentAt = new Map<string, number>(); // `${run}|${step}` -> ms
let errors = 0;
let requests = 0;

for (let i = 0; i < RUNS; i++) {
  const key = keys[i % PROJECTS]!;
  const runId = (await post("/runs", key, { name: `load-run-${i}` })).json.id as string;
  const state = createState();
  const handle = openStream({
    url: `${base}/runs/${runId}/stream`,
    apiKey: key,
    getCursor: () => state.lastSeq,
    onPoints: (points: any[]) => {
      const now = performance.now();
      for (const p of points) {
        const t0 = sentAt.get(`${runId}|${p.step}`);
        if (t0 !== undefined) e2eMs.push(now - t0);
      }
      return applyPoints(state, points);
    },
    onSync: (seq: number) => seq === state.lastSeq,
  });
  sims.push({ runId, key, sent: [], state, close: () => handle.close() });
}

console.log(`load: ${RUNS} runs x ${EPS} events/s = ${RUNS * EPS} events/s for ${SECONDS}s, ${RUNS} live viewers, client flush every ${BATCH_MS} ms`);
const before = await stats();
const t0 = performance.now();
const perFlush = (EPS * BATCH_MS) / 1000;

await Promise.all(
  sims.map(async (sim, idx) => {
    await new Promise((r) => setTimeout(r, (idx * BATCH_MS) / RUNS)); // spread clients over the interval
    let step = 0;
    let owed = 0;
    const inflight: Promise<void>[] = [];
    while (performance.now() - t0 < SECONDS * 1000) {
      const tick = performance.now();
      owed += perFlush;
      const events = [];
      while (owed >= 1) {
        owed -= 1;
        const e = { key: "loss", step: step++, value: Math.round(Math.random() * 1e6) / 1e6 };
        events.push(e);
        sim.sent.push(e);
        sentAt.set(`${sim.runId}|${e.step}`, tick);
      }
      if (events.length) {
        // open loop: a slow server does not slow the client down, it piles up
        inflight.push(
          (async () => {
            const s = performance.now();
            requests++;
            const r = await post(`/runs/${sim.runId}/metrics`, sim.key, { events }).catch(() => ({ status: 0, json: {} }));
            ingestMs.push(performance.now() - s);
            if (r.status !== 200 || r.json.accepted !== events.length) errors++;
          })(),
        );
      }
      await new Promise((r) => setTimeout(r, Math.max(0, BATCH_MS - (performance.now() - tick))));
    }
    await Promise.all(inflight);
  }),
);
const elapsed = (performance.now() - t0) / 1000;
const after = await stats();

// let viewers drain, then compare each one with the database
const deadline = Date.now() + 15000;
while (Date.now() < deadline && sims.some((s) => s.state.lastSeq < s.sent.length)) await new Promise((r) => setTimeout(r, 50));

let mismatched = 0;
let totalSent = 0;
let gaps = 0;
for (const sim of sims) {
  totalSent += sim.sent.length;
  gaps += sim.state.gaps;
  const rows = await db.query("SELECT key, step, value FROM metrics WHERE run_id = $1", [sim.runId]);
  const stored = JSON.stringify(sortPoints(rows.rows.map((r) => ({ key: r.key, step: Number(r.step), value: r.value }))));
  if (stored !== JSON.stringify(toPoints(sim.state)) || stored !== JSON.stringify(sortPoints([...sim.sent]))) mismatched++;
  sim.close();
}

console.log(`sent:            ${totalSent} events in ${requests} requests over ${elapsed.toFixed(1)}s = ${(totalSent / elapsed).toFixed(0)} events/s achieved`);
console.log(`ingest latency:  ${summary(ingestMs)}`);
console.log(`end-to-end:      ${summary(e2eMs)}   (client sends point -> viewer receives it)`);
console.log(`failed requests: ${errors}`);
console.log(`viewers:         ${RUNS - mismatched}/${RUNS} identical to database and to what was sent; seq gaps seen: ${gaps}`);
console.log(`server process:  cpu ${(((after.cpu_seconds - before.cpu_seconds) / elapsed) * 100).toFixed(0)}% of one core, rss ${before.rss_mb} -> ${after.rss_mb} MB, event-loop delay p50 ${after.event_loop_delay_ms.p50.toFixed(1)} / p99 ${after.event_loop_delay_ms.p99.toFixed(1)} / max ${after.event_loop_delay_ms.max.toFixed(1)} ms`);
console.log(`db pool at end of load: ${JSON.stringify(after.db_pool)}; streams: ${JSON.stringify(after.streams)}`);

await db.end();
server?.kill();
process.exit(mismatched === 0 && errors === 0 ? 0 : 1);
