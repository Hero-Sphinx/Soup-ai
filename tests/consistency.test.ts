// Requirement 2 (API half): metrics arrive out of order, duplicated and late,
// while the live connection is dropped again and again mid-stream. At the end
// the consumer's state must equal the database, and the database must equal
// exactly what the client meant to send.
import { afterAll, beforeAll, expect, it } from "vitest";
import { toPoints, sortPoints } from "../public/state.js";
import { makeEvents, mulberry32, sendUnreliably } from "../scripts/lib/flaky-client.js";
import { consume, dbHead, dbPoints, sleep, startApp, waitUntil, type TestApp } from "./helpers.js";

const SEED = Number(process.env.SEED ?? 20261111);
let t: TestApp;

beforeAll(async () => {
  t = await startApp({ streamPage: 37 }); // small odd page: many frames, many cut points
});
afterAll(() => t.close());

it("API: chaotic ingest + repeated stream drops => stream state == database == what was sent", async () => {
  const { key } = await t.project("consistency-api");
  const runId = await t.run(key, "chaos");
  const rng = mulberry32(SEED);
  const events = makeEvents(["loss", "acc", "lr"], 2000, rng); // 6000 points

  const early = consume(t.base, key, runId); // connected before the first event
  let kills = 0;
  const sizes: number[] = [];
  const killer = setInterval(() => {
    const n = t.built.ctx.streams.killAll();
    sizes.push(n);
    kills += n;
  }, 60);

  const sending = sendUnreliably({ baseUrl: t.base, apiKey: key, runId, events, rng, paceMs: 30 });
  await sleep(400);
  const lateJoiner = consume(t.base, key, runId); // joins mid-run: must catch up
  const stats = await sending;
  clearInterval(killer);

  const head = await dbHead(t.db, runId);
  await early.waitSynced(head.lastSeq);
  await lateJoiner.waitSynced(head.lastSeq);
  const stored = await dbPoints(t.db, runId);

  console.log(
    [
      `seed=${SEED}`,
      `sent: ${stats.events} unique events in ${stats.batches} batches, ${stats.requests} HTTP requests`,
      `chaos: ${stats.lostRequests} lost requests, ${stats.lostResponses} lost responses, ${stats.aborted} aborted mid-flight, ` +
        `${stats.staleResends} stale re-sends, ${stats.lateEvents} late events`,
      `server acks: accepted=${stats.accepted} duplicates=${stats.duplicates} conflicts=${stats.conflicts}`,
      `stream: killed ${kills} connections; early consumer connected ${early.connects}x, late joiner ${lateJoiner.connects}x`,
      `stream replays ignored: early=${early.state.replays} late=${lateJoiner.state.replays}; gaps: early=${early.state.gaps} late=${lateJoiner.state.gaps}`,
      `db: rows=${head.count} last_seq=${head.lastSeq} seq range=${head.minSeq}..${head.maxSeq}`,
    ].join("\n"),
  );

  // the chaos really happened
  expect(kills).toBeGreaterThan(5);
  // A dropped stream must leave the server's registry (this caught a real
  // leak: the count used to grow without bound). 2 consumers, each may have
  // one live stream plus one killed stream still finishing its last query.
  expect(Math.max(...sizes)).toBeLessThanOrEqual(4);
  expect(early.connects).toBeGreaterThan(3);
  expect(stats.lostResponses + stats.aborted + stats.staleResends).toBeGreaterThan(10);
  expect(stats.duplicates).toBeGreaterThan(0);

  // database == what was sent, exactly once
  expect(stored).toEqual(sortPoints([...events]));
  expect(stats.conflicts).toBe(0);
  // the log is contiguous: 1..N with N == number of rows
  expect(head).toEqual({ lastSeq: events.length, count: events.length, minSeq: 1, maxSeq: events.length });

  // both consumers == database
  expect(toPoints(early.state)).toEqual(stored);
  expect(toPoints(lateJoiner.state)).toEqual(stored);
  expect(early.state.gaps).toBe(0);
  expect(lateJoiner.state.gaps).toBe(0);

  // and the paginated REST read agrees too
  const viaApi: any[] = [];
  for (let after = 0; ; ) {
    const page = await t.call(key, "GET", `/runs/${runId}/metrics?after_seq=${after}&limit=1000`);
    expect(page.status).toBe(200);
    if (page.json.points.length === 0) break;
    viaApi.push(...page.json.points);
    after = page.json.points.at(-1).seq;
  }
  expect(viaApi.map((p) => p.seq)).toEqual(Array.from({ length: events.length }, (_, i) => i + 1));
  expect(sortPoints(viaApi.map(({ key, step, value }) => ({ key, step, value })))).toEqual(stored);

  early.close();
  lateJoiner.close();
  await waitUntil(() => t.built.ctx.streams.size === 0, 2000, "server forgot both closed streams");
});
