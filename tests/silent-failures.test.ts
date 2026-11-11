// Requirement 4: failures that produce no error anywhere unless you look.
// One describe block per failure named in the task.
import http from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { openStream } from "../public/sse.js";
import { applyPoints, createState, toPoints } from "../public/state.js";
import { consume, dbHead, dbPoints, readFrames, sleep, startApp, waitUntil, type TestApp } from "./helpers.js";

const series = (key: string, n: number, from = 0) =>
  Array.from({ length: n }, (_, i) => ({ key, step: from + i, value: (from + i) / 8 }));

let t: TestApp;
let failInsert = 0;

beforeAll(async () => {
  t = await startApp(
    { streamPollMs: 300, streamStallMs: 1200 },
    {
      afterInsert: () => {
        if (failInsert > 0) {
          failInsert--;
          throw new Error("injected failure between INSERT and COMMIT");
        }
      },
    },
  );
});
afterAll(() => t.close());

const post = (key: string, runId: string, events: unknown[]) => t.call(key, "POST", `/runs/${runId}/metrics`, { events });

describe("events lost on reconnect", () => {
  it("a client that was offline resumes at its cursor: gets exactly the missed points, none twice", async () => {
    const { key } = await t.project("reconnect");
    const runId = await t.run(key, "r");
    await post(key, runId, series("loss", 200));

    const first = consume(t.base, key, runId);
    await first.waitSynced(200);
    first.close(); // offline

    await post(key, runId, series("loss", 300, 200).reverse()); // arrives while offline, out of order
    await post(key, runId, series("acc", 200));

    const again = consume(t.base, key, runId, first.state); // same state => same cursor
    await again.waitSynced(700);
    expect(again.state.replays).toBe(0); // nothing re-delivered
    expect(again.state.gaps).toBe(0);
    expect(toPoints(again.state)).toEqual(await dbPoints(t.db, runId));
    again.close();

    // and on the wire: Last-Event-ID: 200 => the first point sent is seq 201
    const frames = await readFrames(t.base, key, runId, "200");
    const firstPoints = JSON.parse(frames.find((f) => f.event === "points")!.data);
    expect(firstPoints[0].seq).toBe(201);
    expect(frames.at(-1)).toMatchObject({ event: "sync", data: '{"seq":700}' });
  });

  it("a connection cut in the middle of a frame loses nothing (client asks again from its applied cursor)", async () => {
    const points = [1, 2, 3, 4].map((seq) => ({ key: "k", step: seq, value: seq, seq }));
    const cursorsSeen: string[] = [];
    const server = http.createServer((req, res) => {
      cursorsSeen.push(String(req.headers["last-event-id"]));
      res.writeHead(200, { "content-type": "text/event-stream" });
      if (cursorsSeen.length === 1) {
        // one complete frame, then half of the next one, then the link dies
        res.write(`id: 2\nevent: points\ndata: ${JSON.stringify(points.slice(0, 2))}\n\n`);
        res.write(`id: 4\nevent: points\ndata: ${JSON.stringify(points.slice(2)).slice(0, 30)}`);
        setTimeout(() => res.destroy(), 30);
      } else {
        const from = Number(req.headers["last-event-id"]);
        res.write(`id: 4\nevent: points\ndata: ${JSON.stringify(points.filter((p) => p.seq > from))}\n\n`);
        res.write(`event: sync\ndata: {"seq":4}\n\n`);
      }
    });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    const state = createState();
    let synced = false;
    const handle = openStream({
      url: `http://127.0.0.1:${(server.address() as AddressInfo).port}/x`,
      apiKey: "sk_test",
      getCursor: () => state.lastSeq,
      onPoints: (p: any[]) => applyPoints(state, p),
      onSync: (seq: number) => (synced = seq === state.lastSeq),
      retryMinMs: 10,
    });
    await waitUntil(() => synced, 5000, "resynced after mid-frame cut");
    handle.close();
    server.closeAllConnections();
    server.close();
    expect(cursorsSeen).toEqual(["0", "2"]); // resumed after the last COMPLETE frame
    expect(state).toMatchObject({ lastSeq: 4, count: 4, gaps: 0, replays: 0 });
  });

  it("the client-side tripwire: a skipped seq is detected, counted, and not applied", () => {
    const state = createState();
    expect(applyPoints(state, [{ key: "k", step: 1, value: 1, seq: 1 }])).toBe(true);
    expect(applyPoints(state, [{ key: "k", step: 3, value: 3, seq: 3 }])).toBe(false);
    expect(state).toMatchObject({ lastSeq: 1, count: 1, gaps: 1 });
  });

  it("a missed NOTIFY does not lose or stall data: streams fall back to re-reading the log", async () => {
    const { key } = await t.project("lost-notify");
    const runId = await t.run(key, "r");
    const c = consume(t.base, key, runId);
    await c.waitSynced(0);

    // kill the server's LISTEN connection, then write while nobody is listening
    const pid = t.built.ctx.hub.listenerPid;
    expect(pid).not.toBeNull();
    await t.db.query("SELECT pg_terminate_backend($1)", [pid]);
    await waitUntil(() => t.built.ctx.hub.listenerPid === null, 3000, "listener noticed it is dead");
    const started = Date.now();
    await post(key, runId, series("loss", 25));
    await c.waitSynced(25, 3000);
    const viaPoll = Date.now() - started;

    // the listener heals itself and push delivery is back
    await waitUntil(() => t.built.ctx.hub.listenerPid !== null, 5000, "listener reconnected");
    const again = Date.now();
    await post(key, runId, series("loss", 25, 25));
    await c.waitSynced(50, 3000);
    console.log(`delivery with LISTEN connection dead: ${viaPoll} ms (poll interval 300 ms); after it healed: ${Date.now() - again} ms`);
    expect(t.logs.some((l) => l.includes("listen connection lost"))).toBe(true); // and it was not silent
    c.close();
  });

  it("a client whose cursor is ahead of the log is told to reset instead of silently showing stale data", async () => {
    const { key } = await t.project("reset");
    const runId = await t.run(key, "r");
    await post(key, runId, series("loss", 3));
    const frames = await readFrames(t.base, key, runId, "500");
    expect(frames.map((f) => f.event)).toEqual(["reset", "points", "sync"]);
  });
});

describe("duplicates", () => {
  it("the same batch sent 12 times (8 of them concurrently) is stored exactly once", async () => {
    const { key } = await t.project("dupes");
    const runId = await t.run(key, "r");
    const batch = series("loss", 200);
    const replies = await Promise.all(Array.from({ length: 8 }, () => post(key, runId, batch)));
    for (let i = 0; i < 4; i++) replies.push(await post(key, runId, batch));
    expect(replies.every((r) => r.status === 200)).toBe(true);
    expect(replies.reduce((n, r) => n + r.json.accepted, 0)).toBe(200);
    expect(replies.reduce((n, r) => n + r.json.duplicates, 0)).toBe(200 * 11);
    expect(await dbHead(t.db, runId)).toEqual({ lastSeq: 200, count: 200, minSeq: 1, maxSeq: 200 });
  });

  it("a point repeated inside one batch is stored once", async () => {
    const { key } = await t.project("dupes-in-batch");
    const runId = await t.run(key, "r");
    const p = { key: "loss", step: 1, value: 0.5 };
    const r = await post(key, runId, [p, p, { ...p, step: 2 }, p]);
    expect(r.json).toMatchObject({ accepted: 2, duplicates: 2, conflicts: [], last_seq: 2 });
  });

  it("a re-send with a DIFFERENT value is not silently dropped or overwritten: first write wins and the conflict is reported and recorded", async () => {
    const { key } = await t.project("conflict");
    const runId = await t.run(key, "r");
    await post(key, runId, [{ key: "loss", step: 7, value: 0.25 }]);
    const r = await post(key, runId, [
      { key: "loss", step: 7, value: 0.99 },
      { key: "loss", step: 8, value: 0.2 },
    ]);
    expect(r.status).toBe(200);
    expect(r.json).toMatchObject({ accepted: 1, duplicates: 0, conflicts: [{ key: "loss", step: 7, stored: 0.25, rejected: 0.99 }] });
    expect(await dbPoints(t.db, runId)).toEqual([
      { key: "loss", step: 7, value: 0.25 },
      { key: "loss", step: 8, value: 0.2 },
    ]);
    const rec = await t.db.query("SELECT stored_value, rejected_value FROM metric_conflicts WHERE run_id = $1", [runId]);
    expect(rec.rows).toEqual([{ stored_value: 0.25, rejected_value: 0.99 }]);
  });

  it("20 concurrent writers into one run: seq is exactly 1..N with no holes and no repeats", async () => {
    const { key } = await t.project("seq");
    const runId = await t.run(key, "r");
    const replies = await Promise.all(Array.from({ length: 20 }, (_, w) => post(key, runId, series(`m${w}`, 100))));
    expect(replies.every((r) => r.status === 200 && r.json.accepted === 100)).toBe(true);
    const seqs = await t.db.query("SELECT seq FROM metrics WHERE run_id = $1 ORDER BY seq", [runId]);
    expect(seqs.rows.map((r) => Number(r.seq))).toEqual(Array.from({ length: 2000 }, (_, i) => i + 1));
    expect((await dbHead(t.db, runId)).lastSeq).toBe(2000);
  });
});

describe("queue overflow for a slow client", () => {
  it("a client that stops reading costs the server one page of memory, is cut off, and can resume without loss", async () => {
    const { key } = await t.project("slow");
    const runId = await t.run(key, "r");
    const TOTAL = 60_000; // ~3.5 MB of SSE, far more than any socket buffer
    for (let i = 0; i < TOTAL; i += 1000) {
      expect((await post(key, runId, series("loss", 1000, i))).status).toBe(200);
    }
    const before = { ...t.built.ctx.streams.stats };

    // a client that connects and then never reads a byte of the body
    const stuck = http.get(`${t.base}/runs/${runId}/stream`, { headers: { authorization: `Bearer ${key}` } });
    stuck.on("error", () => {});
    // (without a 'response' listener Node would silently discard the body, i.e. read it)
    stuck.on("response", (res) => res.pause());
    await waitUntil(() => t.built.ctx.streams.size === 1, 3000, "stuck client connected");
    await waitUntil(() => t.built.ctx.streams.stats.closedStalled === before.closedStalled + 1, 8000, "server cut off the stalled client");
    await waitUntil(() => t.built.ctx.streams.size === 0, 2000, "stalled stream removed");
    stuck.destroy();

    const closed = t.logs.map((l) => JSON.parse(l)).filter((l) => l.msg === "stream closed" && l.run_id === runId);
    const { maxBufferedBytes } = t.built.ctx.streams.stats;
    console.log(
      `slow client: server stopped reading the log at seq ${closed[0].to_seq} of ${TOTAL} (reason: ${closed[0].reason}); ` +
        `largest per-stream write buffer ever seen: ${maxBufferedBytes} bytes`,
    );
    expect(closed[0].reason).toBe("stalled");
    expect(closed[0].to_seq).toBeLessThan(TOTAL / 2); // it did NOT pull the whole run into memory
    expect(maxBufferedBytes).toBeLessThan(256 * 1024); // bounded by page size, not by backlog

    // the same client comes back healthy and gets everything
    const healthy = consume(t.base, key, runId);
    await healthy.waitSynced(TOTAL, 30000);
    expect(healthy.state.count).toBe(TOTAL);
    expect(healthy.state.gaps).toBe(0);
    healthy.close();
  });
});

describe("partial writes", () => {
  it("one invalid event rejects the whole batch: nothing from it is stored", async () => {
    const { key } = await t.project("partial-validation");
    const runId = await t.run(key, "r");
    const r = await post(key, runId, [...series("loss", 99), { key: "loss", step: -1, value: 1 }]);
    expect(r.status).toBe(400);
    expect(r.json.details[0].path).toBe("events.99.step");
    const tooBig = await post(key, runId, series("loss", 1001));
    expect(tooBig.status).toBe(400);
    expect(await dbHead(t.db, runId)).toEqual({ lastSeq: 0, count: 0, minSeq: 0, maxSeq: 0 });
  });

  it("a crash between INSERT and COMMIT leaves no rows, no seq movement and no live event; the retry then succeeds completely", async () => {
    const { key } = await t.project("partial-crash");
    const runId = await t.run(key, "r");
    await post(key, runId, series("loss", 10));
    const viewer = consume(t.base, key, runId);
    await viewer.waitSynced(10);

    failInsert = 1;
    const batch = series("loss", 50, 10);
    const failed = await post(key, runId, batch);
    expect(failed.status).toBe(500);
    await sleep(700); // > 2 poll intervals
    expect(await dbHead(t.db, runId)).toEqual({ lastSeq: 10, count: 10, minSeq: 1, maxSeq: 10 });
    expect(viewer.state.lastSeq).toBe(10); // the page never showed a rolled-back point

    const retry = await post(key, runId, batch);
    expect(retry.json).toMatchObject({ accepted: 50, duplicates: 0, last_seq: 60 });
    await viewer.waitSynced(60);
    expect(toPoints(viewer.state)).toEqual(await dbPoints(t.db, runId));
    viewer.close();
  });
});

describe("old API key after rotation", () => {
  it("stops working on the next request, on every endpoint, and its open stream is closed", async () => {
    const { key: oldKey } = await t.project("rotation");
    const runId = await t.run(oldKey, "r");
    await post(oldKey, runId, series("loss", 5));
    const viewer = consume(t.base, oldKey, runId);
    await viewer.waitSynced(5);

    const rotated = await t.call(oldKey, "POST", "/keys/rotate");
    const newKey = rotated.json.api_key;
    const at = Date.now();

    // immediately, no grace period, no cache to expire
    expect((await t.call(oldKey, "GET", "/runs")).status).toBe(401);
    expect((await t.call(oldKey, "GET", `/runs/${runId}`)).status).toBe(401);
    expect((await t.call(oldKey, "GET", `/runs/${runId}/metrics`)).status).toBe(401);
    expect((await t.call(oldKey, "GET", `/runs/${runId}/stream`)).status).toBe(401);
    expect((await post(oldKey, runId, series("loss", 5, 5))).status).toBe(401);
    expect((await t.call(oldKey, "POST", "/keys/rotate")).status).toBe(401);
    expect((await dbHead(t.db, runId)).lastSeq).toBe(5);

    // the stream that was ALREADY open with the old key
    await waitUntil(() => viewer.status === "revoked", 3000, "open stream closed after rotation");
    console.log(`open stream on the old key was closed ${Date.now() - at} ms after rotation`);
    await viewer.handle.finished; // and the client does not keep retrying
    await post(newKey, runId, series("loss", 5, 5));
    await sleep(500);
    expect(viewer.state.lastSeq).toBe(5); // it saw nothing written after the rotation

    expect((await t.call(newKey, "GET", "/runs")).status).toBe(200);
    const fresh = consume(t.base, newKey, runId);
    await fresh.waitSynced(10);
    fresh.close();
  });

  it("even if the revocation notice is lost, an open stream dies at its next periodic key check", async () => {
    const { key, projectId } = await t.project("rotation-no-notify");
    const runId = await t.run(key, "r");
    const viewer = consume(t.base, key, runId);
    await viewer.waitSynced(0);
    // revoke behind the server's back: no NOTIFY is sent
    await t.db.query("UPDATE api_keys SET revoked_at = now() WHERE project_id = $1", [projectId]);
    const at = Date.now();
    await waitUntil(() => viewer.status === "revoked", 3000, "stream closed by periodic key check");
    console.log(`without NOTIFY, the stream was closed after ${Date.now() - at} ms (key re-check every 300 ms in this test, 3 s by default)`);
  });
});
