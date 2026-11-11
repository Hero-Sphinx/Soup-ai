// Requirement 3: project A cannot read, write, subscribe to, or guess the IDs
// of project B. Plus the side channels: cache, error messages, logs, timing.
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { toPoints } from "../public/state.js";
import { ADMIN_TOKEN, consume, dbHead, dbPoints, sleep, startApp, type TestApp } from "./helpers.js";

let t: TestApp;
let A: { projectId: string; key: string };
let B: { projectId: string; key: string };
let runA: string;
let runB: string;
let failNextInsert = false;

const pointsFor = (key: string, n: number, from = 0) =>
  Array.from({ length: n }, (_, i) => ({ key, step: from + i, value: (from + i) * 1.5 }));

beforeAll(async () => {
  t = await startApp(
    { streamPollMs: 300 },
    {
      afterInsert: () => {
        if (failNextInsert) {
          failNextInsert = false;
          throw new Error("connection to postgres://admin:hunter2@db-internal:5432 failed");
        }
      },
    },
  );
  A = await t.project("tenant-a");
  B = await t.project("tenant-b");
  runA = await t.run(A.key, "a-run");
  runB = await t.run(B.key, "b-secret-run");
  expect((await t.call(A.key, "POST", `/runs/${runA}/metrics`, { events: pointsFor("a_metric", 50) })).status).toBe(200);
  expect((await t.call(B.key, "POST", `/runs/${runB}/metrics`, { events: pointsFor("b_secret_metric", 50) })).status).toBe(200);
});
afterAll(() => t.close());

/** Nothing of B may appear in anything A receives. */
const expectNoTraceOfB = (text: string) => {
  expect(text).not.toContain("b_secret");
  expect(text).not.toContain(runB);
  expect(text).not.toContain(B.projectId);
};

describe("read", () => {
  it("A gets the same 404 for B's run as for a run that does not exist", async () => {
    const ghost = randomUUID();
    for (const suffix of ["", "/metrics", "/metrics?after_seq=0", "/stream"]) {
      const theirs = await t.call(A.key, "GET", `/runs/${runB}${suffix}`);
      const none = await t.call(A.key, "GET", `/runs/${ghost}${suffix}`);
      expect(theirs.status).toBe(404);
      // byte-identical: status, body, content type. No existence oracle.
      expect(theirs.text).toBe(none.text);
      expect(theirs.text).toBe('{"error":"not found"}');
      expect(theirs.headers.get("content-type")).toBe(none.headers.get("content-type"));
      expectNoTraceOfB(theirs.text);
    }
  });

  it("A's run list contains only A's runs", async () => {
    const list = await t.call(A.key, "GET", "/runs");
    expect(list.json.runs.map((r: any) => r.id)).toEqual([runA]);
    expectNoTraceOfB(list.text);
    expect((await t.call(A.key, "GET", "/project")).json.id).toBe(A.projectId);
  });

  it("B can read its own run (the 404s above are not just a broken endpoint)", async () => {
    expect((await t.call(B.key, "GET", `/runs/${runB}`)).json.name).toBe("b-secret-run");
    expect((await t.call(B.key, "GET", `/runs/${runB}/metrics`)).json.points).toHaveLength(50);
  });
});

describe("write", () => {
  it("A cannot write metrics into B's run, and nothing in B changes", async () => {
    const before = await dbPoints(t.db, runB);
    const r = await t.call(A.key, "POST", `/runs/${runB}/metrics`, { events: pointsFor("injected", 5) });
    expect(r.status).toBe(404);
    expect(r.text).toBe('{"error":"not found"}');
    // also try to overwrite an existing point of B (would be a "conflict" if it reached the table)
    const r2 = await t.call(A.key, "POST", `/runs/${runB}/metrics`, {
      events: [{ key: "b_secret_metric", step: 0, value: 999 }],
    });
    expect(r2.status).toBe(404);
    expectNoTraceOfB(r2.text.replace(runB, "")); // body must not echo stored values
    expect(await dbPoints(t.db, runB)).toEqual(before);
    expect((await dbHead(t.db, runB)).lastSeq).toBe(50);
    const conflicts = await t.db.query("SELECT count(*)::int AS n FROM metric_conflicts WHERE run_id = $1", [runB]);
    expect(conflicts.rows[0].n).toBe(0);
  });

  it("the project in a request comes from the key only: a project_id in the body is rejected", async () => {
    const r = await t.call(A.key, "POST", "/runs", { name: "x", project_id: B.projectId });
    expect(r.status).toBe(400);
    const owned = await t.db.query("SELECT count(*)::int AS n FROM runs WHERE project_id = $1", [B.projectId]);
    expect(owned.rows[0].n).toBe(1);
  });

  it("A cannot rotate B's key, and rotating A's key leaves B's key working", async () => {
    const rotated = await t.call(A.key, "POST", "/keys/rotate");
    expect(rotated.status).toBe(200);
    A.key = rotated.json.api_key;
    expect((await t.call(B.key, "GET", "/runs")).status).toBe(200);
    expect((await t.call(A.key, "GET", "/runs")).status).toBe(200);
  });
});

describe("live stream", () => {
  it("A cannot subscribe to B's run: it gets a JSON 404, not an event stream", async () => {
    const r = await t.call(A.key, "GET", `/runs/${runB}/stream`, undefined, { "last-event-id": "0" });
    expect(r.status).toBe(404);
    expect(r.headers.get("content-type")).toContain("application/json");
  });

  it("while B ingests, A's open stream receives only A's points", async () => {
    const a = consume(t.base, A.key, runA);
    await a.waitSynced(50);
    // B is busy; A writes a little too
    for (let i = 0; i < 5; i++) {
      await t.call(B.key, "POST", `/runs/${runB}/metrics`, { events: pointsFor("b_secret_metric", 40, 50 + i * 40) });
    }
    await t.call(A.key, "POST", `/runs/${runA}/metrics`, { events: pointsFor("a_metric", 10, 50) });
    await a.waitSynced(60);
    await sleep(700); // > 2 poll intervals: anything misrouted would have arrived by now
    expect([...a.state.series.keys()]).toEqual(["a_metric"]);
    expect(toPoints(a.state)).toEqual(await dbPoints(t.db, runA));
    expect(a.state.lastSeq).toBe(60);
    a.close();
  });

  it("a cursor cannot be used to reach outside the run", async () => {
    for (const cursor of ["-1", "1e3", "0 OR 1=1", "99999999999999999999"]) {
      const r = await t.call(A.key, "GET", `/runs/${runA}/stream`, undefined, { "last-event-id": cursor });
      expect(r.status).toBe(400);
      expect(r.text).toBe('{"error":"invalid cursor"}');
    }
  });
});

describe("guessing IDs", () => {
  it("IDs are random UUIDv4 (not sequential), and malformed IDs never reach the database", async () => {
    const second = await t.run(A.key, "a-run-2");
    for (const id of [runA, runB, second]) expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(second.slice(0, 8)).not.toBe(runA.slice(0, 8));

    const guesses = ["1", "0", "abc", runB.toUpperCase(), runB.slice(0, 35), `${runB}0`, "' OR '1'='1", "%00", "..%2F" + runB, "null"];
    for (const g of guesses) {
      for (const suffix of ["", "/metrics", "/stream"]) {
        const r = await t.call(A.key, "GET", `/runs/${encodeURIComponent(g).replace(/%252F/g, "%2F")}${suffix}`);
        expect([r.status, r.text], `guess ${g}${suffix}`).toEqual([404, '{"error":"not found"}']);
      }
    }
  });
});

describe("cache", () => {
  it("every API response forbids caching, so no proxy/browser cache can serve A's data to B", async () => {
    for (const path of ["/runs", `/runs/${runA}`, `/runs/${runA}/metrics`, "/project", `/runs/${runB}`]) {
      const r = await t.call(A.key, "GET", path);
      expect(r.headers.get("cache-control"), path).toBe("no-store");
      expect(r.headers.get("etag"), path).toBeNull();
    }
    const ctrl = new AbortController();
    const stream = await fetch(`${t.base}/runs/${runA}/stream`, {
      headers: { authorization: `Bearer ${A.key}` },
      signal: ctrl.signal,
    });
    expect(stream.headers.get("cache-control")).toBe("no-store");
    ctrl.abort();
  });

  it("the same URL with two different keys returns each project's own data", async () => {
    const a = await t.call(A.key, "GET", "/runs");
    const b = await t.call(B.key, "GET", "/runs");
    const a2 = await t.call(A.key, "GET", "/runs");
    expect(b.json.runs.map((r: any) => r.id)).toEqual([runB]);
    expect(a2.text).toBe(a.text);
    expect(a.text).not.toContain(runB);
    // There is no server-side cache in this system at all (see DESIGN.md);
    // this test would start to matter the day someone adds one.
  });
});

describe("authentication", () => {
  it("rejects missing, malformed, wrong-place and other-scope credentials", async () => {
    const paths: [string, string][] = [
      ["GET", "/runs"],
      ["GET", `/runs/${runA}`],
      ["GET", `/runs/${runA}/metrics`],
      ["GET", `/runs/${runA}/stream`],
      ["POST", `/runs/${runA}/metrics`],
      ["POST", "/runs"],
      ["POST", "/keys/rotate"],
      ["GET", "/project"],
    ];
    for (const [method, path] of paths) {
      const body = method === "POST" ? { events: pointsFor("x", 1), name: "x" } : undefined;
      const attempts = [
        await t.call(null, method, path, body),
        await t.call("sk_" + "A".repeat(43), method, path, body),
        await t.call(null, method, path, body, { authorization: A.key }), // no "Bearer"
        await t.call(null, method, path, body, { authorization: `Bearer ${ADMIN_TOKEN}` }), // admin token is not a project key
        await t.call(null, method, `${path}?api_key=${A.key}`, body), // keys in URLs are never accepted
      ];
      for (const r of attempts) expect([r.status, r.text], `${method} ${path}`).toEqual([401, '{"error":"unauthorized"}']);
    }
    expect((await dbHead(t.db, runA)).lastSeq).toBe(60);
  });

  it("a project key cannot create projects", async () => {
    const r = await t.call(A.key, "POST", "/admin/projects", { name: "evil" });
    expect(r.status).toBe(401);
  });
});

describe("error messages", () => {
  it("client errors are fixed strings; they describe the caller's input and nothing else", async () => {
    const cases: [string, Promise<{ status: number; text: string }>, number][] = [
      ["malformed JSON", t.call(A.key, "POST", `/runs/${runA}/metrics`, '{"events": [', {}), 400],
      ["wrong content type", t.call(A.key, "POST", `/runs/${runA}/metrics`, "x=1", { "content-type": "text/plain" }), 415],
      ["body over 1 MB", t.call(A.key, "POST", `/runs/${runA}/metrics`, { events: [], pad: "x".repeat(1_100_000) }), 413],
      ["wrong types", t.call(A.key, "POST", `/runs/${runA}/metrics`, { events: [{ key: "k", step: "1", value: null }] }), 400],
      ["NaN-ish / huge", t.call(A.key, "POST", `/runs/${runA}/metrics`, '{"events":[{"key":"k","step":1,"value":1e999}]}'), 400],
      ["bad key name", t.call(A.key, "POST", `/runs/${runA}/metrics`, { events: [{ key: "<script>", step: 1, value: 1 }] }), 400],
      ["unknown field", t.call(A.key, "POST", `/runs/${runA}/metrics`, { events: [{ key: "k", step: 1, value: 1, run_id: runB }] }), 400],
      ["unknown route", t.call(A.key, "GET", "/api/v1/users"), 404],
    ];
    for (const [label, pending, status] of cases) {
      const r = await pending;
      expect(r.status, label).toBe(status);
      expect(r.text, label).not.toMatch(/postgres|select |insert |syntax|stack|node_modules|\.ts:|at \w+ \(/i);
      expectNoTraceOfB(r.text);
    }
    expect((await dbHead(t.db, runA)).lastSeq).toBe(60);
  });

  it("an unexpected server error returns a generic 500; the detail goes to the server log only", async () => {
    failNextInsert = true;
    const r = await t.call(A.key, "POST", `/runs/${runA}/metrics`, { events: pointsFor("a_metric", 1, 5000) });
    expect([r.status, r.text]).toEqual([500, '{"error":"internal error"}']);
    expect(t.logs.some((l) => l.includes('"level":"error"') && l.includes("unhandled"))).toBe(true);
  });
});

describe("logs", () => {
  it("no API key, admin token or Authorization header is ever written to the logs", async () => {
    const all = t.logs.join("\n");
    expect(t.logs.length).toBeGreaterThan(50);
    expect(all).not.toContain(A.key);
    expect(all).not.toContain(B.key);
    expect(all).not.toContain(ADMIN_TOKEN);
    expect(all).not.toMatch(/sk_[A-Za-z0-9_-]{20,}/);
    expect(all.toLowerCase()).not.toContain("bearer");
    // the key that was sent in a query string above did not leak either: we log the route pattern, not the URL
    expect(all).not.toContain("api_key");
    // but requests are attributable: project id and key id are there
    expect(all).toContain(A.projectId);
  });

  it("KNOWN LEAK, documented not fixed: the 500 log line carries the raw driver error text", () => {
    // If a database error message ever contains a connection string, it lands
    // in our own server log. Fine for a single-tenant log; a real concern the
    // day logs are shipped to a shared system. See NOTE.md.
    expect(t.logs.join("\n")).toContain("hunter2");
  });
});

describe("timing (measurement, not an assertion)", () => {
  it("prints latency of '404: other project's run' vs '404: no such run'", async () => {
    const sample = async (id: string) => {
      const s = performance.now();
      await t.call(A.key, "GET", `/runs/${id}`);
      return performance.now() - s;
    };
    const theirs: number[] = [];
    const none: number[] = [];
    for (let i = 0; i < 300; i++) {
      // interleaved so drift affects both equally
      theirs.push(await sample(runB));
      none.push(await sample(randomUUID()));
    }
    const median = (xs: number[]) => xs.sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;
    const [mt, mn] = [median(theirs), median(none)];
    console.log(
      `timing over 300 interleaved samples each: other-project 404 median ${mt.toFixed(3)} ms, ` +
        `nonexistent 404 median ${mn.toFixed(3)} ms, difference ${(mt - mn).toFixed(3)} ms\n` +
        `(both paths run the same single indexed query "WHERE id = $1 AND project_id = $2"; ` +
        `this is a local measurement and cannot rule out a difference an attacker could average out)`,
    );
  });
});
