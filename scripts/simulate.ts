// Demo data: creates a project, prints its API key, then feeds three runs at
// ~10 events/s each through the unreliable client (reordering, duplicates,
// lost responses, late points) so the live page has something to show.
import { makeEvents, mulberry32, sendUnreliably } from "./lib/flaky-client.js";

const baseUrl = process.argv[2] ?? process.env.BASE_URL ?? "http://localhost:8080";
const adminToken = process.env.ADMIN_TOKEN ?? "dev-admin-token";
const minutes = Number(process.env.DEMO_MINUTES ?? 10);
const rng = mulberry32(Number(process.env.SEED ?? Date.now()));

const json = async (path: string, token: string, body: unknown) => {
  const res = await fetch(baseUrl + path, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json() as Promise<any>;
};

const created = await json("/admin/projects", adminToken, { name: "Demo" });
const apiKey: string = created.api_key;
console.log("\n==============================================================");
console.log(` Open ${process.env.PUBLIC_URL ?? baseUrl} and paste this API key:`);
console.log(` ${apiKey}`);
console.log("==============================================================\n");

const runs: string[] = [];
for (const name of ["resnet-baseline", "resnet-lr-sweep", "tiny-transformer"]) {
  runs.push((await json("/runs", apiKey, { name })).id);
}

const keys = ["train/loss", "val/acc"];
const stepsPerTick = 5; // 5 steps x 2 keys = 10 events per second per run
const until = Date.now() + minutes * 60_000;
for (let tick = 0; Date.now() < until; tick++) {
  const started = Date.now();
  await Promise.all(
    runs.map((runId) =>
      sendUnreliably({
        baseUrl,
        apiKey,
        runId,
        rng,
        maxBatch: 6,
        concurrency: 2,
        // a decaying curve with noise, so the charts look like training
        events: makeEvents(keys, stepsPerTick, rng, tick * stepsPerTick).map((e) => ({
          ...e,
          value:
            e.key === "train/loss"
              ? Number((2.5 * Math.exp(-e.step / 400) + rng() * 0.15).toFixed(4))
              : Number((1 - Math.exp(-e.step / 500) - rng() * 0.05).toFixed(4)),
          ts: new Date().toISOString(),
        })),
      }).catch((err) => console.error("send failed, will continue:", String(err))),
    ),
  );
  await new Promise((r) => setTimeout(r, Math.max(0, 1000 - (Date.now() - started))));
}
console.log("demo finished");
