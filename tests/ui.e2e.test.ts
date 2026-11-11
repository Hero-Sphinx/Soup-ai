// Requirement 2 (UI half): a real browser has the run page open while metrics
// arrive out of order, duplicated and late, the server drops the live
// connection repeatedly and the browser goes offline for a while.
// At the end, what the page shows must equal what Postgres holds.
import { mkdirSync } from "node:fs";
import { chromium, type Browser } from "playwright";
import { afterAll, beforeAll, expect, it } from "vitest";
import { digestPoints } from "../public/state.js";
import { makeEvents, mulberry32, sendUnreliably } from "../scripts/lib/flaky-client.js";
import { dbHead, dbPoints, sleep, startApp, type TestApp } from "./helpers.js";

const SEED = Number(process.env.SEED ?? 20261111);
let t: TestApp;
let browser: Browser;

beforeAll(async () => {
  t = await startApp({ streamPage: 53 });
  browser = await chromium.launch();
});
afterAll(async () => {
  await browser?.close();
  await t.close();
});

it("UI: page state == database after out-of-order, duplicated, late metrics and a connection that drops and recovers", async () => {
  const { key } = await t.project("consistency-ui");
  const runId = await t.run(key, "ui chaos run");
  await t.run(key, "<b>bold</b><img src=x onerror=window.__xss=1>"); // must render as text

  // phone-sized viewport: the layout target
  const context = await browser.newContext({ viewport: { width: 360, height: 800 } });
  const page = await context.newPage();
  const pageErrors: string[] = [];
  page.on("pageerror", (e) => pageErrors.push(String(e)));

  // sign in through the real form, open the run from the real list
  await page.goto(t.base);
  await page.fill("#key", key);
  await page.click("#signin-form button");
  await page.click(`#runs li[data-run-id="${runId}"] a`);
  await page.waitForSelector('[data-testid="status"][data-status="live"]');

  const rng = mulberry32(SEED + 1);
  const events = makeEvents(["train/loss", "val/acc", "lr"], 2000, rng); // 6000 points, spread over ~8 s
  let kills = 0;
  const killer = setInterval(() => (kills += t.built.ctx.streams.killAll()), 200);
  const sending = sendUnreliably({ baseUrl: t.base, apiKey: key, runId, events, rng, paceMs: 200 });

  // the page visibly admits it lost the connection
  await page.waitForSelector('[data-testid="status"][data-status="reconnecting"]', { timeout: 10000 });
  // browser offline for 1.5 s while data keeps arriving
  await sleep(600);
  await context.setOffline(true);
  const seqWhenOffline = Number(await page.textContent('[data-testid="last-seq"]'));
  await sleep(1500);
  const statusWhileOffline = await page.getAttribute('[data-testid="status"]', "data-status");
  await context.setOffline(false);

  const stats = await sending;
  clearInterval(killer);

  const head = await dbHead(t.db, runId);
  const stored = await dbPoints(t.db, runId);
  await page.waitForFunction(
    (lastSeq) =>
      document.querySelector('[data-testid="status"]')?.getAttribute("data-status") === "live" &&
      document.querySelector('[data-testid="last-seq"]')?.textContent === String(lastSeq),
    head.lastSeq,
    { timeout: 20000 },
  );

  // 1. the page's model, read out of the running page
  const pagePoints = await page.evaluate(() => (window as any).__soup.points());
  // 2. what is actually painted in the DOM
  const dom = {
    count: await page.textContent('[data-testid="count"]'),
    lastSeq: await page.textContent('[data-testid="last-seq"]'),
    digest: await page.textContent('[data-testid="digest"]'),
    gaps: await page.textContent('[data-testid="gaps"]'),
    cards: await page.$$eval("#charts article", (els) =>
      els.map((el) => ({
        key: (el as HTMLElement).dataset.key,
        value: el.querySelector(".value")?.textContent,
        meta: el.querySelector(".meta")?.textContent,
        ys: (el.querySelector("polyline")?.getAttribute("points") ?? "").split(" ").map((c) => Number(c.split(",")[1])),
      })),
    ),
  };

  const streamLogs = t.logs.map((l) => JSON.parse(l)).filter((l) => l.msg === "stream closed" && l.run_id === runId);
  console.log(
    [
      `seed=${SEED + 1}`,
      `browser stream connections closed so far: ${streamLogs.length}, resumed from seq: ${streamLogs.map((l) => l.from_seq).join(",")}`,
      `sent: ${stats.events} unique events, ${stats.requests} HTTP requests; chaos: ${stats.lostRequests} lost requests, ` +
        `${stats.lostResponses} lost responses, ${stats.aborted} aborted, ${stats.staleResends} stale re-sends, ${stats.lateEvents} late`,
      `server killed ${kills} live connections; browser offline 1.5 s at seq ${seqWhenOffline} (status shown while offline: "${statusWhileOffline}")`,
      `db:   rows=${head.count} last_seq=${head.lastSeq} fingerprint=${digestPoints(stored)}`,
      `page: rows=${dom.count} last_seq=${dom.lastSeq} fingerprint=${dom.digest} gaps=${dom.gaps}`,
      `cards: ${JSON.stringify(dom.cards.map(({ ys, ...c }) => ({ ...c, drawnPoints: ys.length })))}`,
    ].join("\n"),
  );

  expect(kills).toBeGreaterThan(3);
  expect(statusWhileOffline).toBe("reconnecting");
  expect(head.count).toBe(events.length);

  expect(pagePoints).toEqual(stored);
  expect(dom.count).toBe(String(head.count));
  expect(dom.lastSeq).toBe(String(head.lastSeq));
  expect(dom.digest).toBe(digestPoints(stored));
  expect(dom.gaps).toBe("0");

  // each card shows the value of the HIGHEST step (not of the last-arrived point)
  for (const card of dom.cards) {
    const mine = stored.filter((p: { key: string }) => p.key === card.key);
    const latest = mine[mine.length - 1]!;
    expect(card.value).toBe(String(latest.value));
    expect(card.meta).toBe(`step ${latest.step} · ${mine.length.toLocaleString()} points`);
    // 2000 points is over the drawing limit, so the line is thinned. The
    // thinning must keep the extremes: the series' min and max are on the line.
    expect(card.ys.length).toBeGreaterThan(1500);
    expect(card.ys.length).toBeLessThanOrEqual(mine.length);
    expect(Math.max(...card.ys)).toBe(76);
    expect(Math.min(...card.ys)).toBe(4);
  }
  expect(dom.cards.map((c) => c.key)).toEqual(["lr", "train/loss", "val/acc"]);

  mkdirSync("test-output", { recursive: true });
  await page.screenshot({ path: "test-output/run-page-mobile.png", fullPage: true });

  // the run list agrees, and a hostile run name is shown as text
  await page.goto(t.base);
  await page.waitForSelector(`#runs li[data-run-id="${runId}"]`);
  expect(await page.getAttribute(`#runs li[data-run-id="${runId}"] [data-testid="run-points"]`, "data-points")).toBe(String(head.lastSeq));
  expect(await page.locator("#runs strong", { hasText: "<b>bold</b>" }).count()).toBe(1);
  expect(await page.locator("#runs b, #runs img").count()).toBe(0);
  expect(await page.evaluate(() => (window as any).__xss)).toBeUndefined();
  await page.screenshot({ path: "test-output/run-list-mobile.png", fullPage: true });

  expect(pageErrors).toEqual([]);
  await context.close();
});
