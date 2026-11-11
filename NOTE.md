# Note

**Read this first: Docker.** The machine this was built on has no Docker, so I never ran `docker compose up` myself. It was checked on GitHub Actions instead: there the compose stack built, started and passed the smoke test. The numbers below come from the same code on a Windows laptop with a real local Postgres (`npm run test:local`).

**The first CI run failed 2 of 42 tests,** both of which passed on my laptop. One was a race in the browser test: it read the page field by field while the last forced disconnect was still landing. The other, by my diagnosis from the symptoms, was the slow-client test: its backlog was smaller than Linux socket buffers, so the client never looked stuck. Both tests are changed; the latest run on the repository shows the current state.

Design: [DESIGN.md](DESIGN.md). Full tables and every number: [EVIDENCE.md](EVIDENCE.md). Raw output and screenshots: [test-output/](test-output/).

## 1. Does what the user sees match what is stored?

In the tests, yes: 42 of 42 pass.

- **Browser test** (real Chromium): 6000 points sent through a client with lost requests, lost responses, aborted requests, re-sends and late points. The server killed the page's live connection 23 times and the browser was offline for 1.5 s. At the end the page's data, the numbers on screen and a fingerprint of the point set equal a direct query on Postgres.
- **API test**: same chaos, 72 forced disconnects, two consumers. Stored rows = sent events exactly once.

**What the tests would NOT catch**

- A wrong picture: I compare data and numbers, not pixels.
- Events the client never sent. **This one happened for real** (section 5).
- A page that is wrong mid-run and right at the end; I compare only after things settle.
- Other browsers, a slow or lossy link, a buffering proxy, a server or database restart, more than one app instance.
- Unlucky timing: each test is one seeded schedule, a sample and not a proof.
- Runs with millions of points; the page holds the whole run in memory.

## 2. Does a project see only its own data?

19 automated tests: project A cannot read, write, list, subscribe to or rotate anything of B. B's run and a non-existent run return byte-identical 404s. While B ingests, A's open stream receives only A's points. Guessed and malformed IDs get the same 404.

There is **no server-side cache**, so the cache test only checks `Cache-Control: no-store`. It would not be enough if a cache were added.

What else could leak:

- **Logs:** no key or token in any log line (asserted). **One leak left in on purpose and pinned by a test:** an unexpected database error is logged with the driver's raw message.
- **Error messages:** fixed strings; 500s are generic (asserted).
- **Timing:** other-project 404 vs non-existent 404, medians 1.018 vs 1.016 ms. Measured, **not asserted**; an attacker averaging many requests is not ruled out.
- **Load: not isolated.** No per-project rate limit, so one project can slow all the others.

## 3. Silent failures

| Failure | How I checked | Result |
|---|---|---|
| Events lost on reconnect | offline client, connection cut mid-frame, notification channel killed | resumes from its last applied event; nothing lost, nothing sent twice |
| Duplicates | same batch 12×, 8 of them at once | stored once |
| Slow client queue overflow | a client that never reads, 300000 points waiting | no queue exists; server buffered 54.5 KB, cut the client off, it later got all 300000 |
| Partial writes | injected crash between INSERT and COMMIT | 0 rows stored, viewer saw nothing, retry stored all |
| Old key after rotation | old key on six endpoints plus an open stream | all 401 at once; stream closed in 8 ms |

In production I would know an event was lost because, per run, the last event number must equal the row count. **Nothing alerts on that yet.**

## 4. Verdict: SHIP for 50 runs at 10 events/s, on conditions

| Load | Requests/s | Ingest p50 / p99 | Result |
|---|---|---|---|
| 50 runs × 10 ev/s (target) | 250 | 16 / 118 ms | 0 failed, 50/50 viewers = database |
| 50 × 10, twice the request rate | 500 | 17 / 168 ms | same |
| 150 × 10 ev/s | 750 | 23 / **3339 ms** | correct, but database pool exhausted |

The limit is between 2× and 3× the target. Past it the system got slow, not wrong. Cost follows requests, not events.

Conditions: these were 20–30 second runs on a laptop, not in Docker, one instance. That is thin evidence.

**Change first:** make ingest one database round trip (it is eight), and have clients send once a second. Then a per-project rate limit, an alert on the lost-event check, and a one-hour soak in the real environment.

## 5. AI tools, and what I checked myself

The code, tests and first drafts of these documents were written with Claude Code from the task brief. Its first output was wrong in these places, each caught by evidence:

- **A leak:** dropped streams were never released. Exposed by one odd number in a passing test ("killed 478 connections" for about 90 connects).
- **A load test that measured itself:** first result 1.7 s, because the load generator shared the server's process. Separated: 16 ms.
- **A test that could not fail correctly:** the "slow client" was quietly reading the data.
- **Two tests that failed only on Linux CI** although all 42 passed on Windows (see the top of this note).
- Smaller: too little chaos in the browser test, a growing reconnect delay, chart thinning that could hide a spike.

**What I verified by hand** (seven checks, screenshots in [test-output/manual-checks.md](test-output/manual-checks.md)):

- Stopping the server: the page said "Reconnecting…", then came back "Live" with 0 gaps.
- Two projects: each list shows only its own runs; opening the other project's run URL gives "Not found".
- Rotating a key: the other tab was signed out and the demo client was refused straight away.
- **Found by looking, not by the tests:** a hole of about 175 steps in a chart. The demo client drops data while the server is down, and the page draws a straight line over the gap. Also two display bugs, both fixed.

## 6. What surprised me, what still concerns me

What surprised me was how fast the system is and how strict key rotation is. Points show up on the page almost as soon as they are sent. When I rotated the key in one tab, the other tab was signed out within seconds and the demo client started failing straight away. There is no grace period at all.

What still concerns me comes from what I saw by hand. When the server was down, the demo client threw data away and the chart drew a straight line over the hole, so a user would never know points were missing. I found two display bugs just by looking, which 42 automated tests had not caught, so there are probably more. And I have only seen Docker work on CI, never on my own machine, and CI failed two tests that my laptop passed.
