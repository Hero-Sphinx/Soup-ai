# Evidence (long version of NOTE.md)

**Read this first.** Docker is not installed on the machine this was built on. `docker compose up` and the two Dockerfiles are therefore **written but never run by me**. Everything below was measured by running the same code against a real local Postgres 17 (`npm run test:local`) on a Windows laptop. The CI workflow builds and smoke-tests the compose stack, but it has not run yet either. Treat the Docker path as unverified until you see it green.

Raw output: [test-output/](test-output/). Counts and timings quoted below are from the saved run and vary a little from run to run. Design: [DESIGN.md](DESIGN.md).

## 1. Does what the user sees match what is stored?

In the tests, yes: 42 of 42 pass, three consecutive runs.

- **Browser test** (`tests/ui.e2e.test.ts`, real Chromium): 6000 points sent through a client with lost requests, lost responses, aborted requests, stale re-sends and ~5% late points. The server killed the page's live connection 27 times and the browser was offline for 1.5 s. At the end the page's model, the numbers in the DOM and a fingerprint of the point set equal a direct query on Postgres.
- **API test** (`tests/consistency.test.ts`): same chaos, 68 forced disconnects, two consumers (one joining mid-run). Stored rows = sent events exactly once, seq = 1..6000, both consumers = database.

**What these tests would NOT catch**

- A wrong picture. I compare the page's data, its numbers and that the drawn line contains the series' min and max. I do not compare pixels.
- Events the client never managed to send (it crashed with a full buffer). The server cannot know about those; my test client retries forever.
- A page that is wrong in the middle of the run and right at the end. I only compare after things settle.
- Other browsers, a backgrounded tab, a sleeping laptop, a slow or lossy link, a proxy that buffers SSE, a half-open connection (the client has a 10 s silence watchdog; it is not covered end to end).
- Server crash or database restart mid-run, and more than one app instance.
- Unlucky timing. Each test runs one seeded schedule (`SEED=… npm test` gives another). That is a sample, not a proof.
- Big runs. The page holds the whole run in memory; I tested thousands of points, not millions.

## 2. Does a project see only its own data?

Tested in `tests/isolation.test.ts` (19 tests): A cannot read, write, list, subscribe to or rotate anything of B. B's run and a non-existent run give A byte-identical 404s. While B ingests, A's open stream receives only A's points. Malformed and guessed IDs all get the same 404 and never reach the database. A key in the URL, a missing "Bearer", the admin token used as a project key: all 401.

**Cache:** there is no server-side cache, so there is nothing to leak through. The test only checks `Cache-Control: no-store` and that the same URL with two keys returns each project's data. If someone adds a cache later, these tests are not enough.

**What else could leak**

| Channel | State | How I checked |
|---|---|---|
| Logs | No key, token or Authorization header in any log line across the whole isolation suite | assertion over captured logs |
| Logs | **Leak, left in on purpose and pinned by a test:** an unexpected database error is logged with the driver's raw message. If that message ever held a connection string, it would be in our log | test named "KNOWN LEAK" |
| Error messages | Fixed strings; validation errors describe only the caller's own input; 500s are generic | 9 malformed requests + injected failure |
| Timing | Other-project 404 vs non-existent 404: medians 0.926 ms vs 0.915 ms over 300 samples each | printed, **not asserted**. Same single query on both paths, but a local measurement cannot rule out a difference an attacker could average out |
| IDs | seq is per run, so it reveals nothing about other projects. UUIDv4 | format check |
| Load | **Not isolated.** No per-project rate limit: one project can exhaust the shared database pool and slow everyone | seen in the 150-run load test |

## 3. Silent failures

| Failure | Why it should not happen | How I checked |
|---|---|---|
| Events lost on reconnect | Client resumes from the last seq it applied; server replays from the log | offline client gets exactly the missed points, 0 replays; connection cut mid-frame resumes after the last complete frame; LISTEN connection killed → delivery in 264 ms by polling, and a warning is logged |
| Duplicates | Primary key `(run, key, step)` | same batch 12× (8 concurrent) → stored once; 20 concurrent writers → seq exactly 1..2000 |
| Different value for the same point | First write wins, returned in `conflicts`, recorded | asserted on response, table and stored value |
| Queue overflow, slow client | There is no queue; the server stops reading the log until the socket drains | a client that never reads: server stopped at seq 5500 of 60000, buffer peaked at 26.5 KB, cut off after the stall timeout; it then reconnected and got all 60000 |
| Partial writes | One batch = one transaction; the live event only exists after commit | invalid last event → 0 rows; injected crash between INSERT and COMMIT → 0 rows, viewer saw nothing, retry stored all 50 |
| Old key after rotation | No grace period, no auth cache; open streams are closed | old key → 401 on six endpoints at once; its open stream closed 8 ms after rotation, and after 303 ms when the notification was deliberately not sent |

How would I know in production that an event was lost? Per run, `runs.last_seq` must equal `count(metrics)` and seqs must be contiguous; the page counts gaps itself; the client checks every ack. None of this is wired to an alert yet. `/admin/stats` exposes pool waiters, event-loop delay and stalled-stream counts.

## 4. Verdict: SHIP for 50 concurrent runs at 10 events/s, on conditions

Server in its own process, 50 live viewers, clients flushing every 200 ms (`test-output/loadtest-*.txt`):

| Load | Requests/s | Ingest p50 / p99 | Send → viewer p99 | Result |
|---|---|---|---|---|
| 50 runs × 10 ev/s (target) | 250 | 16 / 118 ms | 136 ms | 0 failed, 50/50 viewers = database |
| 50 × 40 ev/s | 250 | 14 / 104 ms | 119 ms | same |
| 50 × 10, flush 100 ms | 500 | 17 / 168 ms | 217 ms | same |
| 150 × 10 ev/s | 750 | 23 / **3339 ms** | **5333 ms** | correct, but pool exhausted (130 waiting) |

Cost follows **requests**, not events: four times the events at the same request rate cost nothing. The limit sits between 2× and 3× the target request rate. Past it the system got slow, not wrong.

Conditions, because the evidence is thin: 20–30 s runs on a laptop, not a soak; not the Docker image; one app instance; server CPU was 44–81% of one core across runs, which is noisy and not much headroom.

**Change first:** collapse the ingest into one database round trip (it is eight today), and tell clients to flush once a second. Then: per-project rate limit, alerts on the invariants above, and a one-hour soak in the real environment.

## 5. AI tools

The code, tests and first drafts of these documents were written with Claude Code (Anthropic) from the task brief. Places where its first output was wrong, and what exposed it:

- **Stream leak.** A write after the socket had closed waited forever, so dropped streams were never released. Exposed by one odd number in test output: "killed 478 connections" for about 90 connects. Fixed, and now asserted.
- **A load test that measured itself.** First result was ingest p50 = 1.7 s, because the load generator shared the server's process. Separated: 16–35 ms.
- **A test that could not fail the right way.** The "slow client" was silently discarding data (Node does that with no response listener) instead of refusing to read.
- **Too little chaos in the browser test** (3 disconnects, flaky threshold). Lengthened to over 20.
- Reconnect delay kept growing on a link that was still delivering data; chart thinning could hide a spike (now keeps each bucket's min and max); Fastify accepted `text/plain` bodies by default.

**What I verified myself, by hand** (seven checks with screenshots: [test-output/manual-checks.md](test-output/manual-checks.md)):

- Stopping the server: the page said "Reconnecting…", then came back "Live" with 0 gaps.
- Two projects: each list shows only its own runs, and one project opening the other's run URL gets "Not found".
- Rotating a key: the other tab was signed out and the demo client was refused straight away.
- **Found by looking, not by the tests:** a hole of about 175 steps in a chart, because the demo client drops data while the server is down and the page draws a straight line over it. Also two small display bugs (a misleading "No metrics yet" line, and header buttons that stayed visible when signed out), both fixed.

## 6. What surprised me, what still concerns me

What surprised me was how fast the system is and how strict key rotation is. Points show up on the page almost as soon as they are sent. When I rotated the key in one tab, the other tab was signed out within seconds and the demo client started failing straight away. There is no grace period at all.

What still concerns me comes from what I saw by hand. When the server was down, the demo client threw data away, and the chart drew a straight line over the hole, so a user would never know points were missing. I found two small display bugs just by looking at the pages, which the 42 automated tests had not caught, so there are probably more. And I have not seen this run in Docker yet.
