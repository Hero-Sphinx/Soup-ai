# Design (one page)

**Idea in one line:** each run is an append-only log in Postgres; the live page is a reader of that log that remembers how far it got. Everything else follows from that.

## Event format

```json
POST /runs/{id}/metrics      Authorization: Bearer sk_…
{ "events": [ { "key": "train/loss", "step": 120, "value": 0.31, "ts": "2026-11-11T10:00:00Z" } ] }
→ 200 { "accepted": 1, "duplicates": 0, "conflicts": [], "last_seq": 4812 }
```

- **Identity** of a point is `(run, key, step)`, chosen by the client. It is the primary key.
- Max 1000 events and 1 MB per batch. Unknown fields, non-finite numbers and bad key names reject the whole batch (400).

## Ordering: two different orders, on purpose

| Order | Who assigns it | Used for |
|---|---|---|
| `step` | the client | where a point sits on the chart. Arrival order is irrelevant: a late point lands in its place. |
| `seq` | the server, per run, at commit | delivery: "what have I not sent this viewer yet". |

`seq` is assigned inside the ingest transaction while holding a row lock on the run (`SELECT … FOR UPDATE`). Writers to one run are therefore serialized, so the committed seqs of a run are always exactly `1..last_seq`: no holes, and no seq that becomes visible after a higher one. That property is what makes "give me everything after N" safe. A global database sequence would not have it (transaction 2 can commit before transaction 1), and would also leak other projects' activity through the gaps.

## The same batch sent again

One batch is one transaction. For each event: new → stored and given the next seq; already stored with the same value → counted as `duplicates`, nothing changes; already stored with a different value → **first write wins**, the event is returned in `conflicts` and recorded in `metric_conflicts`. So a resend is a no-op that returns 200, whether the first attempt's response was lost, the request was aborted halfway, or two copies race each other. The client rule is simply: retry until you get a 200, then check `accepted + duplicates + conflicts == sent`.

## Catching up after a disconnect

`GET /runs/{id}/stream` (SSE) takes a cursor (`Last-Event-ID`) and runs one loop: read rows with `seq > cursor` in pages of 500, write them, advance, wait. First load (cursor 0), live updates and reconnect are that same loop. Postgres `NOTIFY` only wakes the loop; it carries no data, and every stream also re-reads on a 3 s timer, so a lost notification costs latency, never data.

- The server keeps no per-viewer queue, so there is nothing to overflow. A slow viewer just is not read for; one that accepts no bytes for 15 s is disconnected and later resumes from its cursor.
- The client resumes from the last seq it has **applied**, not the last it saw on the wire, and only applies complete frames.
- After each drain the server sends `sync {seq}`. The page shows "Live" only when its own seq equals it. A skipped seq is counted on the page ("Gaps detected") and forces a re-read.
- The page model is a map keyed by `(key, step)`, so replays and reordering cannot change the result.

## Project isolation

- A request's project comes from the API key and from nothing else (never the URL or body). Keys are 256-bit random, stored as SHA-256, shown once.
- Every query on tenant data has `project_id = $key's project` in its `WHERE`. Another project's run and a non-existent run take the same code path and return the same bytes: `404 {"error":"not found"}`.
- IDs are random UUIDs; malformed IDs are rejected before reaching the database.
- A stream is authorized when opened, re-checked every 3 s, and closed immediately when its key is rotated. Rotation has no grace period and there is no auth cache.
- There is **no server-side cache** at all; responses carry `Cache-Control: no-store`.
- Routes can only be registered through `projectRoute` / `adminRoute` / `publicRoute`; a test fails the build if a route bypasses them or a public route is not on the allowlist (`GET /health`).
- Logs contain the route pattern, status, project id and key id. Never the URL, headers or body.

## Weakest points (known)

1. The page loads a run's whole history; fine for thousands of points, not for millions (needs server-side downsampling).
2. Eight database round trips per batch, and each viewer does its own read per batch.
3. No per-project rate limit: one project can slow everyone down (availability is not isolated).
4. First-write-wins means a client cannot correct a value it already sent.
