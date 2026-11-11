# Soup tracker: a mini experiment tracker

Projects and API keys, metric ingestion over an unreliable network, a run list, and a run page that updates live (SSE).

- [DESIGN.md](DESIGN.md): the one-page design.
- [NOTE.md](NOTE.md): the two-page note: results, what the tests do not catch, verdict, AI usage.
- [EVIDENCE.md](EVIDENCE.md): the long version with every table and number.
- [test-output/](test-output/): raw test and load-test output, screenshots.

## Run it

```bash
docker compose up
```

Open <http://localhost:8080>. The `demo` service prints an API key in its logs (`docker compose logs demo`); paste it into the page. It feeds three runs through a deliberately flaky client for 10 minutes.

```bash
docker compose run --rm tests       # whole suite, including the browser test
docker compose run --rm loadtest    # 50 runs x 10 events/s for 30 s
```

If port 8080 is taken on your machine: `APP_PORT=8090 docker compose up`.

Without Docker (Node 22): `npm install`, then `npm run test:local`, `npm run loadtest:local`, or `npm run dev:local` (serves <http://localhost:8090>) with `npm run demo:local` in a second terminal for demo data. These start a throwaway real Postgres via `embedded-postgres`. For the browser test also run `npx playwright install chromium`.

> The compose defaults (`ADMIN_TOKEN=dev-admin-token`, database password `soup`) are for a laptop only.

## API

All project endpoints need `Authorization: Bearer <api key>`. Errors are always `{ "error": "…" }`.

| Method | Path | |
|---|---|---|
| POST | `/admin/projects` | admin token. Creates a project, returns its API key once |
| GET | `/admin/stats` | admin token. Operational numbers |
| GET | `/project` | the key's project |
| POST | `/keys/rotate` | new key; all other keys of the project stop working immediately |
| GET, POST | `/runs` | list / create |
| GET | `/runs/{id}` | one run |
| POST | `/runs/{id}/metrics` | ingest a batch (idempotent) |
| GET | `/runs/{id}/metrics?after_seq=&limit=` | read the log in pages |
| GET | `/runs/{id}/stream` | SSE; resume with `Last-Event-ID` |
| GET | `/health` | public |

```bash
curl -s -X POST localhost:8080/admin/projects -H "authorization: Bearer dev-admin-token" \
  -H "content-type: application/json" -d '{"name":"mine"}'
```

## Layout

```
db/schema.sql            schema (idempotent, applied on boot)
src/http.ts              auth wrappers, the only way to register a route
src/ingest.ts            one batch = one transaction; dedupe; seq assignment
src/stream.ts            SSE: a cursor-based reader of the run's log
src/notify.ts            Postgres LISTEN/NOTIFY wake-ups
public/state.js, sse.js  page model and stream client (also imported by the tests)
scripts/lib/flaky-client.ts   the unreliable client used by tests, demo and load test
tests/                   consistency (API + browser), isolation, silent failures, architecture rules
```

## House rules (enforced by `tests/architecture/rules.test.ts`)

1. Register routes only with `projectRoute` / `adminRoute` / `publicRoute`. Public routes must be in `PUBLIC_ROUTES`.
2. Read request input only through `parseWith(schema, …)`.
3. SQL uses `$1` parameters. No interpolation.
4. Browser code never uses `innerHTML` or `eval`.
5. Never log headers, URLs or bodies.
6. No secrets or `.env` files in the repository.

`test-output/architecture-planted-violations.txt` shows each rule catching a planted violation.
