# For AI agents and new contributors

Follow the house rules in [README.md](README.md#house-rules-enforced-by-testsarchitecturerulestestts). They are enforced by `tests/architecture/rules.test.ts`; fix the code, do not add an exception to the test.

- Tenant data is always filtered by the project that owns the API key. Never take a project id from the URL or body.
- Another project's resource and a missing resource must return the same `404 {"error":"not found"}`.
- Do not add a cache or an auth cache without adding isolation and key-rotation tests for it.
- `seq` must only be assigned inside the ingest transaction under the run's row lock (see DESIGN.md, "Ordering").
- Before saying a change is done: `npm run typecheck` and `npm test` (or `npm run test:local` without Docker).
- Test results go into the note as they are, including failures.
