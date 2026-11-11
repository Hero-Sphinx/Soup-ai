-- Single source of truth for the schema. Idempotent: safe to apply on every boot.

CREATE TABLE IF NOT EXISTS projects (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name        text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Only the SHA-256 of a key is stored. Keys are 256 bits of randomness, so a
-- fast hash is enough (there is nothing to brute-force) and lookup is one
-- index probe on the hash.
CREATE TABLE IF NOT EXISTS api_keys (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id  uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  key_hash    text NOT NULL UNIQUE,
  created_at  timestamptz NOT NULL DEFAULT now(),
  revoked_at  timestamptz
);
CREATE INDEX IF NOT EXISTS api_keys_project_idx ON api_keys (project_id);

-- last_seq is the head of this run's event log. It is only ever changed in
-- the same transaction that inserts the metrics, under a row lock on the run.
CREATE TABLE IF NOT EXISTS runs (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id  uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  name        text NOT NULL,
  last_seq    bigint NOT NULL DEFAULT 0,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS runs_project_idx ON runs (project_id, created_at DESC);

-- Identity of a point is (run_id, key, step): that is what makes resends
-- harmless. seq is the per-run delivery order: contiguous 1..last_seq.
CREATE TABLE IF NOT EXISTS metrics (
  run_id      uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  key         text NOT NULL,
  step        bigint NOT NULL,
  value       double precision NOT NULL,
  client_ts   timestamptz,
  seq         bigint NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, key, step),
  UNIQUE (run_id, seq)
);

-- A resend of an existing (key, step) with a DIFFERENT value. First write
-- wins; the disagreement is recorded here instead of being dropped silently.
CREATE TABLE IF NOT EXISTS metric_conflicts (
  id             bigserial PRIMARY KEY,
  run_id         uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  key            text NOT NULL,
  step           bigint NOT NULL,
  stored_value   double precision NOT NULL,
  rejected_value double precision NOT NULL,
  received_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS metric_conflicts_run_idx ON metric_conflicts (run_id);
