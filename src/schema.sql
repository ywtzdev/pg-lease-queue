-- pg-lease-queue schema. Safe to run more than once.
-- Requires PostgreSQL 13+ (gen_random_uuid is built in).

CREATE TABLE IF NOT EXISTS pgq_jobs (
  id               bigserial PRIMARY KEY,
  queue            text        NOT NULL,
  payload          jsonb       NOT NULL DEFAULT '{}'::jsonb,
  status           text        NOT NULL DEFAULT 'queued'
                   CHECK (status IN ('queued', 'running', 'done', 'dead')),
  attempts         integer     NOT NULL DEFAULT 0,
  max_attempts     integer     NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
  run_at           timestamptz NOT NULL DEFAULT now(),
  lease_token      uuid,
  lease_expires_at timestamptz,
  dedupe_key       text,
  last_error       text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

-- What a worker scans: jobs that are due, oldest first.
CREATE INDEX IF NOT EXISTS pgq_jobs_due
  ON pgq_jobs (queue, run_at, id) WHERE status = 'queued';

-- Leases that may have expired because a worker died.
CREATE INDEX IF NOT EXISTS pgq_jobs_leased
  ON pgq_jobs (queue, lease_expires_at) WHERE status = 'running';

-- One live job per (queue, dedupe_key). Finished jobs free the key.
CREATE UNIQUE INDEX IF NOT EXISTS pgq_jobs_dedupe
  ON pgq_jobs (queue, dedupe_key)
  WHERE dedupe_key IS NOT NULL AND status IN ('queued', 'running');
