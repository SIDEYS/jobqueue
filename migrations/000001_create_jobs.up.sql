CREATE TYPE job_status AS ENUM (
    'pending',
    'running',
    'succeeded',
    'failed',
    'dead'
);

CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue           TEXT NOT NULL,
    job_type        TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          job_status NOT NULL DEFAULT 'pending',
    priority        INTEGER NOT NULL DEFAULT 0,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 5,
    last_error      TEXT,
    idempotency_key TEXT UNIQUE,
    claimed_by      TEXT,
    claimed_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The claim query (see internal/store) selects the next batch of runnable
-- jobs with:
--
--   WHERE status = 'pending' AND queue = $1 AND run_at <= now()
--   ORDER BY priority DESC, run_at ASC
--   LIMIT $2
--   FOR UPDATE SKIP LOCKED
--
-- A partial index restricted to status = 'pending' is deliberate: pending
-- rows are a small, fast-changing minority of the table (most rows settle
-- into succeeded/failed/dead and are never touched by this query again), so
-- indexing the other statuses would only bloat the index and slow down
-- writes for rows this query never looks at. The column order
-- (queue, priority, run_at) matches the WHERE/ORDER BY exactly, so Postgres
-- can satisfy the whole query - lookup, ordering, and limit - with a single
-- forward index scan and no separate sort step.
CREATE INDEX idx_jobs_claim ON jobs (queue, priority DESC, run_at ASC)
    WHERE status = 'pending';

-- Supports GET /api/v1/jobs list filtering by queue/status and the queue
-- depth gauge (count of pending/running jobs per queue) added in later
-- phases.
CREATE INDEX idx_jobs_queue_status ON jobs (queue, status);
