-- All timestamps here are UTC instants, and cron expressions are always
-- evaluated against UTC wall-clock time, never a per-schedule or session
-- timezone. Cron expressions combined with local time and DST transitions
-- are a well-known source of jobs firing twice ("fall back") or not at
-- all ("spring forward") once a year. Sidestepping that by fixing
-- everything to UTC is a deliberate scope cut, not an oversight - there is
-- no per-schedule timezone support.
CREATE TABLE schedules (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue        TEXT NOT NULL,
    job_type     TEXT NOT NULL,
    payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    cron_expr    TEXT NOT NULL,
    next_run_at  TIMESTAMPTZ NOT NULL,
    last_run_at  TIMESTAMPTZ,
    enabled      BOOLEAN NOT NULL DEFAULT true,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Supports the scheduler's scan for due schedules (WHERE enabled AND
-- next_run_at <= now()). Partial for the same reason as idx_jobs_claim in
-- migration 0001: disabled schedules are never queried by this path, so
-- indexing them would only add write overhead for no read benefit.
CREATE INDEX idx_schedules_due ON schedules (next_run_at) WHERE enabled = true;
