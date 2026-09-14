-- The plain UNIQUE(idempotency_key) constraint from migration 0001 already
-- permitted multiple NULLs - Postgres treats NULL as distinct for
-- uniqueness purposes, so it never broke jobs with no idempotency key. It's
-- replaced here for two other reasons:
--   1. Scope: idempotency keys are unique per queue, not globally, so two
--      unrelated queues can each use a natural key like "order-123"
--      without coordinating a shared namespace.
--   2. Index size: a partial index (WHERE idempotency_key IS NOT NULL)
--      only indexes the minority of jobs that actually set this column -
--      same rationale as idx_jobs_claim in migration 0001.
ALTER TABLE jobs DROP CONSTRAINT jobs_idempotency_key_key;

CREATE UNIQUE INDEX idx_jobs_queue_idempotency_key
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Supports the reaper's scan for running jobs whose claim has gone stale
-- (claimed_at older than the visibility timeout). Partial on status =
-- 'running' for the same reason as idx_jobs_claim: it's a small,
-- fast-changing minority of rows, and the reaper never looks at any other
-- status.
CREATE INDEX idx_jobs_reap ON jobs (claimed_at)
    WHERE status = 'running';
