DROP INDEX IF EXISTS idx_jobs_reap;
DROP INDEX IF EXISTS idx_jobs_queue_idempotency_key;
ALTER TABLE jobs ADD CONSTRAINT jobs_idempotency_key_key UNIQUE (idempotency_key);
