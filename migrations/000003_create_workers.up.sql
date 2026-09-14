-- Tracks live worker processes via heartbeat. last_heartbeat_at is the
-- only source of truth for liveness - there's no separate status column,
-- because "online" vs "offline" is a threshold computed at read time
-- (dashboard, health checks), not a state that needs writing.
CREATE TABLE workers (
    id                TEXT PRIMARY KEY,
    hostname          TEXT NOT NULL,
    jobs_in_flight    INTEGER NOT NULL DEFAULT 0,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Supports DeleteStaleWorkers' scan for rows past the heartbeat TTL.
CREATE INDEX idx_workers_last_heartbeat_at ON workers (last_heartbeat_at);
