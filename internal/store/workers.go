package store

import (
	"context"
	"fmt"
	"time"
)

// UpsertHeartbeat records that worker id is alive, running jobsInFlight
// jobs right now. Called on its own ticker from a dedicated goroutine (see
// worker.Heartbeater) so a slow job's claim-and-execute path can never
// delay it - pgxpool hands each concurrent caller its own connection, so
// this write is never queued behind one a busy job holds.
func (s *Store) UpsertHeartbeat(ctx context.Context, id, hostname string, jobsInFlight int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers (id, hostname, jobs_in_flight, last_heartbeat_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (id) DO UPDATE
		SET hostname = $2, jobs_in_flight = $3, last_heartbeat_at = now()`,
		id, hostname, jobsInFlight,
	)
	if err != nil {
		return fmt.Errorf("store: upsert heartbeat: %w", err)
	}
	return nil
}

// DeleteStaleWorkers removes worker rows whose last_heartbeat_at is older
// than threshold - a process that stopped heartbeating (crashed, killed,
// or never deregistered) without anyone cleaning up after it. Returns how
// many rows were removed, mainly so callers can log something on the rare
// occasion it's nonzero.
func (s *Store) DeleteStaleWorkers(ctx context.Context, threshold time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM workers WHERE last_heartbeat_at < $1`, threshold)
	if err != nil {
		return 0, fmt.Errorf("store: delete stale workers: %w", err)
	}
	return tag.RowsAffected(), nil
}
