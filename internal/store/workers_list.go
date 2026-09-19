package store

import (
	"context"
	"fmt"
	"time"
)

type Worker struct {
	ID              string
	Hostname        string
	JobsInFlight    int32
	LastHeartbeatAt time.Time
	CreatedAt       time.Time
}

// ListWorkers returns every worker row, most recently heartbeated first.
// There's no stored "online"/"offline" status to filter on - a row's mere
// presence means it's within its heartbeat TTL (DeleteStaleWorkers already
// removes anything older), and how stale to treat a given LastHeartbeatAt
// as "concerning" is a display-time judgment call, not something baked
// into a query here.
func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, hostname, jobs_in_flight, last_heartbeat_at, created_at
		FROM workers
		ORDER BY last_heartbeat_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("store: list workers: %w", err)
	}
	defer rows.Close()

	var workers []Worker
	for rows.Next() {
		var w Worker
		if err := rows.Scan(&w.ID, &w.Hostname, &w.JobsInFlight, &w.LastHeartbeatAt, &w.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: list workers: %w", err)
		}
		workers = append(workers, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list workers: %w", err)
	}
	return workers, nil
}
