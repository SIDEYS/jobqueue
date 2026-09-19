package store

import (
	"context"
	"fmt"
)

type QueueStatRow struct {
	Queue  string
	Status JobStatus
	Count  int64
}

// QueueStats returns job counts grouped by (queue, status) across every
// status, not just pending/running - this is a user-triggered read for
// the API/dashboard, not a Prometheus collector polled every 15s (see
// metrics.dbCollectors, which does restrict to pending/running for
// exactly that reason). It still rides idx_jobs_queue_status (migration
// 0001), which covers every status, not just the partial pending index -
// grouping/ordering by (queue, status) is exactly what that index supports.
func (s *Store) QueueStats(ctx context.Context) ([]QueueStatRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT queue, status, count(*)
		FROM jobs
		GROUP BY queue, status
		ORDER BY queue, status`)
	if err != nil {
		return nil, fmt.Errorf("store: queue stats: %w", err)
	}
	defer rows.Close()

	var stats []QueueStatRow
	for rows.Next() {
		var r QueueStatRow
		if err := rows.Scan(&r.Queue, &r.Status, &r.Count); err != nil {
			return nil, fmt.Errorf("store: queue stats: %w", err)
		}
		stats = append(stats, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: queue stats: %w", err)
	}
	return stats, nil
}
