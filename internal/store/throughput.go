package store

import (
	"context"
	"fmt"
	"time"
)

type ThroughputBucket struct {
	Minute    time.Time
	Succeeded int64
}

// Throughput returns succeeded-job counts grouped by the minute they
// completed in, for every minute from since to now - including minutes
// with zero completions, so a chart built directly off this has a
// continuous x-axis instead of gaps wherever nothing finished. The
// zero-filling happens here rather than via a SQL generate_series join:
// simpler to read, and the row count this deals with (one hour's worth of
// minutes) is trivial either way.
//
// Keyed off updated_at, not created_at: that's the column every terminal
// write (CompleteJob) actually sets to now() at the moment of success, so
// it's the true completion time - created_at would measure enqueue time
// instead, which is a different metric.
func (s *Store) Throughput(ctx context.Context, since time.Time) ([]ThroughputBucket, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT date_trunc('minute', updated_at) AS minute, count(*)
		FROM jobs
		WHERE status = 'succeeded' AND updated_at >= $1
		GROUP BY minute
		ORDER BY minute`, since)
	if err != nil {
		return nil, fmt.Errorf("store: throughput: %w", err)
	}
	defer rows.Close()

	counts := make(map[time.Time]int64)
	for rows.Next() {
		var minute time.Time
		var count int64
		if err := rows.Scan(&minute, &count); err != nil {
			return nil, fmt.Errorf("store: throughput: %w", err)
		}
		counts[minute.UTC()] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: throughput: %w", err)
	}

	start := since.UTC().Truncate(time.Minute)
	end := time.Now().UTC().Truncate(time.Minute)
	buckets := make([]ThroughputBucket, 0, int(end.Sub(start)/time.Minute)+1)
	for m := start; !m.After(end); m = m.Add(time.Minute) {
		buckets = append(buckets, ThroughputBucket{Minute: m, Succeeded: counts[m]})
	}
	return buckets, nil
}
