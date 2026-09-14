//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/SIDEYS/jobqueue/internal/store"
)

func TestUpsertHeartbeatInsertsThenUpdatesInPlace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.UpsertHeartbeat(ctx, "worker-1", "host-a", 0))

	var hostname string
	var jobsInFlight int
	var firstBeat time.Time
	require.NoError(t, s.Pool().QueryRow(ctx,
		`SELECT hostname, jobs_in_flight, last_heartbeat_at FROM workers WHERE id = $1`, "worker-1",
	).Scan(&hostname, &jobsInFlight, &firstBeat))
	require.Equal(t, "host-a", hostname)
	require.Equal(t, 0, jobsInFlight)

	require.NoError(t, s.UpsertHeartbeat(ctx, "worker-1", "host-a", 3))

	var count int
	require.NoError(t, s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM workers WHERE id = $1`, "worker-1",
	).Scan(&count))
	require.Equal(t, 1, count, "a second heartbeat for the same id must update the row, not insert another")

	var jobsInFlight2 int
	var secondBeat time.Time
	require.NoError(t, s.Pool().QueryRow(ctx,
		`SELECT jobs_in_flight, last_heartbeat_at FROM workers WHERE id = $1`, "worker-1",
	).Scan(&jobsInFlight2, &secondBeat))
	require.Equal(t, 3, jobsInFlight2)
	require.False(t, secondBeat.Before(firstBeat), "heartbeat timestamp must advance, not go backwards")
}

func TestDeleteStaleWorkersRemovesOnlyRowsPastThreshold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.UpsertHeartbeat(ctx, "fresh", "host", 0))
	require.NoError(t, s.UpsertHeartbeat(ctx, "stale", "host", 0))

	_, err = s.Pool().Exec(ctx, `UPDATE workers SET last_heartbeat_at = $2 WHERE id = $1`,
		"stale", time.Now().Add(-time.Hour))
	require.NoError(t, err)

	n, err := s.DeleteStaleWorkers(ctx, time.Now().Add(-time.Minute))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	rows, err := s.Pool().Query(ctx, `SELECT id FROM workers`)
	require.NoError(t, err)
	defer rows.Close()

	var remaining []string
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		remaining = append(remaining, id)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"fresh"}, remaining)
}
