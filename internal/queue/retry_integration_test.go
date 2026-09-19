//go:build integration

package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/SIDEYS/jobqueue/internal/queue"
	"github.com/SIDEYS/jobqueue/internal/store"
)

func startPostgres(t *testing.T, ctx context.Context) string {
	t.Helper()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("jobqueue"),
		postgres.WithUsername("jobqueue"),
		postgres.WithPassword("jobqueue"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ctr.Terminate(context.Background()))
	})

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return connStr
}

// TestFailRetriesThenDeadLetters exercises queue.Fail's actual
// retry-vs-dead decision - the code path worker.go calls - across a job's
// full attempt budget: two failures with attempts remaining must return it
// to pending, and the failure that hits max_attempts must dead-letter it.
func TestFailRetriesThenDeadLetters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	defer s.Close()

	q := queue.New(s)

	job, _, err := q.Enqueue(ctx, queue.EnqueueParams{
		Queue:       "retry-test",
		JobType:     "sleep",
		Payload:     []byte(`{}`),
		MaxAttempts: 3,
	})
	require.NoError(t, err)

	for attempt := int32(1); attempt <= 2; attempt++ {
		// Backoff pushes run_at into the future; force it claimable now so
		// the test doesn't wait out real backoff delays.
		_, err = s.Pool().Exec(ctx, `UPDATE jobs SET run_at = now() WHERE id = $1`, job.ID)
		require.NoError(t, err)

		claimed, err := q.Claim(ctx, "retry-test", 1, "worker")
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		require.Equal(t, attempt, claimed[0].Attempts)

		require.NoError(t, q.Fail(ctx, claimed[0], errors.New("boom")))

		after, err := q.Get(ctx, job.ID)
		require.NoError(t, err)
		require.Equalf(t, store.StatusPending, after.Status, "attempt %d of 3 should retry, not dead-letter", attempt)
	}

	// Third and final attempt: max_attempts is exhausted, this must dead-letter.
	_, err = s.Pool().Exec(ctx, `UPDATE jobs SET run_at = now() WHERE id = $1`, job.ID)
	require.NoError(t, err)

	claimed, err := q.Claim(ctx, "retry-test", 1, "worker")
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, int32(3), claimed[0].Attempts)

	require.NoError(t, q.Fail(ctx, claimed[0], errors.New("boom final")))

	final, err := q.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, store.StatusDead, final.Status)
	require.Equal(t, int32(3), final.Attempts)
	require.Contains(t, *final.LastError, "boom final")
}
