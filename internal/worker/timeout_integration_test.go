//go:build integration

package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/SIDEYS/jobqueue/internal/queue"
	"github.com/SIDEYS/jobqueue/internal/store"
	"github.com/SIDEYS/jobqueue/internal/worker"
)

// TestPoolJobTimeoutFailsThenDeadLetters proves a job whose handler never
// returns on its own still follows the normal retry/dead-letter path: the
// per-job context.WithTimeout expires, the handler's ctx.Done() branch
// returns that as an error, and Pool.report writes the outcome using its
// own fresh context rather than the now-expired job context - if it
// reused the job's own context here, the write to the database would fail
// with context.DeadlineExceeded instead of ever recording the failure.
func TestPoolJobTimeoutFailsThenDeadLetters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	defer s.Close()

	q := queue.New(s)

	reg := worker.NewRegistry()
	reg.Register("hang", func(ctx context.Context, payload []byte) error {
		<-ctx.Done() // only ever returns via the per-job timeout firing
		return ctx.Err()
	})

	pool := worker.NewPool(q, reg, worker.Config{
		QueueName:   "timeout-test",
		WorkerID:    "pool-timeout",
		Concurrency: 1,
		BatchSize:   1,
		PollBase:    50 * time.Millisecond,
		PollMax:     200 * time.Millisecond,
		JobTimeout:  200 * time.Millisecond,
	})

	inserted, _, err := q.Enqueue(ctx, queue.EnqueueParams{
		Queue: "timeout-test", JobType: "hang", Payload: []byte(`{}`), MaxAttempts: 2,
	})
	require.NoError(t, err)

	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx) }()

	require.Eventually(t, func() bool {
		j, err := q.Get(ctx, inserted.ID)
		return err == nil && j.Status == store.StatusPending && j.Attempts == 1
	}, 5*time.Second, 50*time.Millisecond, "job should time out on attempt 1 and return to pending with an attempt remaining")

	afterFirstTimeout, err := q.Get(ctx, inserted.ID)
	require.NoError(t, err)
	require.NotNil(t, afterFirstTimeout.LastError)
	require.Contains(t, *afterFirstTimeout.LastError, "context deadline exceeded")

	require.Eventually(t, func() bool {
		j, err := q.Get(ctx, inserted.ID)
		return err == nil && j.Status == store.StatusDead
	}, 5*time.Second, 50*time.Millisecond, "job should dead-letter once max_attempts is exhausted via repeated timeouts")

	require.NoError(t, pool.Shutdown(context.Background()))
	require.NoError(t, <-runDone)
}
