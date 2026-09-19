//go:build integration

package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/SIDEYS/jobqueue/internal/queue"
	"github.com/SIDEYS/jobqueue/internal/store"
	"github.com/SIDEYS/jobqueue/internal/worker"
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

// TestPoolShutdownReleasesInFlightJob is the test the drain-then-release
// design in Pool.Shutdown exists to prove: a job mid-execution when the
// drain deadline expires must come back as immediately claimable pending
// work, not get lost or stuck in running - and the original goroutine's
// eventual, stale completion report must not clobber whoever claims it
// next. It calls Shutdown directly, not via an OS signal, precisely so
// this is a deterministic method call instead of a flaky subprocess test.
func TestPoolShutdownReleasesInFlightJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	defer s.Close()

	q := queue.New(s)

	started := make(chan struct{})
	unblock := make(chan struct{})

	reg := worker.NewRegistry()
	reg.Register("blocking", func(ctx context.Context, payload []byte) error {
		close(started)
		select {
		case <-unblock:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	pool := worker.NewPool(q, reg, worker.Config{
		QueueName:   "shutdown-test",
		WorkerID:    "pool-a",
		Concurrency: 1,
		BatchSize:   1,
		PollBase:    50 * time.Millisecond,
		PollMax:     200 * time.Millisecond,
		JobTimeout:  time.Minute, // long enough not to fire during this test
	}, nil)

	inserted, _, err := q.Enqueue(ctx, queue.EnqueueParams{
		Queue: "shutdown-test", JobType: "blocking", Payload: []byte(`{}`), MaxAttempts: 5,
	})
	require.NoError(t, err)

	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	claimed, err := q.Get(ctx, inserted.ID)
	require.NoError(t, err)
	require.Equal(t, store.StatusRunning, claimed.Status)
	require.Equal(t, int32(1), claimed.Attempts)
	originalClaimant := *claimed.ClaimedBy

	// Deadline is well short of the handler's block - it never returns on
	// its own during this test, so hitting the deadline is the only way
	// Shutdown can return.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer shutdownCancel()
	require.NoError(t, pool.Shutdown(shutdownCtx))

	released, err := q.Get(ctx, inserted.ID)
	require.NoError(t, err)
	require.Equal(t, store.StatusPending, released.Status, "job must be released back to pending, not left running")
	require.Nil(t, released.ClaimedBy)
	require.Equal(t, int32(1), released.Attempts, "release must not charge an attempt")
	require.WithinDuration(t, time.Now(), released.RunAt, 2*time.Second, "released job must be immediately claimable, not backed off")

	// The job isn't lost: a second claimer can pick it up and complete it
	// right away.
	claimedByB, err := q.Claim(ctx, "shutdown-test", 1, "pool-b")
	require.NoError(t, err)
	require.Len(t, claimedByB, 1)
	require.NotEqual(t, originalClaimant, *claimedByB[0].ClaimedBy)
	require.NoError(t, q.Complete(ctx, claimedByB[0]))

	// Let A's abandoned handler goroutine finish, and wait for it - not just
	// Run() itself, which already exited when Shutdown first fired - so the
	// test doesn't tear its container down while that goroutine's stale
	// completion report is still in flight. A second Shutdown call is a
	// clean way to do that: stopping is idempotent (stopOnce), and with no
	// deadline on this ctx it can only return via wg reaching zero, i.e.
	// once every dispatched goroutine - including A's - has fully finished.
	close(unblock)
	require.NoError(t, pool.Shutdown(context.Background()))
	require.NoError(t, <-runDone)

	final, err := q.Get(ctx, inserted.ID)
	require.NoError(t, err)
	require.Equal(t, store.StatusSucceeded, final.Status)
	require.Equal(t, *claimedByB[0].ClaimedBy, *final.ClaimedBy, "B's completion must be what survives, not a stale write from A")
}

// TestPoolSurvivesTransientClaimError proves a claim error - a dropped
// connection, a database restart - gets logged and retried, not treated as
// fatal. Closing the pool out from under Run is a fast, deterministic stand-in
// for a real connection drop: every subsequent Claim fails immediately with
// "closed pool", exactly like a live database blip would surface once
// pgxpool's own dial retries are exhausted. Before this test's underlying
// fix, that error propagated straight out of Run and the process exited -
// making a worker the one long-running loop in this codebase that died to a
// transient DB error instead of riding it out, unlike the reaper, the
// heartbeater, and the scheduler's leader connection.
func TestPoolSurvivesTransientClaimError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)

	q := queue.New(s)
	reg := worker.NewRegistry()

	pool := worker.NewPool(q, reg, worker.Config{
		QueueName:   "claim-error-test",
		WorkerID:    "pool-a",
		Concurrency: 1,
		BatchSize:   1,
		PollBase:    20 * time.Millisecond,
		PollMax:     50 * time.Millisecond,
		JobTimeout:  time.Second,
	}, nil)

	runCtx, runCancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(runCtx) }()

	// Force every subsequent Claim to fail immediately.
	s.Close()

	// Run must keep looping through repeated claim errors, not exit on the
	// first one.
	select {
	case err := <-runDone:
		t.Fatalf("Run exited on a claim error instead of retrying it (err=%v)", err)
	case <-time.After(500 * time.Millisecond):
	}

	// It must still be responsive to cancellation once the error condition
	// isn't fatal on its own.
	runCancel()
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}
}
