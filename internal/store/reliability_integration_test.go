//go:build integration

package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/SIDEYS/jobqueue/internal/store"
)

// TestIdempotentEnqueueUnderConcurrency fires many concurrent InsertJob
// calls with the same (queue, idempotency_key) and asserts they all
// collapse onto a single row - the property the ON CONFLICT ... DO UPDATE
// ... RETURNING statement exists to guarantee atomically, without the
// isolation-level-dependent race a separate insert-then-fetch would have.
func TestIdempotentEnqueueUnderConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	defer s.Close()

	const (
		concurrency = 20
		queueName   = "orders"
	)
	key := "order-123"

	var (
		mu       sync.Mutex
		ids      []string
		firstErr error
		wg       sync.WaitGroup
	)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, err := s.InsertJob(ctx, store.InsertJobParams{
				Queue:          queueName,
				JobType:        "charge",
				Payload:        []byte(`{}`),
				RunAt:          time.Now(),
				MaxAttempts:    5,
				IdempotencyKey: &key,
			})

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			ids = append(ids, job.ID.String())
		}()
	}
	wg.Wait()

	require.NoError(t, firstErr)
	require.Len(t, ids, concurrency)
	for _, id := range ids[1:] {
		require.Equal(t, ids[0], id, "every concurrent enqueue with the same idempotency key must return the same job id")
	}

	var count int
	err = s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM jobs WHERE queue = $1 AND idempotency_key = $2`,
		queueName, key,
	).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "exactly one row should exist despite the concurrent duplicate enqueues")
}

// backdateAsStaleClaim inserts no new row; it force-sets an existing job
// into a running state with claimed_at far in the past, standing in for
// "a worker claimed this an hour ago and never reported back" without
// sleeping in the test.
func backdateAsStaleClaim(t *testing.T, ctx context.Context, s *store.Store, id pgtype.UUID, claimedBy string, attempts int32) {
	t.Helper()
	_, err := s.Pool().Exec(ctx, `
		UPDATE jobs
		SET status = 'running', claimed_by = $2, claimed_at = $3, attempts = $4
		WHERE id = $1`,
		id, claimedBy, time.Now().Add(-time.Hour), attempts,
	)
	require.NoError(t, err)
}

// TestReclaimStaleRetriesOrDeadLettersBasedOnAttempts proves ReclaimStale's
// per-row CASE actually picks the right outcome: a job with attempts left
// goes back to pending for another try, one already at its last attempt
// goes straight to dead instead of retrying forever by repeatedly escaping
// via the visibility timeout.
func TestReclaimStaleRetriesOrDeadLettersBasedOnAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	defer s.Close()

	retryable, err := s.InsertJob(ctx, store.InsertJobParams{
		Queue: "reap", JobType: "sleep", Payload: []byte(`{}`), RunAt: time.Now(), MaxAttempts: 3,
	})
	require.NoError(t, err)
	backdateAsStaleClaim(t, ctx, s, retryable.ID, "zombie-a", 1) // 1 of 3 attempts used

	exhausted, err := s.InsertJob(ctx, store.InsertJobParams{
		Queue: "reap", JobType: "sleep", Payload: []byte(`{}`), RunAt: time.Now(), MaxAttempts: 2,
	})
	require.NoError(t, err)
	backdateAsStaleClaim(t, ctx, s, exhausted.ID, "zombie-b", 1) // 1 of 2 attempts used - next is the last

	reclaimed, err := s.ReclaimStale(ctx, time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, reclaimed, 2)

	byID := make(map[string]*store.Job, len(reclaimed))
	for _, j := range reclaimed {
		byID[j.ID.String()] = j
	}

	r, ok := byID[retryable.ID.String()]
	require.True(t, ok)
	require.Equal(t, store.StatusPending, r.Status)
	require.Equal(t, int32(2), r.Attempts)
	require.Nil(t, r.ClaimedBy)

	d, ok := byID[exhausted.ID.String()]
	require.True(t, ok)
	require.Equal(t, store.StatusDead, d.Status)
	require.Equal(t, int32(2), d.Attempts)
	require.Nil(t, d.ClaimedBy)
}

// TestZombieWriterFencing is the scenario the claimed_by fencing check
// exists for: a worker (A) claims a job, goes quiet long enough for the
// reaper to reclaim it (a GC pause or a slow downstream call, not
// necessarily a crash - A is still alive and will eventually finish and
// call Complete), a second worker (B) claims the now-pending job and
// completes it for real, and only then does A's delayed Complete call
// finally land. Without the WHERE claimed_by = $2 fencing check, A's write
// would silently overwrite B's result. With it, A's write affects zero
// rows and B's result is what survives.
func TestZombieWriterFencing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	defer s.Close()

	inserted, err := s.InsertJob(ctx, store.InsertJobParams{
		Queue: "zombie", JobType: "sleep", Payload: []byte(`{}`), RunAt: time.Now(), MaxAttempts: 5,
	})
	require.NoError(t, err)

	claimedByA, err := s.ClaimJobs(ctx, "zombie", 1, "worker-a")
	require.NoError(t, err)
	require.Len(t, claimedByA, 1)
	jobA := claimedByA[0]

	// A goes quiet - backdate its claim instead of sleeping past the
	// visibility timeout.
	_, err = s.Pool().Exec(ctx, `UPDATE jobs SET claimed_at = $2 WHERE id = $1`,
		jobA.ID, time.Now().Add(-time.Hour))
	require.NoError(t, err)

	reclaimed, err := s.ReclaimStale(ctx, time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	require.Equal(t, store.StatusPending, reclaimed[0].Status)

	claimedByB, err := s.ClaimJobs(ctx, "zombie", 1, "worker-b")
	require.NoError(t, err)
	require.Len(t, claimedByB, 1)
	jobB := claimedByB[0]
	require.Equal(t, jobA.ID, jobB.ID)

	// A, unaware it lost the claim, finally reports success. This must be
	// a no-op: zero rows affected, nothing about the row changes.
	err = s.CompleteJob(ctx, jobA.ID, *jobA.ClaimedBy)
	require.ErrorIs(t, err, store.ErrStaleClaim)

	afterStaleWrite, err := s.GetJob(ctx, inserted.ID)
	require.NoError(t, err)
	require.Equal(t, store.StatusRunning, afterStaleWrite.Status, "A's stale write must not change the job's status")
	require.Equal(t, *jobB.ClaimedBy, *afterStaleWrite.ClaimedBy, "the job must still show B as the claimant")

	// B completes it for real.
	err = s.CompleteJob(ctx, jobB.ID, *jobB.ClaimedBy)
	require.NoError(t, err)

	final, err := s.GetJob(ctx, inserted.ID)
	require.NoError(t, err)
	require.Equal(t, store.StatusSucceeded, final.Status)
	require.Equal(t, *jobB.ClaimedBy, *final.ClaimedBy)
}
