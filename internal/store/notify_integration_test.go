//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/SIDEYS/jobqueue/internal/store"
)

// listenForTest opens a dedicated LISTEN connection on store.JobEventsChannel
// and returns a function that collects the next n notifications (or times
// out), decoded to plain maps so the test can assert on exactly which keys
// are present - not just their values.
func listenForTest(t *testing.T, ctx context.Context, s *store.Store) func(n int, timeout time.Duration) []map[string]any {
	t.Helper()

	conn, err := s.Pool().Acquire(ctx)
	require.NoError(t, err)
	t.Cleanup(conn.Release)

	_, err = conn.Exec(ctx, "LISTEN "+store.JobEventsChannel)
	require.NoError(t, err)

	return func(n int, timeout time.Duration) []map[string]any {
		var got []map[string]any
		deadline := time.Now().Add(timeout)
		for len(got) < n {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			waitCtx, cancel := context.WithTimeout(ctx, remaining)
			notif, err := conn.Conn().WaitForNotification(waitCtx)
			cancel()
			if err != nil {
				break
			}
			var payload map[string]any
			require.NoError(t, json.Unmarshal([]byte(notif.Payload), &payload))
			got = append(got, payload)
		}
		return got
	}
}

// TestJobEventsNotifyOnRealWritesOnly exercises the store-level
// notification choke point across the main write paths (insert, claim,
// complete), and specifically proves the two things worth getting wrong:
// an idempotent duplicate enqueue does not publish a second event for a
// job that wasn't actually created, and every payload is the minimal
// {id, status, queue} shape - nothing from the job's own (potentially
// large, NOTIFY-payload-breaking) payload column leaks through.
func TestJobEventsNotifyOnRealWritesOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	// t.Cleanup, not defer: cleanups run LIFO after the test function
	// returns, while a defer runs immediately on return - registering
	// Close this way, before listenForTest registers its own connection
	// Release cleanup, guarantees Release runs first. Getting this
	// backwards deadlocks: Pool.Close waits for every acquired connection
	// to be released, but a bare `defer s.Close()` would run before
	// listenForTest's t.Cleanup(conn.Release) ever fires.
	t.Cleanup(s.Close)

	collect := listenForTest(t, ctx, s)

	key := "order-1"
	job, inserted, err := s.InsertJob(ctx, store.InsertJobParams{
		Queue: "notify-test", JobType: "sleep", Payload: []byte(`{}`),
		RunAt: time.Now(), MaxAttempts: 5, IdempotencyKey: &key,
	})
	require.NoError(t, err)
	require.True(t, inserted)

	evts := collect(1, 5*time.Second)
	require.Len(t, evts, 1, "a fresh insert must publish exactly one notification")
	require.Equal(t, job.ID.String(), evts[0]["id"])
	require.Equal(t, "pending", evts[0]["status"])
	require.Equal(t, "notify-test", evts[0]["queue"])
	require.Len(t, evts[0], 3, "payload must be exactly {id, status, queue} - nothing more")

	// Idempotent duplicate: same (queue, idempotency_key), hits the ON
	// CONFLICT path, not a real insert - must not publish anything.
	_, insertedAgain, err := s.InsertJob(ctx, store.InsertJobParams{
		Queue: "notify-test", JobType: "sleep", Payload: []byte(`{}`),
		RunAt: time.Now(), MaxAttempts: 5, IdempotencyKey: &key,
	})
	require.NoError(t, err)
	require.False(t, insertedAgain)

	silence := collect(1, 500*time.Millisecond)
	require.Empty(t, silence, "an idempotent duplicate enqueue must not publish a notification")

	claimed, err := s.ClaimJobs(ctx, "notify-test", 1, "worker-a")
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	evts = collect(1, 5*time.Second)
	require.Len(t, evts, 1, "claiming must publish exactly one notification")
	require.Equal(t, "running", evts[0]["status"])

	require.NoError(t, s.CompleteJob(ctx, claimed[0].ID, *claimed[0].ClaimedBy))

	evts = collect(1, 5*time.Second)
	require.Len(t, evts, 1, "completing must publish exactly one notification")
	require.Equal(t, "succeeded", evts[0]["status"])
}
