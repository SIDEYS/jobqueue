//go:build integration

package store_test

// This is the test the whole claim mechanism exists to justify: it proves
// that FOR UPDATE SKIP LOCKED does what internal/store/jobs.go's comment
// claims it does under real concurrency, against a real Postgres instance
// (via testcontainers), not a mock. Run with:
//
//	go test -tags=integration ./internal/store/...

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

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

// TestClaimJobsNoDoubleClaim enqueues 100 jobs and runs 4 claimers
// concurrently against the same table, each repeatedly pulling batches of 5
// until the queue is empty. It asserts every job is claimed by exactly one
// claimer - the property SKIP LOCKED exists to guarantee.
func TestClaimJobsNoDoubleClaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	defer s.Close()

	const (
		totalJobs   = 100
		numClaimers = 4
		claimBatch  = 5
		queueName   = "loadtest"
	)

	for i := 0; i < totalJobs; i++ {
		_, err := s.InsertJob(ctx, store.InsertJobParams{
			Queue:       queueName,
			JobType:     "sleep",
			Payload:     []byte(`{}`),
			Priority:    0,
			RunAt:       time.Now(),
			MaxAttempts: 5,
		})
		require.NoError(t, err)
	}

	var (
		mu       sync.Mutex
		claimed  = make(map[string]string) // job id -> claimer id
		firstErr error
		wg       sync.WaitGroup
	)

	// t.Fatal/require must only be called from the test's own goroutine
	// (they call FailNow, which is only safe there), so claimer goroutines
	// record failures into shared state instead and the test goroutine
	// asserts on it once every claimer has finished.
	recordErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}

	for c := 0; c < numClaimers; c++ {
		claimerID := fmt.Sprintf("claimer-%d", c)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				jobs, err := s.ClaimJobs(ctx, queueName, claimBatch, claimerID)
				if err != nil {
					recordErr(fmt.Errorf("claim by %s: %w", claimerID, err))
					return
				}
				if len(jobs) == 0 {
					return
				}

				mu.Lock()
				for _, j := range jobs {
					id := j.ID.String()
					if existing, ok := claimed[id]; ok {
						mu.Unlock()
						recordErr(fmt.Errorf("job %s claimed twice: by %s and again by %s", id, existing, claimerID))
						return
					}
					claimed[id] = claimerID
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	require.NoError(t, firstErr)
	require.Len(t, claimed, totalJobs, "every enqueued job must be claimed exactly once")

	var stillPending int
	err = s.Pool().QueryRow(ctx, `SELECT count(*) FROM jobs WHERE queue = $1 AND status = 'pending'`, queueName).Scan(&stillPending)
	require.NoError(t, err)
	require.Zero(t, stillPending, "no job should be left pending after all claimers drained the queue")

	var running int
	err = s.Pool().QueryRow(ctx, `SELECT count(*) FROM jobs WHERE queue = $1 AND status = 'running'`, queueName).Scan(&running)
	require.NoError(t, err)
	require.Equal(t, totalJobs, running, "every claimed job must be marked running")
}
