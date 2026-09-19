//go:build integration

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/SIDEYS/jobqueue/internal/scheduler"
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

// newStore opens an independent pool against connStr. Each call is a
// genuinely separate session - required for these tests, since advisory
// locks are session-scoped and a shared pool would make "3 independent
// scheduler processes" meaningless.
func newStore(t *testing.T, ctx context.Context, connStr string) *store.Store {
	t.Helper()
	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

func countLeaders(schedulers ...*scheduler.Scheduler) int {
	n := 0
	for _, sc := range schedulers {
		if sc.IsLeader() {
			n++
		}
	}
	return n
}

// TestThreeSchedulersExactlyOneEnqueuePerTick runs 3 independent scheduler
// instances against one database and one already-due schedule, and
// asserts exactly one of them becomes leader and exactly one job gets
// enqueued - not zero (no leader ever elected) and not three (every
// instance firing independently).
func TestThreeSchedulersExactlyOneEnqueuePerTick(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s1 := newStore(t, ctx, connStr)
	s2 := newStore(t, ctx, connStr)
	s3 := newStore(t, ctx, connStr)

	// Standard 5-field, fires once a minute - deliberately not the
	// sub-minute expressions the failover test uses, so there is no
	// realistic chance of a second, legitimate firing landing inside this
	// test's short observation window and muddying the "exactly one"
	// assertion.
	_, err := s1.InsertSchedule(ctx, store.InsertScheduleParams{
		Queue: "cron-test", JobType: "sleep", Payload: []byte(`{}`),
		MaxAttempts: 5, CronExpr: "* * * * *", NextRunAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	sc1 := scheduler.New(s1, scheduler.DefaultLockKey, nil)
	sc2 := scheduler.New(s2, scheduler.DefaultLockKey, nil)
	sc3 := scheduler.New(s3, scheduler.DefaultLockKey, nil)

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	const tick = 100 * time.Millisecond
	go func() { _ = sc1.Run(runCtx, tick) }()
	go func() { _ = sc2.Run(runCtx, tick) }()
	go func() { _ = sc3.Run(runCtx, tick) }()

	require.Eventually(t, func() bool {
		return countLeaders(sc1, sc2, sc3) == 1
	}, 5*time.Second, 20*time.Millisecond, "exactly one of the three instances should become leader")

	require.Eventually(t, func() bool {
		var n int
		err := s1.Pool().QueryRow(ctx, `SELECT count(*) FROM jobs WHERE queue = $1`, "cron-test").Scan(&n)
		return err == nil && n >= 1
	}, 5*time.Second, 20*time.Millisecond, "the due schedule should fire")

	// Keep both leader election and the tick loop running for many more
	// cycles across all three instances - if the "one leader" property
	// were flaky (e.g. two instances both intermittently believing they
	// lead), this window gives it room to show up as a second job.
	time.Sleep(1 * time.Second)

	var count int
	require.NoError(t, s1.Pool().QueryRow(ctx, `SELECT count(*) FROM jobs WHERE queue = $1`, "cron-test").Scan(&count))
	require.Equal(t, 1, count, "exactly one job should be enqueued for the one due schedule")
	require.Equal(t, 1, countLeaders(sc1, sc2, sc3), "still exactly one leader")
}
