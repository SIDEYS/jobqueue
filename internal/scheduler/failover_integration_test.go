//go:build integration

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/SIDEYS/jobqueue/internal/scheduler"
	"github.com/SIDEYS/jobqueue/internal/store"
)

// instance bundles a Scheduler with its own cancellable context, so the
// test can stop exactly one of three running instances independently of
// the others - simulating one replica crashing or being redeployed while
// its peers keep going.
type instance struct {
	sc     *scheduler.Scheduler
	cancel context.CancelFunc
	done   chan error
}

func startInstance(s *store.Store, tick time.Duration) *instance {
	sc := scheduler.New(s, scheduler.DefaultLockKey)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sc.Run(ctx, tick) }()
	return &instance{sc: sc, cancel: cancel, done: done}
}

// stop cancels the instance's context and waits for Run to actually
// return, so the test can assert the old leader's goroutine is genuinely
// gone - not just that its writes would be harmless even if it weren't
// (they would be: RunSchedule's atomic gate makes a zombie leader's
// writes safe regardless, by design - see docs/adr/0001. This checks the
// separate property that shutdown actually stops the loop, not just that
// the system tolerates it if it didn't).
func (i *instance) stop(t *testing.T) {
	t.Helper()
	i.cancel()
	select {
	case err := <-i.done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler did not stop within 5s of cancellation")
	}
	require.False(t, i.sc.IsLeader(), "a stopped scheduler must not still consider itself leader")
}

// TestLeaderFailoverContinuity proves more than "someone else takes over":
// it asserts the schedule's actual firing cadence has no gap and no
// duplicate across the handover, by inspecting the enqueued jobs'
// timestamps directly rather than predicting an expected count from
// wall-clock duration (which would make this test timing-fragile in
// exactly the way sleep-based timing assertions are).
func TestLeaderFailoverContinuity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s1 := newStore(t, ctx, connStr)
	s2 := newStore(t, ctx, connStr)
	s3 := newStore(t, ctx, connStr)

	const period = 1 * time.Second
	_, err := s1.InsertSchedule(ctx, store.InsertScheduleParams{
		Queue: "failover-test", JobType: "sleep", Payload: []byte(`{}`),
		MaxAttempts: 5, CronExpr: "*/1 * * * * *", NextRunAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	const tick = 100 * time.Millisecond
	instances := []*instance{
		startInstance(s1, tick),
		startInstance(s2, tick),
		startInstance(s3, tick),
	}

	leaderOf := func() *instance {
		for _, inst := range instances {
			if inst.sc.IsLeader() {
				return inst
			}
		}
		return nil
	}

	require.Eventually(t, func() bool {
		return leaderOf() != nil
	}, 5*time.Second, 20*time.Millisecond, "a leader should be elected")

	// Let the first leader accumulate several firings before killing it.
	time.Sleep(3 * time.Second)

	firstLeader := leaderOf()
	require.NotNil(t, firstLeader)
	firstLeader.stop(t)

	survivors := make([]*instance, 0, 2)
	for _, inst := range instances {
		if inst != firstLeader {
			survivors = append(survivors, inst)
		}
	}

	require.Eventually(t, func() bool {
		for _, inst := range survivors {
			if inst.sc.IsLeader() {
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond, "one of the surviving instances should take over leadership")

	// Let the new leader accumulate several firings of its own.
	time.Sleep(3 * time.Second)

	for _, inst := range survivors {
		inst.stop(t)
	}

	rows, err := s1.Pool().Query(ctx,
		`SELECT created_at FROM jobs WHERE queue = $1 ORDER BY created_at ASC`, "failover-test")
	require.NoError(t, err)
	defer rows.Close()

	var firings []time.Time
	for rows.Next() {
		var ts time.Time
		require.NoError(t, rows.Scan(&ts))
		firings = append(firings, ts)
	}
	require.NoError(t, rows.Err())

	// ~6s of firing once a second, across a leader handover, should
	// produce firings comfortably into the low single digits at minimum -
	// a much lower bar than the ~6 a perfectly gapless run would produce,
	// to leave room for scheduling jitter without weakening what actually
	// matters: the spacing check below.
	require.GreaterOrEqualf(t, len(firings), 4, "expected several firings spanning both leaders, got %d", len(firings))

	// The gap between firing 0 and firing 1 is excluded on purpose: the
	// schedule's initial next_run_at is time.Now() at insert time, an
	// arbitrary sub-second offset, not aligned to a whole-second boundary
	// the way "*/1 * * * * *" 's firings are. So firing 0 lands whenever
	// leader election finishes, and firing 1 lands at the next aligned
	// second boundary after that - anywhere from just under 0s to just
	// under 1s later, legitimately. Every firing from 1 onward is between
	// two aligned boundaries and should be a clean ~1s apart regardless.
	for i := 2; i < len(firings); i++ {
		delta := firings[i].Sub(firings[i-1])
		require.GreaterOrEqualf(t, delta, period/2, "firing %d and %d are only %s apart - looks like a duplicate fire", i-1, i, delta)
		require.LessOrEqualf(t, delta, period*5/2, "firing %d and %d are %s apart - looks like a missed tick around the handover", i-1, i, delta)
	}
}
