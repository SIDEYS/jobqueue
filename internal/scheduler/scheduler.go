package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/SIDEYS/jobqueue/internal/metrics"
	"github.com/SIDEYS/jobqueue/internal/store"
)

// DefaultLockKey is the advisory lock key identifying scheduler
// leadership for this application. Any int64 works, as long as every
// instance uses the same one and it doesn't collide with some other
// unrelated advisory-lock user on the same database - this is the ASCII
// bytes of "jobq" read as a 32-bit int, chosen only to be memorable.
const DefaultLockKey int64 = 0x6a6f6271

type Scheduler struct {
	store   *store.Store
	elector *LeaderElector
}

func New(s *store.Store, lockKey int64) *Scheduler {
	return &Scheduler{
		store:   s,
		elector: NewLeaderElector(s.Pool(), lockKey),
	}
}

// IsLeader reports whether this instance currently believes it holds
// leadership. Best-effort - see LeaderElector.
func (sc *Scheduler) IsLeader() bool {
	return sc.elector.IsLeader()
}

// Run ticks every interval until ctx is cancelled, at which point it
// releases leadership (if held) before returning.
func (sc *Scheduler) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			sc.elector.Close(context.Background())
			return nil
		case <-ticker.C:
			sc.tick(ctx)
		}
	}
}

func (sc *Scheduler) tick(ctx context.Context) {
	isLeader, err := sc.elector.EnsureLeadership(ctx)
	if err != nil {
		log.Printf("scheduler: leader election: %v", err)
		return
	}
	if !isLeader {
		return
	}

	now := time.Now().UTC()
	due, err := sc.store.DueSchedules(ctx, now, 100)
	if err != nil {
		log.Printf("scheduler: scan due schedules: %v", err)
		return
	}

	for _, s := range due {
		sc.runOne(ctx, s, now)
	}
}

func (sc *Scheduler) runOne(ctx context.Context, s *store.Schedule, now time.Time) {
	// Always computed forward from now, never from s.LastRunAt or the
	// stale s.NextRunAt - this is the missed-tick policy. If the scheduler
	// was down for an hour and this schedule fires every five minutes, it
	// fires once on the next tick and jumps straight to its next real
	// future occurrence, rather than replaying twelve backlogged runs.
	// That's the standard at-most-once-per-interval cron semantic, and the
	// same thundering-herd concern the Phase 2 retry jitter exists for:
	// a deploy that takes a few minutes must not produce a burst of
	// catch-up jobs the moment the scheduler comes back.
	next, err := NextRun(s.CronExpr, now)
	if err != nil {
		log.Printf("scheduler: schedule %s: %v", s.ID.String(), err)
		return
	}

	// RunSchedule bypasses queue.Enqueue entirely (it's a store-level
	// transaction, not a per-job domain call), so this is also where a
	// scheduler-fired job's enqueued metric gets recorded - nothing else
	// sees this write to record it otherwise.
	_, ran, err := sc.store.RunSchedule(ctx, s.ID, next, store.ScheduledJobParams{
		Queue:       s.Queue,
		JobType:     s.JobType,
		Payload:     s.Payload,
		Priority:    s.Priority,
		MaxAttempts: s.MaxAttempts,
	})
	if err != nil {
		log.Printf("scheduler: schedule %s: run: %v", s.ID.String(), err)
		return
	}
	if ran {
		metrics.RecordEnqueued(s.Queue, s.JobType)
		log.Printf("scheduler: fired schedule %s, next run %s", s.ID.String(), next)
	}
}
