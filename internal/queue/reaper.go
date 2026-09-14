package queue

import (
	"context"
	"log"
	"time"

	"github.com/SIDEYS/jobqueue/internal/store"
)

// Clock abstracts time.Now so the reaper's staleness threshold can be
// computed deterministically in tests, without sleeping or manipulating
// real timestamps in the database.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type Reaper struct {
	store             *store.Store
	clock             Clock
	visibilityTimeout time.Duration
	batchSize         int
}

func NewReaper(s *store.Store, visibilityTimeout time.Duration) *Reaper {
	return &Reaper{
		store:             s,
		clock:             realClock{},
		visibilityTimeout: visibilityTimeout,
		batchSize:         100,
	}
}

// WithClock overrides the reaper's clock - used by tests to control what
// counts as stale without waiting or backdating rows.
func (r *Reaper) WithClock(c Clock) *Reaper {
	r.clock = c
	return r
}

// Threshold returns the claimed_at cutoff below which a running job is
// considered abandoned: anything claimed before this instant has had
// longer than visibilityTimeout to report back. Exposed as its own method,
// separate from the DB call it feeds, so the time math is testable on its
// own without a database.
func (r *Reaper) Threshold() time.Time {
	return r.clock.Now().Add(-r.visibilityTimeout)
}

// ReclaimOnce runs a single reclaim pass. See store.ReclaimStale for what
// happens to each reclaimed job.
func (r *Reaper) ReclaimOnce(ctx context.Context) ([]*store.Job, error) {
	return r.store.ReclaimStale(ctx, r.Threshold(), r.batchSize)
}

// Run calls ReclaimOnce every interval until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			jobs, err := r.ReclaimOnce(ctx)
			if err != nil {
				log.Printf("reaper: reclaim: %v", err)
				continue
			}
			if len(jobs) > 0 {
				log.Printf("reaper: reclaimed %d stale job(s)", len(jobs))
			}
		}
	}
}
