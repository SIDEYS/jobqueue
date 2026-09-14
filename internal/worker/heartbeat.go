package worker

import (
	"context"
	"log"
	"time"

	"github.com/SIDEYS/jobqueue/internal/store"
)

// Heartbeater periodically records that a worker process is alive. It runs
// on its own ticker in its own goroutine (see cmd/worker), issuing its own
// store calls independent of whatever the claim loop is doing - pgxpool
// hands each concurrent caller its own connection, so a job stuck for
// minutes never delays a heartbeat write behind it. A worker that goes
// quiet without deregistering (crash, SIGKILL) simply stops updating its
// row; DeleteStaleWorkers is what actually removes it once ttl has passed,
// there's no separate liveness signal to fail.
type Heartbeater struct {
	store    *store.Store
	id       string
	hostname string
	interval time.Duration
	ttl      time.Duration
	inFlight func() int
}

func NewHeartbeater(s *store.Store, id, hostname string, interval, ttl time.Duration, inFlight func() int) *Heartbeater {
	return &Heartbeater{
		store:    s,
		id:       id,
		hostname: hostname,
		interval: interval,
		ttl:      ttl,
		inFlight: inFlight,
	}
}

// Run beats immediately (so the worker shows up without waiting a full
// interval) and then every interval until ctx is cancelled.
func (h *Heartbeater) Run(ctx context.Context) error {
	h.beat(ctx)

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			h.beat(ctx)
		}
	}
}

func (h *Heartbeater) beat(ctx context.Context) {
	if err := h.store.UpsertHeartbeat(ctx, h.id, h.hostname, h.inFlight()); err != nil {
		log.Printf("heartbeat: upsert: %v", err)
	}

	n, err := h.store.DeleteStaleWorkers(ctx, time.Now().Add(-h.ttl))
	if err != nil {
		log.Printf("heartbeat: cleanup: %v", err)
		return
	}
	if n > 0 {
		log.Printf("heartbeat: removed %d stale worker row(s)", n)
	}
}
