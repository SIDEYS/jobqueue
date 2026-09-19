package worker

import (
	"context"
	"log/slog"
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
	log      *slog.Logger
}

func NewHeartbeater(s *store.Store, id, hostname string, interval, ttl time.Duration, inFlight func() int, log *slog.Logger) *Heartbeater {
	if log == nil {
		log = slog.Default()
	}
	return &Heartbeater{
		store:    s,
		id:       id,
		hostname: hostname,
		interval: interval,
		ttl:      ttl,
		inFlight: inFlight,
		log:      log,
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
		h.log.Error("heartbeat: upsert", "error", err)
	}

	n, err := h.store.DeleteStaleWorkers(ctx, time.Now().Add(-h.ttl))
	if err != nil {
		h.log.Error("heartbeat: cleanup", "error", err)
		return
	}
	if n > 0 {
		h.log.Info("heartbeat: removed stale worker rows", "count", n)
	}
}
