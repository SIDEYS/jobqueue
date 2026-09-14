package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/SIDEYS/jobqueue/internal/queue"
	"github.com/SIDEYS/jobqueue/internal/store"
)

// Config controls a Pool's claiming and execution behavior.
type Config struct {
	QueueName   string
	WorkerID    string
	Concurrency int
	BatchSize   int
	PollBase    time.Duration
	PollMax     time.Duration
	JobTimeout  time.Duration
}

// Pool is a concurrent claim/execute loop: up to Concurrency jobs run at
// once, claimed in batches of up to BatchSize, backing off (see
// pollInterval) when the queue is empty.
type Pool struct {
	queue    *queue.Queue
	registry *Registry
	cfg      Config

	mu       sync.Mutex
	inFlight map[string]*store.Job

	wg       sync.WaitGroup
	stopOnce sync.Once
	stopCh   chan struct{}
}

func NewPool(q *queue.Queue, r *Registry, cfg Config) *Pool {
	return &Pool{
		queue:    q,
		registry: r,
		cfg:      cfg,
		inFlight: make(map[string]*store.Job),
		stopCh:   make(chan struct{}),
	}
}

// InFlightCount is the number of jobs currently executing - reported to
// the workers table by Heartbeater.
func (p *Pool) InFlightCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.inFlight)
}

// Run claims and executes jobs until ctx is cancelled or Shutdown is
// called - both are watched independently, so a caller can stop the pool
// via either one.
func (p *Pool) Run(ctx context.Context) error {
	emptyStreak := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-p.stopCh:
			return nil
		default:
		}

		capacity := p.cfg.Concurrency - p.InFlightCount()
		if capacity <= 0 {
			if !p.wait(ctx, p.cfg.PollBase) {
				return nil
			}
			continue
		}

		batch := p.cfg.BatchSize
		if batch > capacity {
			batch = capacity
		}

		jobs, err := p.queue.Claim(ctx, p.cfg.QueueName, batch, p.cfg.WorkerID)
		if err != nil {
			return fmt.Errorf("worker: claim: %w", err)
		}

		if len(jobs) == 0 {
			emptyStreak++
			if !p.wait(ctx, pollInterval(emptyStreak, p.cfg.PollBase, p.cfg.PollMax)) {
				return nil
			}
			continue
		}
		emptyStreak = 0

		for _, job := range jobs {
			p.dispatch(job)
		}
	}
}

// wait pauses for d, or returns false early if ctx or Shutdown fires.
func (p *Pool) wait(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-p.stopCh:
		return false
	case <-time.After(d):
		return true
	}
}

func (p *Pool) dispatch(job *store.Job) {
	p.mu.Lock()
	p.inFlight[job.ID.String()] = job
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() {
			p.mu.Lock()
			delete(p.inFlight, job.ID.String())
			p.mu.Unlock()
		}()
		p.execute(job)
	}()
}

func (p *Pool) execute(job *store.Job) {
	handler, ok := p.registry.Get(job.JobType)
	if !ok {
		p.report(job, fmt.Errorf("no handler registered for job_type %q", job.JobType))
		return
	}

	// Deliberately detached from Run's ctx and from Shutdown: a shutdown
	// signal must not abort an in-flight job outright. Shutdown handles
	// shutdown by waiting up to its own deadline and then releasing the
	// claim explicitly (see Shutdown) - not by cancelling this context out
	// from under a handler that may be mid-side-effect.
	jobCtx, cancel := context.WithTimeout(context.Background(), p.cfg.JobTimeout)
	defer cancel()

	err := handler(jobCtx, job.Payload)
	p.report(job, err)
}

// report writes a job's outcome using a fresh short-lived context, never
// the job's own jobCtx - that context may already be Done (it just timed
// out, which is often exactly why err is non-nil), and the outcome still
// needs to reach the database.
func (p *Pool) report(job *store.Job, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if cause != nil {
		if err := p.queue.Fail(ctx, job, cause); err != nil {
			logTerminalWrite(job.ID.String(), "failed", err)
		}
		return
	}
	if err := p.queue.Complete(ctx, job); err != nil {
		logTerminalWrite(job.ID.String(), "succeeded", err)
	}
}

// Shutdown stops the pool from claiming new work and waits for in-flight
// jobs to finish, up to ctx's deadline. If the deadline is hit first,
// every job still running has its claim released immediately - set back
// to pending, claimed_by cleared, attempts untouched - so another worker
// (or this same one, next boot) picks it up right away instead of waiting
// out the full visibility timeout. The abandoned goroutine keeps running
// in the background (Go cannot force-kill it), but that's fine: on a
// platform like Fly or ECS the process is about to be SIGKILLed anyway,
// and if that goroutine eventually reports its outcome, Phase 2's
// claimed_by fencing check already makes that write a safe no-op if
// someone else has since reclaimed the job.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.stopOnce.Do(func() { close(p.stopCh) })

	allDone := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(allDone)
	}()

	select {
	case <-allDone:
		return nil
	case <-ctx.Done():
		return p.releaseInFlight()
	}
}

func (p *Pool) releaseInFlight() error {
	p.mu.Lock()
	remaining := make([]*store.Job, 0, len(p.inFlight))
	for _, j := range p.inFlight {
		remaining = append(remaining, j)
	}
	p.mu.Unlock()

	if len(remaining) == 0 {
		return nil
	}

	// The passed-in ctx is already Done (that's why we're here) - releasing
	// still needs a live context to actually reach the database before the
	// process is killed.
	releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var firstErr error
	for _, job := range remaining {
		if err := p.queue.Release(releaseCtx, job); err != nil {
			logTerminalWrite(job.ID.String(), "released", err)
			if !errors.Is(err, queue.ErrStaleClaim) && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// logTerminalWrite distinguishes the expected case - the reaper (or a
// concurrent Shutdown) already reclaimed this job out from under its
// claimant, so this write is stale by design - from a genuine unexpected
// error.
func logTerminalWrite(jobID, outcome string, err error) {
	if errors.Is(err, queue.ErrStaleClaim) {
		log.Printf("worker: job %s attempted to record %s but its claim was already reclaimed; discarding this attempt's result", jobID, outcome)
		return
	}
	log.Printf("worker: job %s %s: %v", jobID, outcome, err)
}
