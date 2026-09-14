package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/SIDEYS/jobqueue/internal/queue"
	"github.com/SIDEYS/jobqueue/internal/store"
)

// Worker is a single-job-at-a-time claim/execute loop. Concurrency,
// batching, and backoff on an empty queue are added in a later phase; this
// is deliberately the simplest thing that can claim a job over HTTP-free
// SQL and run it to completion.
type Worker struct {
	queue     *queue.Queue
	registry  *Registry
	queueName string
	id        string
}

func New(q *queue.Queue, r *Registry, queueName, id string) *Worker {
	return &Worker{queue: q, registry: r, queueName: queueName, id: id}
}

// Run polls for work until ctx is cancelled, waiting pollInterval between
// empty claims.
func (w *Worker) Run(ctx context.Context, pollInterval time.Duration) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		jobs, err := w.queue.Claim(ctx, w.queueName, 1, w.id)
		if err != nil {
			return fmt.Errorf("worker: claim: %w", err)
		}

		if len(jobs) == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(pollInterval):
			}
			continue
		}

		w.execute(ctx, jobs[0])
	}
}

func (w *Worker) execute(ctx context.Context, job *store.Job) {
	handler, ok := w.registry.Get(job.JobType)
	if !ok {
		w.fail(ctx, job, fmt.Errorf("no handler registered for job_type %q", job.JobType))
		return
	}

	if err := handler(ctx, job.Payload); err != nil {
		w.fail(ctx, job, err)
		return
	}

	if err := w.queue.Complete(ctx, job); err != nil {
		logTerminalWriteErr(job.ID.String(), "succeeded", err)
	}
}

func (w *Worker) fail(ctx context.Context, job *store.Job, cause error) {
	if err := w.queue.Fail(ctx, job, cause); err != nil {
		logTerminalWriteErr(job.ID.String(), "failed", err)
	}
}

// logTerminalWriteErr distinguishes the expected case - the reaper already
// reclaimed this job out from under a worker that took too long to report
// back, so its own completion/failure write is stale by design - from a
// genuine unexpected error.
func logTerminalWriteErr(jobID, outcome string, err error) {
	if errors.Is(err, queue.ErrStaleClaim) {
		log.Printf("worker: job %s reported %s but its claim was already reclaimed; discarding this attempt's result", jobID, outcome)
		return
	}
	log.Printf("worker: mark job %s %s: %v", jobID, outcome, err)
}
