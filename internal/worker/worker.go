package worker

import (
	"context"
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

	if err := w.queue.Complete(ctx, job.ID); err != nil {
		log.Printf("worker: mark job %s succeeded: %v", job.ID.String(), err)
	}
}

func (w *Worker) fail(ctx context.Context, job *store.Job, cause error) {
	if err := w.queue.Fail(ctx, job.ID, cause.Error()); err != nil {
		log.Printf("worker: mark job %s failed: %v", job.ID.String(), err)
	}
}
