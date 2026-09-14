// Package queue is the domain layer between HTTP/worker code and the
// store: it fills in defaults, validates input, and translates store
// errors into queue-level ones, but every SQL statement still lives in
// internal/store.
package queue

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/SIDEYS/jobqueue/internal/store"
)

var (
	ErrNotFound      = store.ErrNotFound
	ErrStaleClaim    = store.ErrStaleClaim
	ErrNotReplayable = store.ErrNotReplayable
)

const DefaultMaxAttempts = 5

type Queue struct {
	store *store.Store

	// rngMu guards rng: math/rand's *rand.Rand is not safe for concurrent
	// use (unlike the deprecated top-level package functions), and Fail is
	// reachable from multiple worker goroutines once the worker pool gains
	// concurrency.
	rngMu sync.Mutex
	rng   *rand.Rand
}

func New(s *store.Store) *Queue {
	return &Queue{
		store: s,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type EnqueueParams struct {
	Queue          string
	JobType        string
	Payload        []byte
	Priority       int32
	RunAt          time.Time
	MaxAttempts    int32
	IdempotencyKey *string
}

func (q *Queue) Enqueue(ctx context.Context, p EnqueueParams) (*store.Job, error) {
	if p.Queue == "" {
		return nil, errors.New("queue: queue name is required")
	}
	if p.JobType == "" {
		return nil, errors.New("queue: job_type is required")
	}
	if p.RunAt.IsZero() {
		p.RunAt = time.Now()
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = DefaultMaxAttempts
	}
	return q.store.InsertJob(ctx, store.InsertJobParams{
		Queue:          p.Queue,
		JobType:        p.JobType,
		Payload:        p.Payload,
		Priority:       p.Priority,
		RunAt:          p.RunAt,
		MaxAttempts:    p.MaxAttempts,
		IdempotencyKey: p.IdempotencyKey,
	})
}

func (q *Queue) Get(ctx context.Context, id pgtype.UUID) (*store.Job, error) {
	return q.store.GetJob(ctx, id)
}

// Claim atomically hands up to limit pending jobs in queue to workerID. See
// store.ClaimJobs for why this is safe under concurrent callers.
func (q *Queue) Claim(ctx context.Context, queueName string, limit int, workerID string) ([]*store.Job, error) {
	return q.store.ClaimJobs(ctx, queueName, limit, workerID)
}

// Complete marks a claimed job succeeded. job must be a row returned by
// Claim - its ClaimedBy is used as the fencing token, so a worker that has
// since had its claim reclaimed by the reaper writes nothing instead of
// clobbering a newer attempt. See store.ErrStaleClaim.
func (q *Queue) Complete(ctx context.Context, job *store.Job) error {
	claimedBy, err := claimant(job)
	if err != nil {
		return err
	}
	return q.store.CompleteJob(ctx, job.ID, claimedBy)
}

// Fail records that a claimed job's handler returned cause, and decides
// whether it gets another attempt or is dead-lettered. job must be a row
// returned by Claim: its Attempts (already incremented by the claim) and
// MaxAttempts decide the outcome, and its ClaimedBy is the fencing token
// for the write - see Complete and store.ErrStaleClaim.
func (q *Queue) Fail(ctx context.Context, job *store.Job, cause error) error {
	claimedBy, err := claimant(job)
	if err != nil {
		return err
	}

	if job.Attempts >= job.MaxAttempts {
		return q.store.MarkDead(ctx, job.ID, claimedBy, cause.Error())
	}

	q.rngMu.Lock()
	delay := Backoff(int(job.Attempts), q.rng)
	q.rngMu.Unlock()

	return q.store.MarkFailedForRetry(ctx, job.ID, claimedBy, cause.Error(), time.Now().Add(delay))
}

func claimant(job *store.Job) (string, error) {
	if job.ClaimedBy == nil {
		return "", fmt.Errorf("queue: job %s has no claimant - it did not come from Claim", job.ID.String())
	}
	return *job.ClaimedBy, nil
}

// Replay requeues a dead job. See store.ReplayJob for the exact reset and
// ErrNotFound/ErrNotReplayable for why it can fail.
func (q *Queue) Replay(ctx context.Context, id pgtype.UUID) (*store.Job, error) {
	return q.store.ReplayJob(ctx, id)
}
