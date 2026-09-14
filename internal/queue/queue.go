// Package queue is the domain layer between HTTP/worker code and the
// store: it fills in defaults, validates input, and translates store
// errors into queue-level ones, but every SQL statement still lives in
// internal/store.
package queue

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/SIDEYS/jobqueue/internal/store"
)

var ErrNotFound = store.ErrNotFound

const DefaultMaxAttempts = 5

type Queue struct {
	store *store.Store
}

func New(s *store.Store) *Queue {
	return &Queue{store: s}
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

func (q *Queue) Complete(ctx context.Context, id pgtype.UUID) error {
	return q.store.CompleteJob(ctx, id)
}

func (q *Queue) Fail(ctx context.Context, id pgtype.UUID, cause string) error {
	return q.store.FailJob(ctx, id, cause)
}
