package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusSucceeded JobStatus = "succeeded"
	StatusFailed    JobStatus = "failed"
	StatusDead      JobStatus = "dead"
)

type Job struct {
	ID             pgtype.UUID
	Queue          string
	JobType        string
	Payload        []byte
	Status         JobStatus
	Priority       int32
	RunAt          time.Time
	Attempts       int32
	MaxAttempts    int32
	LastError      *string
	IdempotencyKey *string
	ClaimedBy      *string
	ClaimedAt      *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ErrNotFound is returned when a lookup by id matches no row.
var ErrNotFound = errors.New("store: not found")

const jobColumns = `id, queue, job_type, payload, status, priority, run_at,
	attempts, max_attempts, last_error, idempotency_key, claimed_by,
	claimed_at, created_at, updated_at`

func scanJob(row pgx.Row) (*Job, error) {
	var j Job
	err := row.Scan(
		&j.ID, &j.Queue, &j.JobType, &j.Payload, &j.Status, &j.Priority, &j.RunAt,
		&j.Attempts, &j.MaxAttempts, &j.LastError, &j.IdempotencyKey, &j.ClaimedBy,
		&j.ClaimedAt, &j.CreatedAt, &j.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: scan job: %w", err)
	}
	return &j, nil
}

type InsertJobParams struct {
	Queue          string
	JobType        string
	Payload        []byte
	Priority       int32
	RunAt          time.Time
	MaxAttempts    int32
	IdempotencyKey *string
}

func (s *Store) InsertJob(ctx context.Context, p InsertJobParams) (*Job, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (queue, job_type, payload, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING `+jobColumns,
		p.Queue, p.JobType, p.Payload, p.Priority, p.RunAt, p.MaxAttempts, p.IdempotencyKey,
	)
	return scanJob(row)
}

func (s *Store) GetJob(ctx context.Context, id pgtype.UUID) (*Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	return scanJob(row)
}

// ClaimJobs atomically selects and marks up to limit pending, due jobs in
// queue as running, returning the claimed rows.
//
// The SELECT...FOR UPDATE SKIP LOCKED CTE is what makes this safe to call
// concurrently from many worker processes against the same table:
//
//   - FOR UPDATE takes a row lock on every candidate row as it's selected,
//     so no two transactions can select the same row and both believe they
//     own it.
//   - Without SKIP LOCKED, a second transaction's SELECT...FOR UPDATE would
//     *block* on any row the first transaction has already locked, until
//     the first transaction commits - then it would see the row in its
//     now-committed 'running' state and correctly skip it. That's not
//     incorrect, but it serializes every concurrent claim attempt into a
//     queue behind whichever worker got there first, so claim latency
//     grows with the number of concurrent workers instead of staying flat.
//   - SKIP LOCKED tells Postgres to simply exclude rows that are already
//     locked by another in-flight transaction from the result set, instead
//     of waiting for them. Each worker's claim query then only sees jobs
//     nobody else is currently claiming, so N workers can claim N disjoint
//     batches in parallel with no blocking and no double-claims.
//
// The UPDATE...FROM claimed pattern folds the "mark as running" step into
// the same statement as the SELECT, so the whole claim is one round trip
// and one implicit transaction - there is no window between "select" and
// "mark claimed" for another process to see the row as still pending.
func (s *Store) ClaimJobs(ctx context.Context, queue string, limit int, workerID string) ([]*Job, error) {
	rows, err := s.pool.Query(ctx, `
		WITH claimed AS (
			SELECT id FROM jobs
			WHERE status = 'pending' AND queue = $1 AND run_at <= now()
			ORDER BY priority DESC, run_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs
		SET status = 'running',
			claimed_by = $3,
			claimed_at = now(),
			attempts = attempts + 1,
			updated_at = now()
		FROM claimed
		WHERE jobs.id = claimed.id
		RETURNING `+jobColumns,
		queue, limit, workerID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: claim jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: claim jobs: %w", err)
	}
	return jobs, nil
}

func (s *Store) CompleteJob(ctx context.Context, id pgtype.UUID) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'succeeded', updated_at = now() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("store: complete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) FailJob(ctx context.Context, id pgtype.UUID, cause string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'failed', last_error = $2, updated_at = now() WHERE id = $1`,
		id, cause,
	)
	if err != nil {
		return fmt.Errorf("store: fail job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
