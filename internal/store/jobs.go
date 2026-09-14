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

// jobsTableColumns is jobColumns qualified with the jobs. table prefix, for
// statements that join jobs against another relation in scope (the claimed
// CTE below) where an unqualified column list would be ambiguous.
const jobsTableColumns = `jobs.id, jobs.queue, jobs.job_type, jobs.payload, jobs.status, jobs.priority, jobs.run_at,
	jobs.attempts, jobs.max_attempts, jobs.last_error, jobs.idempotency_key, jobs.claimed_by,
	jobs.claimed_at, jobs.created_at, jobs.updated_at`

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

// InsertJob enqueues a job. If IdempotencyKey is set and a job with the
// same (queue, idempotency_key) already exists, the existing row is
// returned instead of creating a duplicate.
//
// This is one statement rather than an insert-then-fetch-on-conflict,
// because a two-step version is only correct under READ COMMITTED: the
// fetch would rely on seeing a row committed by a concurrent transaction
// after this transaction's own insert returned nothing, which a
// REPEATABLE READ (or stricter) transaction's snapshot would not see,
// silently returning "no job" instead of the real one. ON CONFLICT ...
// DO UPDATE ... RETURNING has no such window: Postgres resolves the
// conflict and returns the winning row within the same statement,
// regardless of isolation level. The DO UPDATE itself is a genuine no-op
// (updated_at is set to its own current value) - it exists only because
// RETURNING requires a matching DO UPDATE; ON CONFLICT DO NOTHING returns
// no row at all on a conflict.
func (s *Store) InsertJob(ctx context.Context, p InsertJobParams) (*Job, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (queue, job_type, payload, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO UPDATE SET updated_at = jobs.updated_at
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
		RETURNING `+jobsTableColumns,
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

// ErrStaleClaim is returned when a terminal write (complete, mark dead, or
// mark failed-for-retry) targets a job that is no longer held by the given
// claimant. That happens when the reaper has already reclaimed the job out
// from under a worker that is still alive but stuck (a GC pause, a slow
// network call - not necessarily a crash) and a different claimant now
// owns it, or has already moved it to a different terminal state.
//
// Every terminal write is fenced with `WHERE claimed_by = $claimant`
// precisely so this can happen safely: the original worker's write affects
// zero rows instead of overwriting whatever the new claimant did. Callers
// that get this error must not retry the write - the row already reflects
// a different attempt's outcome.
var ErrStaleClaim = errors.New("store: claim is no longer held")

// CompleteJob marks a running job succeeded, but only if claimedBy still
// matches the row's claimed_by. See ErrStaleClaim.
func (s *Store) CompleteJob(ctx context.Context, id pgtype.UUID, claimedBy string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'succeeded', updated_at = now()
		WHERE id = $1 AND claimed_by = $2 AND status = 'running'`,
		id, claimedBy,
	)
	if err != nil {
		return fmt.Errorf("store: complete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleClaim
	}
	return nil
}

// MarkDead moves a running job straight to the dead-letter state, but only
// if claimedBy still matches the row's claimed_by. See ErrStaleClaim.
func (s *Store) MarkDead(ctx context.Context, id pgtype.UUID, claimedBy, cause string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'dead', last_error = $3, claimed_by = NULL, claimed_at = NULL, updated_at = now()
		WHERE id = $1 AND claimed_by = $2 AND status = 'running'`,
		id, claimedBy, cause,
	)
	if err != nil {
		return fmt.Errorf("store: mark dead: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleClaim
	}
	return nil
}

// ReleaseJob returns a running job to pending immediately (run_at = now()),
// but only if claimedBy still matches the row's claimed_by. See
// ErrStaleClaim.
//
// Unlike MarkFailedForRetry, attempts is left untouched: this isn't a
// reported failure or a suspected-dead reclaim, it's an orderly worker
// shutdown voluntarily giving back work it simply didn't get to. The job
// shouldn't be charged an attempt, and it shouldn't wait out a backoff
// window either - some other worker is very likely still healthy and
// idle, so making the job immediately claimable again is strictly better
// than making it wait.
func (s *Store) ReleaseJob(ctx context.Context, id pgtype.UUID, claimedBy string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'pending', run_at = now(),
			claimed_by = NULL, claimed_at = NULL, updated_at = now()
		WHERE id = $1 AND claimed_by = $2 AND status = 'running'`,
		id, claimedBy,
	)
	if err != nil {
		return fmt.Errorf("store: release job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleClaim
	}
	return nil
}

// MarkFailedForRetry returns a running job to pending with a future run_at,
// but only if claimedBy still matches the row's claimed_by. See
// ErrStaleClaim. The retry delay (runAt) is computed by the caller -
// queue.Backoff - since it needs the job's already-known attempts count,
// which the claim step already returned.
func (s *Store) MarkFailedForRetry(ctx context.Context, id pgtype.UUID, claimedBy, cause string, runAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'pending', last_error = $3, run_at = $4,
			claimed_by = NULL, claimed_at = NULL, updated_at = now()
		WHERE id = $1 AND claimed_by = $2 AND status = 'running'`,
		id, claimedBy, cause, runAt,
	)
	if err != nil {
		return fmt.Errorf("store: mark failed for retry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleClaim
	}
	return nil
}
