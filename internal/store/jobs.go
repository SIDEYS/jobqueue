package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
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

// scanJobWithInserted scans a row whose first column is the
// `(xmax = 0) AS inserted` marker described in InsertJob, followed by the
// usual jobColumns.
func scanJobWithInserted(row pgx.Row) (*Job, bool, error) {
	var (
		j        Job
		inserted bool
	)
	err := row.Scan(
		&inserted,
		&j.ID, &j.Queue, &j.JobType, &j.Payload, &j.Status, &j.Priority, &j.RunAt,
		&j.Attempts, &j.MaxAttempts, &j.LastError, &j.IdempotencyKey, &j.ClaimedBy,
		&j.ClaimedAt, &j.CreatedAt, &j.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, ErrNotFound
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: scan job: %w", err)
	}
	return &j, inserted, nil
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
// returned instead of creating a duplicate - inserted reports which of
// the two happened, so callers can decide whether this was a real new
// job (e.g. for metrics and the job_events notification) or just an
// idempotent replay of a request they'd already seen.
//
// The insert statement itself is one round trip rather than an
// insert-then-fetch-on-conflict, because a two-step version is only
// correct under READ COMMITTED: the fetch would rely on seeing a row
// committed by a concurrent transaction after this transaction's own
// insert returned nothing, which a REPEATABLE READ (or stricter)
// transaction's snapshot would not see, silently returning "no job"
// instead of the real one. ON CONFLICT ... DO UPDATE ... RETURNING has no
// such window: Postgres resolves the conflict and returns the winning row
// within the same statement, regardless of isolation level. The DO UPDATE
// itself is a genuine no-op (updated_at is set to its own current value)
// - it exists only because RETURNING requires a matching DO UPDATE; ON
// CONFLICT DO NOTHING returns no row at all on a conflict.
//
// `(xmax = 0) AS inserted` is how that distinction is read back: xmax is
// Postgres's internal "which transaction deleted/updated this row" system
// column, left at its zero default on a row's original INSERT and set to
// the current transaction's id the moment something updates it - so a row
// this same command just freshly inserted always has xmax = 0, while one
// it instead hit via the ON CONFLICT DO UPDATE path does not.
//
// Enqueueing and notifying happen in the same transaction: NOTIFY is
// itself transactional (delivered at COMMIT, discarded on ROLLBACK), so
// this can't publish an event for a job that didn't actually get created.
func (s *Store) InsertJob(ctx context.Context, p InsertJobParams) (job *Job, inserted bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("store: insert job: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	row := tx.QueryRow(ctx, `
		INSERT INTO jobs (queue, job_type, payload, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO UPDATE SET updated_at = jobs.updated_at
		RETURNING (xmax = 0) AS inserted, `+jobColumns,
		p.Queue, p.JobType, p.Payload, p.Priority, p.RunAt, p.MaxAttempts, p.IdempotencyKey,
	)

	j, wasInserted, err := scanJobWithInserted(row)
	if err != nil {
		return nil, false, err
	}

	if wasInserted {
		if err := notifyJobEvent(ctx, tx, j.ID.String(), string(j.Status), j.Queue); err != nil {
			return nil, false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("store: insert job: commit: %w", err)
	}
	return j, wasInserted, nil
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
// "mark claimed" for another process to see the row as still pending. It's
// wrapped in an explicit transaction here (rather than left as its own
// single implicit one) only so a job_events notification can be published
// for each claimed row before commit - see notifyJobEvent.
func (s *Store) ClaimJobs(ctx context.Context, queue string, limit int, workerID string) ([]*Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: claim jobs: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	rows, err := tx.Query(ctx, `
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

	var jobs []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		jobs = append(jobs, j)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return nil, fmt.Errorf("store: claim jobs: %w", rowsErr)
	}

	for _, j := range jobs {
		if err := notifyJobEvent(ctx, tx, j.ID.String(), string(j.Status), j.Queue); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: claim jobs: commit: %w", err)
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
	return mutateAndNotify(ctx, s.pool, id, string(StatusSucceeded), `
		UPDATE jobs
		SET status = 'succeeded', updated_at = now()
		WHERE id = $1 AND claimed_by = $2 AND status = 'running'
		RETURNING queue`,
		id, claimedBy,
	)
}

// MarkDead moves a running job straight to the dead-letter state, but only
// if claimedBy still matches the row's claimed_by. See ErrStaleClaim.
func (s *Store) MarkDead(ctx context.Context, id pgtype.UUID, claimedBy, cause string) error {
	return mutateAndNotify(ctx, s.pool, id, string(StatusDead), `
		UPDATE jobs
		SET status = 'dead', last_error = $3, claimed_by = NULL, claimed_at = NULL, updated_at = now()
		WHERE id = $1 AND claimed_by = $2 AND status = 'running'
		RETURNING queue`,
		id, claimedBy, cause,
	)
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
	return mutateAndNotify(ctx, s.pool, id, string(StatusPending), `
		UPDATE jobs
		SET status = 'pending', run_at = now(),
			claimed_by = NULL, claimed_at = NULL, updated_at = now()
		WHERE id = $1 AND claimed_by = $2 AND status = 'running'
		RETURNING queue`,
		id, claimedBy,
	)
}

// MarkFailedForRetry returns a running job to pending with a future run_at,
// but only if claimedBy still matches the row's claimed_by. See
// ErrStaleClaim. The retry delay (runAt) is computed by the caller -
// queue.Backoff - since it needs the job's already-known attempts count,
// which the claim step already returned.
func (s *Store) MarkFailedForRetry(ctx context.Context, id pgtype.UUID, claimedBy, cause string, runAt time.Time) error {
	return mutateAndNotify(ctx, s.pool, id, string(StatusPending), `
		UPDATE jobs
		SET status = 'pending', last_error = $3, run_at = $4,
			claimed_by = NULL, claimed_at = NULL, updated_at = now()
		WHERE id = $1 AND claimed_by = $2 AND status = 'running'
		RETURNING queue`,
		id, claimedBy, cause, runAt,
	)
}

// mutateAndNotify runs a single-row UPDATE...RETURNING queue statement
// (sql, args) and, if it matched a row, publishes a job_events
// notification for it in the same transaction before committing. Every
// caller here targets exactly one job by id and is fenced by claimed_by in
// its own WHERE clause, so "matched no row" always means ErrStaleClaim,
// never an id that doesn't exist at all (GetJob is what surfaces that
// distinction; these are all writes against a job a caller just claimed).
func mutateAndNotify(ctx context.Context, pool *pgxpool.Pool, id pgtype.UUID, newStatus, sql string, args ...any) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	var queue string
	err = tx.QueryRow(ctx, sql, args...).Scan(&queue)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrStaleClaim
	}
	if err != nil {
		return fmt.Errorf("store: update: %w", err)
	}

	if err := notifyJobEvent(ctx, tx, id.String(), newStatus, queue); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}
