package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// ScheduledJobParams describes the job a schedule fires. Deliberately not
// InsertJobParams: RunSchedule always sets run_at to the moment it fires
// (now(), inside the same statement/transaction), so a RunAt field that
// callers might expect to control would be silently ignored - a separate,
// smaller type avoids that trap.
type ScheduledJobParams struct {
	Queue          string
	JobType        string
	Payload        []byte
	Priority       int32
	MaxAttempts    int32
	IdempotencyKey *string
}

// RunSchedule atomically advances schedule id's next_run_at to nextRunAt
// and enqueues the job it describes, in one transaction gated by
// `WHERE next_run_at <= now()` on the advance step.
//
// This - not leader election - is what actually prevents a schedule from
// firing twice. A session-scoped advisory lock (see the scheduler package
// and docs/adr/0001) can be lost silently: the holder's connection drops,
// Postgres releases the lock, a second instance acquires it, and the
// first instance has no way to know that happened until its next health
// check. There is a real window where two processes both believe they are
// leader. If both attempt this at nearly the same moment, only one's
// UPDATE can see next_run_at still due and lock that row; the loser's
// WHERE clause matches zero rows once the winner commits, so it enqueues
// nothing. Leader election exists to stop N-1 instances from doing this
// query every tick for no reason - it is an optimization, not the
// correctness mechanism.
//
// Returns ran=false (no error) if the schedule was not due when this ran:
// already advanced by a concurrent caller, or disabled in the meantime.
func (s *Store) RunSchedule(ctx context.Context, scheduleID pgtype.UUID, nextRunAt time.Time, jp ScheduledJobParams) (job *Job, ran bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("store: run schedule: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	tag, err := tx.Exec(ctx, `
		UPDATE schedules
		SET last_run_at = now(), next_run_at = $2, updated_at = now()
		WHERE id = $1 AND enabled = true AND next_run_at <= now()`,
		scheduleID, nextRunAt,
	)
	if err != nil {
		return nil, false, fmt.Errorf("store: run schedule: advance: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, false, nil
	}

	row := tx.QueryRow(ctx, `
		INSERT INTO jobs (queue, job_type, payload, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, $4, now(), $5, $6)
		RETURNING `+jobColumns,
		jp.Queue, jp.JobType, jp.Payload, jp.Priority, jp.MaxAttempts, jp.IdempotencyKey,
	)
	j, err := scanJob(row)
	if err != nil {
		return nil, false, fmt.Errorf("store: run schedule: enqueue: %w", err)
	}

	if err := notifyJobEvent(ctx, tx, j.ID.String(), string(j.Status), j.Queue); err != nil {
		return nil, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("store: run schedule: commit: %w", err)
	}
	return j, true, nil
}
