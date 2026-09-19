package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type Schedule struct {
	ID          pgtype.UUID
	Queue       string
	JobType     string
	Payload     []byte
	Priority    int32
	MaxAttempts int32
	CronExpr    string
	NextRunAt   time.Time
	LastRunAt   *time.Time
	Enabled     bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

const scheduleColumns = `id, queue, job_type, payload, priority, max_attempts,
	cron_expr, next_run_at, last_run_at, enabled, created_at, updated_at`

func scanSchedule(row pgx.Row) (*Schedule, error) {
	var sc Schedule
	err := row.Scan(
		&sc.ID, &sc.Queue, &sc.JobType, &sc.Payload, &sc.Priority, &sc.MaxAttempts,
		&sc.CronExpr, &sc.NextRunAt, &sc.LastRunAt, &sc.Enabled, &sc.CreatedAt, &sc.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: scan schedule: %w", err)
	}
	return &sc, nil
}

type InsertScheduleParams struct {
	Queue       string
	JobType     string
	Payload     []byte
	Priority    int32
	MaxAttempts int32
	CronExpr    string
	NextRunAt   time.Time
}

func (s *Store) InsertSchedule(ctx context.Context, p InsertScheduleParams) (*Schedule, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO schedules (queue, job_type, payload, priority, max_attempts, cron_expr, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING `+scheduleColumns,
		p.Queue, p.JobType, p.Payload, p.Priority, p.MaxAttempts, p.CronExpr, p.NextRunAt,
	)
	return scanSchedule(row)
}

func (s *Store) GetSchedule(ctx context.Context, id pgtype.UUID) (*Schedule, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+scheduleColumns+` FROM schedules WHERE id = $1`, id)
	return scanSchedule(row)
}

// ListSchedules returns every schedule, newest first. Not paginated -
// unlike jobs, the number of schedules is operator-defined and expected to
// stay small (this is cron entries, not a job history), so keyset
// pagination would be complexity without a real problem behind it.
func (s *Store) ListSchedules(ctx context.Context) ([]*Schedule, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+scheduleColumns+` FROM schedules ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("store: list schedules: %w", err)
	}
	defer rows.Close()

	var schedules []*Schedule
	for rows.Next() {
		sc, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list schedules: %w", err)
	}
	return schedules, nil
}

// UpdateScheduleParams is a partial update: nil/unset fields leave the
// existing column value unchanged (via COALESCE), so a caller only needs
// to supply the fields it actually wants to change. Payload uses nil (not
// an empty-but-non-nil slice) as its "unchanged" sentinel, the same
// convention api.enqueueJob already uses for a job's payload.
type UpdateScheduleParams struct {
	Queue       *string
	JobType     *string
	Payload     []byte
	Priority    *int32
	MaxAttempts *int32
	CronExpr    *string
	NextRunAt   *time.Time
	Enabled     *bool
}

func (s *Store) UpdateSchedule(ctx context.Context, id pgtype.UUID, p UpdateScheduleParams) (*Schedule, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE schedules
		SET queue = COALESCE($2, queue),
			job_type = COALESCE($3, job_type),
			payload = COALESCE($4, payload),
			priority = COALESCE($5, priority),
			max_attempts = COALESCE($6, max_attempts),
			cron_expr = COALESCE($7, cron_expr),
			next_run_at = COALESCE($8, next_run_at),
			enabled = COALESCE($9, enabled),
			updated_at = now()
		WHERE id = $1
		RETURNING `+scheduleColumns,
		id, p.Queue, p.JobType, p.Payload, p.Priority, p.MaxAttempts, p.CronExpr, p.NextRunAt, p.Enabled,
	)
	return scanSchedule(row)
}

func (s *Store) DeleteSchedule(ctx context.Context, id pgtype.UUID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM schedules WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("store: delete schedule: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DueSchedules lists enabled schedules whose next_run_at has arrived. This
// is a plain read with no row locking - it only decides which schedules
// are worth attempting. The actual correctness guarantee against
// double-firing lives entirely in RunSchedule's atomic UPDATE, so an
// unlocked read here can't cause a duplicate enqueue even if two callers
// (e.g. two schedulers that both currently believe they're leader) run
// this at the same moment - see docs/adr/0001.
func (s *Store) DueSchedules(ctx context.Context, now time.Time, limit int) ([]*Schedule, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+scheduleColumns+`
		FROM schedules
		WHERE enabled = true AND next_run_at <= $1
		ORDER BY next_run_at ASC
		LIMIT $2`,
		now, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: due schedules: %w", err)
	}
	defer rows.Close()

	var schedules []*Schedule
	for rows.Next() {
		sc, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: due schedules: %w", err)
	}
	return schedules, nil
}
