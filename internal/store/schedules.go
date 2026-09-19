package store

import (
	"context"
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
