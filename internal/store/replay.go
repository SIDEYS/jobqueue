package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

// ErrNotReplayable is returned when replay targets a job that exists but
// is not in the dead status. Replaying anything else - a running job in
// particular - would race a live execution and could duplicate in-flight
// work, so it's rejected rather than silently reset.
var ErrNotReplayable = errors.New("store: job is not dead")

// ReplayJob resets a dead job back to pending: attempts to 0, last_error
// and claimed_by/claimed_at cleared, run_at set to now. It only applies to
// jobs currently in the dead status; ErrNotFound or ErrNotReplayable is
// returned otherwise.
func (s *Store) ReplayJob(ctx context.Context, id pgtype.UUID) (*Job, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'pending', attempts = 0, last_error = NULL,
			claimed_by = NULL, claimed_at = NULL, run_at = now(), updated_at = now()
		WHERE id = $1 AND status = 'dead'
		RETURNING `+jobColumns,
		id,
	)
	job, err := scanJob(row)
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	// The UPDATE matched no row: either id doesn't exist at all, or it
	// exists but isn't dead. GetJob distinguishes the two so the API layer
	// can return 404 vs 409.
	existing, getErr := s.GetJob(ctx, id)
	if getErr != nil {
		return nil, getErr
	}
	return nil, fmt.Errorf("%w: job status is %q", ErrNotReplayable, existing.Status)
}
