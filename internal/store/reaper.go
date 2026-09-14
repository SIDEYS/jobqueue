package store

import (
	"context"
	"fmt"
	"time"
)

// ReclaimStale reclaims up to limit running jobs whose claimed_at is older
// than threshold - the visibility timeout has expired, so the worker that
// claimed them is presumed dead (crashed, killed, or wedged past any
// reasonable per-job timeout; not necessarily gone, see ErrStaleClaim).
//
// Reclaiming counts as an attempt, the same as a normal claim: a job whose
// claiming worker keeps disappearing (e.g. it OOMs on this exact payload
// every time) must still exhaust max_attempts and land in dead, instead of
// retrying forever just because it keeps escaping via the visibility
// timeout rather than reporting a failure. Each row's outcome - back to
// pending, or straight to dead - is decided from its own attempts and
// max_attempts in the same statement, since one reclaim pass covers many
// jobs at different attempt counts at once.
//
// FOR UPDATE SKIP LOCKED here guards a narrower race than in ClaimJobs: a
// genuine completion or failure write from the original claimant landing
// in the same instant the reaper decides that claimant is dead. Skipping a
// locked row just leaves it for the next reclaim pass, once whichever
// write is in flight has committed.
func (s *Store) ReclaimStale(ctx context.Context, threshold time.Time, limit int) ([]*Job, error) {
	rows, err := s.pool.Query(ctx, `
		WITH stale AS (
			SELECT id FROM jobs
			WHERE status = 'running' AND claimed_at < $1
			ORDER BY claimed_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs
		SET
			attempts = jobs.attempts + 1,
			status = CASE WHEN jobs.attempts + 1 >= jobs.max_attempts THEN 'dead'::job_status ELSE 'pending'::job_status END,
			last_error = CASE WHEN jobs.attempts + 1 >= jobs.max_attempts
				THEN 'reaper: exceeded max_attempts after reclaiming an abandoned claim'
				ELSE 'reaper: reclaimed an abandoned claim (visibility timeout exceeded)'
			END,
			run_at = CASE WHEN jobs.attempts + 1 >= jobs.max_attempts THEN jobs.run_at ELSE now() END,
			claimed_by = NULL,
			claimed_at = NULL,
			updated_at = now()
		FROM stale
		WHERE jobs.id = stale.id
		RETURNING `+jobsTableColumns,
		threshold, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: reclaim stale: %w", err)
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
		return nil, fmt.Errorf("store: reclaim stale: %w", err)
	}
	return jobs, nil
}
