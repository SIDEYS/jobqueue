package store

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// ListJobsCursor identifies a position in the (created_at, id) DESC
// ordering ListJobs uses - the last row a caller has already seen.
type ListJobsCursor struct {
	CreatedAt time.Time
	ID        pgtype.UUID
}

// Encode renders the cursor as an opaque token safe to hand to a client
// and accept back later. It's deliberately not JSON or anything a client
// might be tempted to parse and depend on - the format is free to change,
// callers must only ever treat it as an opaque string.
func (c ListJobsCursor) Encode() string {
	raw := fmt.Sprintf("%s|%s", c.CreatedAt.UTC().Format(time.RFC3339Nano), c.ID.String())
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

func DecodeListJobsCursor(token string) (ListJobsCursor, error) {
	raw, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return ListJobsCursor{}, fmt.Errorf("store: decode cursor: %w", err)
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return ListJobsCursor{}, fmt.Errorf("store: decode cursor: malformed")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return ListJobsCursor{}, fmt.Errorf("store: decode cursor: bad timestamp: %w", err)
	}
	var id pgtype.UUID
	if err := id.Scan(parts[1]); err != nil {
		return ListJobsCursor{}, fmt.Errorf("store: decode cursor: bad id: %w", err)
	}
	return ListJobsCursor{CreatedAt: createdAt, ID: id}, nil
}

type ListJobsParams struct {
	Status  *JobStatus
	Queue   *string
	JobType *string
	Before  *ListJobsCursor // exclusive: only rows older than this position
	Limit   int
}

// ListJobs returns up to Limit jobs matching the given filters, newest
// first, along with hasMore indicating whether more rows exist past the
// last one returned. Pagination is keyset-based (WHERE (created_at, id) <
// (cursor)), not offset-based: an offset (LIMIT/OFFSET) has to re-scan and
// discard every prior row on each page, getting slower the deeper a caller
// pages, and its notion of "page 5" silently shifts under concurrent
// inserts/deletes. A keyset cursor's cost is independent of how deep the
// caller has paged, and a row inserted or removed elsewhere never causes
// the same row to be skipped or repeated across pages.
func (s *Store) ListJobs(ctx context.Context, p ListJobsParams) (jobs []*Job, hasMore bool, err error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobColumns+`
		FROM jobs
		WHERE ($1::job_status IS NULL OR status = $1)
		  AND ($2::text IS NULL OR queue = $2)
		  AND ($3::text IS NULL OR job_type = $3)
		  AND ($4::timestamptz IS NULL OR (created_at, id) < ($4, $5))
		ORDER BY created_at DESC, id DESC
		LIMIT $6`,
		p.Status, p.Queue, p.JobType,
		beforeCreatedAt(p.Before), beforeID(p.Before),
		p.Limit+1,
	)
	if err != nil {
		return nil, false, fmt.Errorf("store: list jobs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, false, err
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("store: list jobs: %w", err)
	}

	if len(jobs) > p.Limit {
		jobs = jobs[:p.Limit]
		hasMore = true
	}
	return jobs, hasMore, nil
}

func beforeCreatedAt(c *ListJobsCursor) *time.Time {
	if c == nil {
		return nil
	}
	return &c.CreatedAt
}

func beforeID(c *ListJobsCursor) *pgtype.UUID {
	if c == nil {
		return nil
	}
	return &c.ID
}
