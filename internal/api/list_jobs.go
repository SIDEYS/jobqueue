package api

import (
	"net/http"
	"strconv"

	"github.com/SIDEYS/jobqueue/internal/store"
)

const (
	defaultListLimit = 50
	maxListLimit     = 200
)

type listJobsResponse struct {
	Jobs       []jobResponse `json:"jobs"`
	NextCursor *string       `json:"next_cursor,omitempty"`
}

// listJobs serves GET /api/v1/jobs?status=&queue=&job_type=&cursor=&limit=.
// The dead-letter view is just this with status=dead - not a separate
// endpoint, since the filter already covers it.
func (a *API) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	params := store.ListJobsParams{Limit: defaultListLimit}

	if v := q.Get("status"); v != "" {
		s := store.JobStatus(v)
		params.Status = &s
	}
	if v := q.Get("queue"); v != "" {
		params.Queue = &v
	}
	if v := q.Get("job_type"); v != "" {
		params.JobType = &v
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if n > maxListLimit {
			n = maxListLimit
		}
		params.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		cursor, err := store.DecodeListJobsCursor(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		params.Before = &cursor
	}

	jobs, hasMore, err := a.store.ListJobs(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	resp := listJobsResponse{Jobs: make([]jobResponse, len(jobs))}
	for i, j := range jobs {
		resp.Jobs[i] = toJobResponse(j)
	}
	if hasMore && len(jobs) > 0 {
		last := jobs[len(jobs)-1]
		cursor := store.ListJobsCursor{CreatedAt: last.CreatedAt, ID: last.ID}
		token := cursor.Encode()
		resp.NextCursor = &token
	}

	writeJSON(w, http.StatusOK, resp)
}
