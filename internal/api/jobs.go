package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/SIDEYS/jobqueue/internal/queue"
	"github.com/SIDEYS/jobqueue/internal/store"
)

type enqueueRequest struct {
	Queue          string          `json:"queue"`
	JobType        string          `json:"job_type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int32           `json:"priority"`
	RunAt          *time.Time      `json:"run_at"`
	MaxAttempts    int32           `json:"max_attempts"`
	IdempotencyKey *string         `json:"idempotency_key"`
}

type jobResponse struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	JobType        string          `json:"job_type"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Priority       int32           `json:"priority"`
	RunAt          time.Time       `json:"run_at"`
	Attempts       int32           `json:"attempts"`
	MaxAttempts    int32           `json:"max_attempts"`
	LastError      *string         `json:"last_error,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	ClaimedBy      *string         `json:"claimed_by,omitempty"`
	ClaimedAt      *time.Time      `json:"claimed_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

func toJobResponse(j *store.Job) jobResponse {
	return jobResponse{
		ID:             j.ID.String(),
		Queue:          j.Queue,
		JobType:        j.JobType,
		Payload:        j.Payload,
		Status:         string(j.Status),
		Priority:       j.Priority,
		RunAt:          j.RunAt,
		Attempts:       j.Attempts,
		MaxAttempts:    j.MaxAttempts,
		LastError:      j.LastError,
		IdempotencyKey: j.IdempotencyKey,
		ClaimedBy:      j.ClaimedBy,
		ClaimedAt:      j.ClaimedAt,
		CreatedAt:      j.CreatedAt,
		UpdatedAt:      j.UpdatedAt,
	}
}

func (a *API) enqueueJob(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	payload := req.Payload
	if payload == nil {
		payload = json.RawMessage("{}")
	}

	params := queue.EnqueueParams{
		Queue:          req.Queue,
		JobType:        req.JobType,
		Payload:        payload,
		Priority:       req.Priority,
		MaxAttempts:    req.MaxAttempts,
		IdempotencyKey: req.IdempotencyKey,
	}
	if req.RunAt != nil {
		params.RunAt = *req.RunAt
	}

	job, err := a.queue.Enqueue(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, toJobResponse(job))
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	idParam := chi.URLParam(r, "id")

	var id pgtype.UUID
	if err := id.Scan(idParam); err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := a.queue.Get(r.Context(), id)
	if errors.Is(err, queue.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, toJobResponse(job))
}

func (a *API) replayJob(w http.ResponseWriter, r *http.Request) {
	idParam := chi.URLParam(r, "id")

	var id pgtype.UUID
	if err := id.Scan(idParam); err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := a.queue.Replay(r.Context(), id)
	switch {
	case errors.Is(err, queue.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
		return
	case errors.Is(err, queue.ErrNotReplayable):
		// Replaying anything but a dead job would race a live execution
		// (or a job still legitimately waiting its turn), so this is a
		// conflict with the job's current state, not a bad request.
		writeError(w, http.StatusConflict, err.Error())
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, toJobResponse(job))
}
