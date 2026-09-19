package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/SIDEYS/jobqueue/internal/scheduler"
	"github.com/SIDEYS/jobqueue/internal/store"
)

type scheduleResponse struct {
	ID          string          `json:"id"`
	Queue       string          `json:"queue"`
	JobType     string          `json:"job_type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int32           `json:"priority"`
	MaxAttempts int32           `json:"max_attempts"`
	CronExpr    string          `json:"cron_expr"`
	NextRunAt   time.Time       `json:"next_run_at"`
	LastRunAt   *time.Time      `json:"last_run_at,omitempty"`
	Enabled     bool            `json:"enabled"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

func toScheduleResponse(sc *store.Schedule) scheduleResponse {
	return scheduleResponse{
		ID:          sc.ID.String(),
		Queue:       sc.Queue,
		JobType:     sc.JobType,
		Payload:     sc.Payload,
		Priority:    sc.Priority,
		MaxAttempts: sc.MaxAttempts,
		CronExpr:    sc.CronExpr,
		NextRunAt:   sc.NextRunAt,
		LastRunAt:   sc.LastRunAt,
		Enabled:     sc.Enabled,
		CreatedAt:   sc.CreatedAt,
		UpdatedAt:   sc.UpdatedAt,
	}
}

func parseScheduleID(r *http.Request) (pgtype.UUID, error) {
	var id pgtype.UUID
	err := id.Scan(chi.URLParam(r, "id"))
	return id, err
}

type createScheduleRequest struct {
	Queue       string          `json:"queue"`
	JobType     string          `json:"job_type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int32           `json:"priority"`
	MaxAttempts int32           `json:"max_attempts"`
	CronExpr    string          `json:"cron_expr"`
}

func (a *API) createSchedule(w http.ResponseWriter, r *http.Request) {
	var req createScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Queue == "" {
		writeError(w, http.StatusBadRequest, "queue is required")
		return
	}
	if req.JobType == "" {
		writeError(w, http.StatusBadRequest, "job_type is required")
		return
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = 5
	}

	payload := req.Payload
	if payload == nil {
		payload = json.RawMessage("{}")
	}

	next, err := scheduler.NextRun(req.CronExpr, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid cron_expr: "+err.Error())
		return
	}

	sc, err := a.store.InsertSchedule(r.Context(), store.InsertScheduleParams{
		Queue:       req.Queue,
		JobType:     req.JobType,
		Payload:     payload,
		Priority:    req.Priority,
		MaxAttempts: req.MaxAttempts,
		CronExpr:    req.CronExpr,
		NextRunAt:   next,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusCreated, toScheduleResponse(sc))
}

func (a *API) listSchedules(w http.ResponseWriter, r *http.Request) {
	schedules, err := a.store.ListSchedules(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	resp := make([]scheduleResponse, len(schedules))
	for i, sc := range schedules {
		resp[i] = toScheduleResponse(sc)
	}
	writeJSON(w, http.StatusOK, struct {
		Schedules []scheduleResponse `json:"schedules"`
	}{Schedules: resp})
}

func (a *API) getSchedule(w http.ResponseWriter, r *http.Request) {
	id, err := parseScheduleID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid schedule id")
		return
	}

	sc, err := a.store.GetSchedule(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, toScheduleResponse(sc))
}

type updateScheduleRequest struct {
	Queue       *string          `json:"queue"`
	JobType     *string          `json:"job_type"`
	Payload     *json.RawMessage `json:"payload"`
	Priority    *int32           `json:"priority"`
	MaxAttempts *int32           `json:"max_attempts"`
	CronExpr    *string          `json:"cron_expr"`
	Enabled     *bool            `json:"enabled"`
}

// updateSchedule serves PATCH /api/v1/schedules/:id. Every field is
// optional; only what's supplied changes (see store.UpdateScheduleParams).
// Changing cron_expr recomputes next_run_at from now - the old schedule's
// next_run_at was computed against the old expression and has no
// necessary relationship to the new one.
func (a *API) updateSchedule(w http.ResponseWriter, r *http.Request) {
	id, err := parseScheduleID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid schedule id")
		return
	}

	var req updateScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	params := store.UpdateScheduleParams{
		Queue:       req.Queue,
		JobType:     req.JobType,
		Priority:    req.Priority,
		MaxAttempts: req.MaxAttempts,
		CronExpr:    req.CronExpr,
		Enabled:     req.Enabled,
	}
	if req.Payload != nil {
		params.Payload = []byte(*req.Payload)
	}
	if req.CronExpr != nil {
		next, err := scheduler.NextRun(*req.CronExpr, time.Now().UTC())
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid cron_expr: "+err.Error())
			return
		}
		params.NextRunAt = &next
	}

	sc, err := a.store.UpdateSchedule(r.Context(), id, params)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, toScheduleResponse(sc))
}

func (a *API) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	id, err := parseScheduleID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid schedule id")
		return
	}

	err = a.store.DeleteSchedule(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
