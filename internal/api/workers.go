package api

import (
	"net/http"
	"time"
)

type workerResponse struct {
	ID              string    `json:"id"`
	Hostname        string    `json:"hostname"`
	JobsInFlight    int32     `json:"jobs_in_flight"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
	CreatedAt       time.Time `json:"created_at"`
}

type listWorkersResponse struct {
	Workers []workerResponse `json:"workers"`
}

// listWorkers serves GET /api/v1/workers. Heartbeat age (how long ago
// last_heartbeat_at was, and whether that counts as "online") is left for
// the client to compute against the returned timestamp - see
// store.ListWorkers.
func (a *API) listWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := a.store.ListWorkers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	resp := listWorkersResponse{Workers: make([]workerResponse, len(workers))}
	for i, wk := range workers {
		resp.Workers[i] = workerResponse{
			ID:              wk.ID,
			Hostname:        wk.Hostname,
			JobsInFlight:    wk.JobsInFlight,
			LastHeartbeatAt: wk.LastHeartbeatAt,
			CreatedAt:       wk.CreatedAt,
		}
	}

	writeJSON(w, http.StatusOK, resp)
}
