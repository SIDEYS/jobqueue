package api

import "net/http"

type queueStatsResponse struct {
	Queues []queueStat `json:"queues"`
}

type queueStat struct {
	Queue  string           `json:"queue"`
	Counts map[string]int64 `json:"counts"`
}

// queueStats serves GET /api/v1/queues: job counts by queue and status,
// grouped for direct use by a dashboard overview.
func (a *API) queueStats(w http.ResponseWriter, r *http.Request) {
	rows, err := a.store.QueueStats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	order := make([]string, 0)
	byQueue := make(map[string]map[string]int64)
	for _, row := range rows {
		counts, ok := byQueue[row.Queue]
		if !ok {
			counts = make(map[string]int64)
			byQueue[row.Queue] = counts
			order = append(order, row.Queue)
		}
		counts[string(row.Status)] = row.Count
	}

	resp := queueStatsResponse{Queues: make([]queueStat, len(order))}
	for i, q := range order {
		resp.Queues[i] = queueStat{Queue: q, Counts: byQueue[q]}
	}

	writeJSON(w, http.StatusOK, resp)
}
