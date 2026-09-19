package api

import (
	"net/http"
	"time"
)

const throughputWindow = time.Hour

type throughputBucket struct {
	Minute    string `json:"minute"`
	Succeeded int64  `json:"succeeded"`
}

type throughputResponse struct {
	Buckets []throughputBucket `json:"buckets"`
}

// throughput serves GET /api/v1/stats/throughput: succeeded-job counts by
// minute for the last hour, for the dashboard's overview chart.
func (a *API) throughput(w http.ResponseWriter, r *http.Request) {
	since := time.Now().Add(-throughputWindow)
	rows, err := a.store.Throughput(r.Context(), since)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	resp := throughputResponse{Buckets: make([]throughputBucket, len(rows))}
	for i, row := range rows {
		resp.Buckets[i] = throughputBucket{
			Minute:    row.Minute.Format(time.RFC3339),
			Succeeded: row.Succeeded,
		}
	}

	writeJSON(w, http.StatusOK, resp)
}
