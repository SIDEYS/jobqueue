package api

import (
	"context"
	"net/http"
	"time"
)

// healthz reports whether the process itself is alive - no dependency
// checks. An orchestrator (Kubernetes, Fly) uses this to decide whether to
// kill and restart the process: if this doesn't respond, the process is
// stuck or dead, and restarting is the right fix.
func (a *API) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// readyz reports whether the process can currently serve real requests -
// specifically, whether the database is reachable. A load balancer uses
// this to decide whether to route traffic here, not whether to restart the
// process: a Postgres blip is not fixed by killing and restarting the API,
// and doing so on every readiness failure would turn a transient database
// hiccup into an unnecessary restart storm across every replica at once.
// Separating the two checks means a database outage takes this instance
// out of rotation - stops sending it traffic it can't serve - without
// touching the process's liveness at all.
func (a *API) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := a.store.Pool().Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	w.WriteHeader(http.StatusOK)
}
