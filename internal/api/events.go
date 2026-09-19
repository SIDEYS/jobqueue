package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// keepaliveInterval bounds how long the connection can go without any
// bytes crossing the wire. Some intermediate proxies and load balancers
// time out an idle connection well before either side would otherwise
// notice anything wrong; a periodic SSE comment line keeps traffic
// flowing without being a real event.
const keepaliveInterval = 20 * time.Second

// events streams job state transitions as Server-Sent Events.
//
// This stream is not a source of truth - see docs/ARCHITECTURE.md.
// Delivery is at-most-once: a client that connects late, or that
// reconnects after a drop, has simply missed whatever happened while it
// wasn't listening. Callers must fetch current state (e.g. GET
// /api/v1/jobs) on connect and treat each event as a hint that something
// changed - "go refetch the thing this refers to" - not as a reliable
// log of everything that happened.
func (a *API) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	ch := a.hub.Subscribe()
	defer a.hub.Unsubscribe(ch)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	keepalive := time.NewTicker(keepaliveInterval)
	defer keepalive.Stop()

	for {
		select {
		case evt, ok := <-ch:
			if !ok {
				// The hub dropped us - most likely backpressure (we
				// weren't keeping up). Ending the response makes the
				// client's EventSource reconnect on its own, which lands
				// on a fresh subscription with an empty buffer.
				return
			}
			data, err := json.Marshal(evt)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-keepalive.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
