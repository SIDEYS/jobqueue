// Package api holds the HTTP surface: routing, request/response DTOs, and
// translation between JSON and the queue domain layer. No SQL lives here.
package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/SIDEYS/jobqueue/internal/events"
	"github.com/SIDEYS/jobqueue/internal/queue"
)

type API struct {
	queue *queue.Queue
	hub   *events.Hub
}

func NewRouter(q *queue.Queue, hub *events.Hub) http.Handler {
	a := &API{queue: q, hub: hub}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/jobs", a.enqueueJob)
		r.Get("/jobs/{id}", a.getJob)
		r.Post("/jobs/{id}/replay", a.replayJob)
		r.Get("/events", a.events)
	})

	return r
}
