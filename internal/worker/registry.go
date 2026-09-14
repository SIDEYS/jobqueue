package worker

import "context"

// Handler executes a single job's payload. Returning an error marks the
// job failed; the retry/backoff decision on top of that is queue-level
// policy, not the handler's concern.
type Handler func(ctx context.Context, payload []byte) error

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(jobType string, h Handler) {
	r.handlers[jobType] = h
}

func (r *Registry) Get(jobType string) (Handler, bool) {
	h, ok := r.handlers[jobType]
	return h, ok
}
