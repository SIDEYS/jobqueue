// Package logging provides the one slog.Logger construction used by every
// binary (cmd/api, cmd/worker, cmd/scheduler) and the HTTP middleware that
// carries a request-scoped logger - one that already has the request ID
// attached - through context.Context for the API to use.
package logging

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

// New builds the JSON logger every binary uses. level is LOG_LEVEL's raw
// value; anything unrecognized falls back to info rather than erroring -
// a typo'd log level shouldn't stop the process from starting.
func New(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

type ctxKey struct{}

func WithLogger(ctx context.Context, log *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, log)
}

// FromContext returns the request-scoped logger attached by Middleware, or
// the default logger if called outside a request (e.g. from a background
// loop that doesn't go through the HTTP layer at all).
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

// Middleware attaches a per-request logger carrying the chi request ID to
// the request's context (retrieve it with FromContext), and logs each
// request's outcome once it completes. Must run after chi's
// middleware.RequestID, which is what actually generates the id this
// reads via middleware.GetReqID.
func Middleware(base *slog.Logger) func(http.Handler) http.Handler {
	if base == nil {
		base = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			log := base
			if reqID := middleware.GetReqID(r.Context()); reqID != "" {
				log = base.With("request_id", reqID)
			}

			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r.WithContext(WithLogger(r.Context(), log)))

			log.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}
