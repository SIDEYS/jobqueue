package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/SIDEYS/jobqueue/internal/api"
	"github.com/SIDEYS/jobqueue/internal/events"
	"github.com/SIDEYS/jobqueue/internal/logging"
	"github.com/SIDEYS/jobqueue/internal/metrics"
	"github.com/SIDEYS/jobqueue/internal/queue"
	"github.com/SIDEYS/jobqueue/internal/store"
)

func main() {
	log := logging.New(getenv("LOG_LEVEL", "info"))
	if err := run(log); err != nil {
		log.Error("api: fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	dbURL := getenv("DATABASE_URL", "postgres://jobqueue:jobqueue@localhost:5432/jobqueue?sslmode=disable")
	addr := getenv("API_ADDR", ":8080")

	if err := store.Migrate(dbURL); err != nil {
		return err
	}

	// listenerCtx is independent of the HTTP server's own shutdown timing
	// below - the event listener has nothing to drain, so it's simplest
	// to just cancel it alongside the server rather than sequence them.
	listenerCtx, cancelListener := context.WithCancel(context.Background())
	defer cancelListener()

	s, err := store.New(listenerCtx, dbURL)
	if err != nil {
		return err
	}
	defer s.Close()

	prometheus.MustRegister(metrics.NewDBCollectors(s.Pool()))

	q := queue.New(s)
	hub := events.NewHub()
	defer hub.Close()
	listener := events.NewListener(s.Pool(), hub, log)

	router := api.NewRouter(q, hub, log)

	srv := &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		log.Info("api: listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		errCh <- listener.Run(listenerCtx)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-sigCh:
		log.Info("api: shutting down")
		cancelListener()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		return srv.Shutdown(shutdownCtx)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
