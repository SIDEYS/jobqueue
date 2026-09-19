package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/SIDEYS/jobqueue/internal/logging"
	"github.com/SIDEYS/jobqueue/internal/scheduler"
	"github.com/SIDEYS/jobqueue/internal/store"
)

func main() {
	log := logging.New(getenv("LOG_LEVEL", "info"))
	if err := run(log); err != nil {
		log.Error("scheduler: fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	dbURL := getenv("DATABASE_URL", "postgres://jobqueue:jobqueue@localhost:5432/jobqueue?sslmode=disable")
	tickInterval := getenvDuration("SCHEDULER_TICK_INTERVAL", 10*time.Second, log)
	metricsAddr := getenv("SCHEDULER_METRICS_ADDR", ":9092")

	if err := store.Migrate(dbURL); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s, err := store.New(ctx, dbURL)
	if err != nil {
		return err
	}
	defer s.Close()

	sched := scheduler.New(s, scheduler.DefaultLockKey, log)

	// Its own process, its own in-memory Prometheus registry - see the
	// identical comment in cmd/worker/main.go for why this can't just
	// piggyback on the API's /metrics.
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: promhttp.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("scheduler: metrics listening", "addr", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("scheduler: metrics server", "error", err)
		}
	}()
	defer metricsSrv.Close()

	log.Info("scheduler: ticking", "interval", tickInterval)
	return sched.Run(ctx, tickInterval)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration, log *slog.Logger) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Warn("scheduler: invalid duration, using default", "key", key, "value", v, "default", fallback)
		return fallback
	}
	return d
}
