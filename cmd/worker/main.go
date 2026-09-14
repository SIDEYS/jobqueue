package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SIDEYS/jobqueue/internal/queue"
	"github.com/SIDEYS/jobqueue/internal/store"
	"github.com/SIDEYS/jobqueue/internal/worker"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dbURL := getenv("DATABASE_URL", "postgres://jobqueue:jobqueue@localhost:5432/jobqueue?sslmode=disable")
	queueName := getenv("WORKER_QUEUE", "default")
	visibilityTimeout := getenvDuration("JOB_VISIBILITY_TIMEOUT", time.Minute)
	reapInterval := getenvDuration("REAPER_INTERVAL", 30*time.Second)

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

	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	q := queue.New(s)

	reg := worker.NewRegistry()
	worker.RegisterDemoHandlers(reg)

	w := worker.New(q, reg, queueName, workerID)
	reaper := queue.NewReaper(s, visibilityTimeout)

	// The reaper runs alongside every worker process rather than as a
	// dedicated singleton. That's deliberately redundant when multiple
	// worker replicas are running (Phase 7 scales this to 3): each one
	// runs its own reclaim loop against the same table. That's safe
	// because ReclaimStale uses the same FOR UPDATE SKIP LOCKED pattern as
	// claiming - concurrent reapers split the stale-job set instead of
	// racing over the same rows - and simpler than electing a single
	// reaper leader for a maintenance task that's already safe to run
	// concurrently.
	errCh := make(chan error, 2)
	go func() {
		log.Printf("worker: polling queue %q as %q", queueName, workerID)
		errCh <- w.Run(ctx, 500*time.Millisecond)
	}()
	go func() {
		log.Printf("reaper: reclaiming jobs claimed over %s ago, every %s", visibilityTimeout, reapInterval)
		errCh <- reaper.Run(ctx, reapInterval)
	}()

	if err := <-errCh; err != nil {
		return err
	}
	return <-errCh
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("worker: invalid duration for %s=%q, using default %s", key, v, fallback)
		return fallback
	}
	return d
}
