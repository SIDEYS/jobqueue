package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
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
	concurrency := getenvInt("WORKER_CONCURRENCY", 4)
	batchSize := getenvInt("WORKER_CLAIM_BATCH_SIZE", 4)
	pollBase := getenvDuration("WORKER_POLL_INTERVAL", 500*time.Millisecond)
	pollMax := getenvDuration("WORKER_MAX_POLL_INTERVAL", 2*time.Second)
	jobTimeout := getenvDuration("WORKER_JOB_TIMEOUT", 30*time.Second)
	drainTimeout := getenvDuration("WORKER_DRAIN_TIMEOUT", 25*time.Second)
	heartbeatInterval := getenvDuration("WORKER_HEARTBEAT_INTERVAL", 10*time.Second)
	heartbeatTTL := getenvDuration("WORKER_HEARTBEAT_TTL", 60*time.Second)

	if err := store.Migrate(dbURL); err != nil {
		return err
	}

	// runCtx is long-lived, not tied to the OS signal below: Shutdown
	// alone is what stops the pool (see the signal case), so it behaves
	// identically whether triggered by a real SIGTERM here or called
	// directly in a test. Heartbeat and the reaper do watch runCtx, so
	// they keep running through the drain (a worker that's draining should
	// still show up as alive, not vanish from the dashboard mid-shutdown)
	// and stop only once Shutdown has finished.
	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()

	s, err := store.New(runCtx, dbURL)
	if err != nil {
		return err
	}
	defer s.Close()

	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	q := queue.New(s)

	reg := worker.NewRegistry()
	worker.RegisterDemoHandlers(reg)

	pool := worker.NewPool(q, reg, worker.Config{
		QueueName:   queueName,
		WorkerID:    workerID,
		Concurrency: concurrency,
		BatchSize:   batchSize,
		PollBase:    pollBase,
		PollMax:     pollMax,
		JobTimeout:  jobTimeout,
	})
	heartbeat := worker.NewHeartbeater(s, workerID, hostname, heartbeatInterval, heartbeatTTL, pool.InFlightCount)
	reaper := queue.NewReaper(s, visibilityTimeout)

	// See the reaper, run alongside every worker replica rather than as a
	// dedicated singleton - concurrent reapers split the stale-job set via
	// the same SKIP LOCKED pattern as claiming, instead of needing leader
	// election for a task that's already safe to run concurrently.
	errCh := make(chan error, 3)
	go func() {
		log.Printf("worker: polling queue %q as %q (concurrency=%d, batch=%d)", queueName, workerID, concurrency, batchSize)
		errCh <- pool.Run(runCtx)
	}()
	go func() {
		errCh <- heartbeat.Run(runCtx)
	}()
	go func() {
		log.Printf("reaper: reclaiming jobs claimed over %s ago, every %s", visibilityTimeout, reapInterval)
		errCh <- reaper.Run(runCtx, reapInterval)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-sigCh:
		return shutdown(pool, drainTimeout, stopRun)
	}
}

// shutdown is deliberately thin: all the actual drain-then-release logic
// lives in Pool.Shutdown, so it's exercised the same way whether it's
// triggered by this signal handler or called directly by a test.
//
// WORKER_DRAIN_TIMEOUT must be set shorter than your deployment platform's
// grace period between SIGTERM and SIGKILL (Fly's kill_timeout, ECS's
// stopTimeout), with margin for the release write itself to reach the
// database - see README. If it isn't, the platform kills the process
// before Shutdown gets to release in-flight claims, and those jobs sit
// stuck until the reaper's visibility timeout catches them instead of
// being immediately reclaimable.
func shutdown(pool *worker.Pool, drainTimeout time.Duration, stopRun context.CancelFunc) error {
	log.Print("worker: shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()

	err := pool.Shutdown(ctx)
	stopRun()
	return err
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

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("worker: invalid integer for %s=%q, using default %d", key, v, fallback)
		return fallback
	}
	return n
}
