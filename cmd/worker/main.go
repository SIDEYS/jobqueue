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

	reg := worker.NewRegistry()
	worker.RegisterDemoHandlers(reg)

	w := worker.New(queue.New(s), reg, queueName, workerID)

	log.Printf("worker: polling queue %q as %q", queueName, workerID)
	return w.Run(ctx, 500*time.Millisecond)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
