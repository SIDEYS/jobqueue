package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SIDEYS/jobqueue/internal/scheduler"
	"github.com/SIDEYS/jobqueue/internal/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dbURL := getenv("DATABASE_URL", "postgres://jobqueue:jobqueue@localhost:5432/jobqueue?sslmode=disable")
	tickInterval := getenvDuration("SCHEDULER_TICK_INTERVAL", 10*time.Second)

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

	sched := scheduler.New(s, scheduler.DefaultLockKey)

	log.Printf("scheduler: ticking every %s", tickInterval)
	return sched.Run(ctx, tickInterval)
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
		log.Printf("scheduler: invalid duration for %s=%q, using default %s", key, v, fallback)
		return fallback
	}
	return d
}
