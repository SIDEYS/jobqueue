// Command seed populates a fresh database with realistic-looking demo
// history: a spread of succeeded jobs over the last hour, a few
// dead-lettered ones, and a couple of recurring schedules. It exists purely
// so the deployed dashboard is never empty when someone opens it cold -
// none of this data goes through the normal enqueue/claim/complete path,
// it's inserted directly with backdated timestamps to look like an hour of
// real activity already happened.
//
// Idempotent by way of a blunt guard: it does nothing if the jobs table
// already has any rows, so re-running it against a live database (a
// redeploy, a restart) never duplicates or disturbs real data.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/SIDEYS/jobqueue/internal/logging"
	"github.com/SIDEYS/jobqueue/internal/store"
)

// seedQueue matches cmd/worker's default WORKER_QUEUE - seeding other
// queues would just produce jobs and schedules nothing is ever polling in
// the single-worker demo deployment this is meant for.
const seedQueue = "default"

func main() {
	log := logging.New(getenv("LOG_LEVEL", "info"))
	if err := run(context.Background(), log); err != nil {
		log.Error("seed: fatal", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	dbURL := getenv("DATABASE_URL", "postgres://jobqueue:jobqueue@localhost:5432/jobqueue?sslmode=disable")

	if err := store.Migrate(dbURL); err != nil {
		return err
	}

	s, err := store.New(ctx, dbURL)
	if err != nil {
		return err
	}
	defer s.Close()

	var existing int64
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&existing); err != nil {
		return fmt.Errorf("seed: check existing jobs: %w", err)
	}
	if existing > 0 {
		log.Info("seed: jobs table already has data, skipping", "existing_jobs", existing)
		return nil
	}

	if err := seedSucceededJobs(ctx, s, 45); err != nil {
		return err
	}
	if err := seedDeadJobs(ctx, s, 5); err != nil {
		return err
	}
	if err := seedSchedules(ctx, s); err != nil {
		return err
	}

	log.Info("seed: done", "succeeded_jobs", 45, "dead_jobs", 5, "schedules", 2)
	return nil
}

var workers = []string{"seed-worker-1", "seed-worker-2"}

// seedSucceededJobs backdates n jobs to random instants across the last
// hour, each claimed and completed a few seconds after it was created - a
// plausible claim-then-run latency, not simultaneous.
func seedSucceededJobs(ctx context.Context, s *store.Store, n int) error {
	now := time.Now().UTC()
	for i := 0; i < n; i++ {
		createdAt := now.Add(-time.Duration(rand.Intn(60)) * time.Minute).Add(-time.Duration(rand.Intn(60)) * time.Second)
		claimedAt := createdAt.Add(time.Duration(200+rand.Intn(800)) * time.Millisecond)
		completedAt := claimedAt.Add(time.Duration(1+rand.Intn(4)) * time.Second)

		jobType, payload := "sleep", fmt.Sprintf(`{"duration_ms":%d}`, 100+rand.Intn(900))
		if rand.Intn(3) == 0 {
			jobType, payload = "flaky", `{"fail_rate":0.2}`
		}

		_, err := s.Pool().Exec(ctx, `
			INSERT INTO jobs (queue, job_type, payload, status, attempts, max_attempts,
				claimed_by, claimed_at, run_at, created_at, updated_at)
			VALUES ($1, $2, $3, 'succeeded', 1, 5, $4, $5, $6, $6, $7)`,
			seedQueue, jobType, payload, workers[i%len(workers)], claimedAt, createdAt, completedAt)
		if err != nil {
			return fmt.Errorf("seed: insert succeeded job: %w", err)
		}
	}
	return nil
}

// seedDeadJobs backdates n jobs that exhausted every retry - the DLQ view's
// reason to exist. last_error mirrors FlakyHandler's real error format
// (internal/worker/handlers.go) so it reads exactly like a genuine
// dead-letter, not obviously synthetic text.
func seedDeadJobs(ctx context.Context, s *store.Store, n int) error {
	now := time.Now().UTC()
	for i := 0; i < n; i++ {
		createdAt := now.Add(-time.Duration(rand.Intn(60)) * time.Minute)
		lastAttemptAt := createdAt.Add(time.Duration(30+rand.Intn(90)) * time.Second)

		_, err := s.Pool().Exec(ctx, `
			INSERT INTO jobs (queue, job_type, payload, status, attempts, max_attempts,
				last_error, claimed_by, claimed_at, run_at, created_at, updated_at)
			VALUES ($1, 'flaky', '{"fail_rate":0.8}', 'dead', 3, 3,
				'flaky: simulated failure (fail_rate=0.80)', $2, $3, $4, $4, $3)`,
			seedQueue, workers[i%len(workers)], lastAttemptAt, createdAt)
		if err != nil {
			return fmt.Errorf("seed: insert dead job: %w", err)
		}
	}
	return nil
}

// seedSchedules adds a couple of recurring schedules so the scheduler has
// something to fire the moment it starts, instead of the dashboard's
// schedules tab sitting empty until someone creates one by hand.
func seedSchedules(ctx context.Context, s *store.Store) error {
	now := time.Now().UTC()
	schedules := []struct {
		jobType, payload, cronExpr string
	}{
		{"sleep", `{"duration_ms":300}`, "*/2 * * * *"},
		{"flaky", `{"fail_rate":0.3}`, "*/5 * * * *"},
	}

	for _, sch := range schedules {
		_, err := s.Pool().Exec(ctx, `
			INSERT INTO schedules (queue, job_type, payload, cron_expr, enabled, next_run_at, created_at, updated_at)
			VALUES ($1, $2, $3, $4, true, $5, $5, $5)`,
			seedQueue, sch.jobType, sch.payload, sch.cronExpr, now)
		if err != nil {
			return fmt.Errorf("seed: insert schedule: %w", err)
		}
	}
	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
