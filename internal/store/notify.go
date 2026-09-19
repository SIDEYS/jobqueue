package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// JobEventsChannel is the Postgres NOTIFY channel every job state
// transition is published on. internal/events listens on it to drive the
// SSE stream.
const JobEventsChannel = "job_events"

// jobEventPayload is the entire body of every job-state-change
// notification - deliberately just enough to identify what changed, not
// what it changed to in detail. Postgres caps a NOTIFY payload at 8000
// bytes; a job's own payload column has no such bound, so including it
// here could error out a transaction that was otherwise perfectly fine.
// Clients that need more than this fetch the job by id.
type jobEventPayload struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Queue  string `json:"queue"`
}

// notifyJobEvent publishes a job state change on tx. It must be called
// inside the same transaction as the write that caused the change -
// Postgres NOTIFY is itself transactional (queued during the transaction,
// delivered to listeners only at COMMIT, silently discarded on ROLLBACK),
// so doing it in-transaction gets atomicity for free: there is no way for
// a listener to see an event for a write that didn't actually land, and no
// separate step that could fail after the write already committed.
func notifyJobEvent(ctx context.Context, tx pgx.Tx, id, status, queue string) error {
	payload, err := json.Marshal(jobEventPayload{ID: id, Status: status, Queue: queue})
	if err != nil {
		return fmt.Errorf("store: marshal job event payload: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, JobEventsChannel, string(payload)); err != nil {
		return fmt.Errorf("store: notify job event: %w", err)
	}
	return nil
}
