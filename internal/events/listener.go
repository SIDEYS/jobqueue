package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SIDEYS/jobqueue/internal/store"
)

// reconnectBackoff is how long Listener waits between reconnect attempts
// after losing its LISTEN connection, so a genuinely down database doesn't
// turn into a hot retry loop.
const reconnectBackoff = 1 * time.Second

// healthCheckInterval bounds how long Listener will wait for a
// notification before proactively verifying its connection is still
// alive. A hard TCP failure surfaces immediately as an error from
// WaitForNotification; this catches the quieter case - a half-open
// connection that hasn't errored yet because nothing has tried to use it
// - the same discipline the scheduler's LeaderElector applies to its own
// dedicated connection.
const healthCheckInterval = 30 * time.Second

// Listener holds one dedicated LISTEN connection to Postgres and forwards
// every job_events notification to hub. If the connection drops - a
// network blip, the database restarting - it reconnects with a short
// backoff rather than leaving the stream silently dead: without this, the
// API would keep serving every other endpoint just fine while the
// dashboard quietly stopped updating, which is a much easier failure to
// miss than an outright error.
type Listener struct {
	pool *pgxpool.Pool
	hub  *Hub
	log  *slog.Logger
}

func NewListener(pool *pgxpool.Pool, hub *Hub, log *slog.Logger) *Listener {
	if log == nil {
		log = slog.Default()
	}
	return &Listener{pool: pool, hub: hub, log: log}
}

// Run listens until ctx is cancelled, reconnecting on any error.
func (l *Listener) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		if err := l.listenOnce(ctx); err != nil && ctx.Err() == nil {
			l.log.Error("events: listen connection lost, reconnecting", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(reconnectBackoff):
			}
		}
	}
}

func (l *Listener) listenOnce(ctx context.Context) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("events: acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+store.JobEventsChannel); err != nil {
		return fmt.Errorf("events: listen: %w", err)
	}

	for {
		waitCtx, cancel := context.WithTimeout(ctx, healthCheckInterval)
		notification, err := conn.Conn().WaitForNotification(waitCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				// Graceful shutdown, not a failure: this connection is
				// still healthy, so explicitly stop listening before
				// Release hands it back to the pool - same tidiness
				// LeaderElector.Close applies to its dedicated connection.
				// Unlike an unreleased advisory lock, a connection that
				// still has LISTEN registered isn't unsafe for whoever
				// reuses it next, just untidy - so this is best-effort.
				if _, unlistenErr := conn.Exec(context.Background(), "UNLISTEN "+store.JobEventsChannel); unlistenErr != nil {
					l.log.Warn("events: unlisten on shutdown", "error", unlistenErr)
				}
				return nil
			}
			if errors.Is(err, context.DeadlineExceeded) {
				// No notification in healthCheckInterval - not
				// necessarily a problem, but confirm the connection is
				// still actually alive rather than assuming a quiet
				// channel means a healthy one.
				if pingErr := conn.Ping(ctx); pingErr != nil {
					return fmt.Errorf("events: connection unhealthy: %w", pingErr)
				}
				continue
			}
			return fmt.Errorf("events: wait for notification: %w", err)
		}

		var evt Event
		if err := json.Unmarshal([]byte(notification.Payload), &evt); err != nil {
			l.log.Error("events: discarding malformed notification payload", "error", err)
			continue
		}
		l.hub.Broadcast(evt)
	}
}
