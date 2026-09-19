package scheduler

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LeaderElector uses a Postgres session-scoped advisory lock
// (pg_try_advisory_lock) to decide which of possibly many Scheduler
// instances actually does work each tick.
//
// This is an optimization, not the correctness mechanism - see
// docs/adr/0001 and store.RunSchedule's doc comment. A session-scoped
// advisory lock can be lost silently (the connection drops, Postgres
// releases the lock, another instance acquires it) with no notification
// to the instance that lost it. The only thing that actually prevents a
// double-fire is RunSchedule's atomic UPDATE ... WHERE next_run_at <=
// now(); this type exists purely so that N-1 idle instances aren't
// querying the schedules table on every tick for no reason.
type LeaderElector struct {
	pool    *pgxpool.Pool
	lockKey int64

	mu   sync.Mutex
	conn *pgxpool.Conn // non-nil only while this instance holds the lock
}

func NewLeaderElector(pool *pgxpool.Pool, lockKey int64) *LeaderElector {
	return &LeaderElector{pool: pool, lockKey: lockKey}
}

// EnsureLeadership reports whether this instance currently holds
// leadership, acquiring it if not already held.
//
// If leadership is currently assumed, the held connection is health-
// checked (Ping) first rather than assumed still valid - the advisory
// lock is tied to that specific Postgres session/connection, so if the
// connection is dead, the lock is already gone too, silently, and this
// instance must stop believing it's leader.
func (le *LeaderElector) EnsureLeadership(ctx context.Context) (bool, error) {
	le.mu.Lock()
	defer le.mu.Unlock()

	if le.conn != nil {
		if err := le.conn.Ping(ctx); err == nil {
			return true, nil
		}
		// The connection is already broken, so pgx will discard rather
		// than pool it on Release - safe to call even though we never
		// managed to pg_advisory_unlock on a dead connection. See Close
		// for why a *healthy* connection can't be released this way.
		le.conn.Release()
		le.conn = nil
	}

	conn, err := le.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("scheduler: acquire connection: %w", err)
	}

	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, le.lockKey).Scan(&acquired); err != nil {
		conn.Release()
		return false, fmt.Errorf("scheduler: try advisory lock: %w", err)
	}

	if !acquired {
		conn.Release()
		return false, nil
	}

	le.conn = conn
	return true, nil
}

func (le *LeaderElector) IsLeader() bool {
	le.mu.Lock()
	defer le.mu.Unlock()
	return le.conn != nil
}

// Close releases leadership, if held. Unlike the failure path in
// EnsureLeadership, this connection is (as far as we know) healthy, so it
// must be explicitly pg_advisory_unlock'd before Release: returning a
// connection to the pool while it still holds our session-scoped lock
// would hand that lock to whatever unrelated caller the pool gives this
// connection to next, leaking it - the pool has no idea an advisory lock
// is attached to this session, since that's purely an application-level
// convention, not something pgx tracks.
func (le *LeaderElector) Close(ctx context.Context) {
	le.mu.Lock()
	defer le.mu.Unlock()

	if le.conn == nil {
		return
	}

	if _, err := le.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, le.lockKey); err != nil {
		// The unlock call itself failed, which in practice means the
		// connection just broke (the query has no other realistic failure
		// mode for a lock we know we hold). A broken connection is exactly
		// the case Release() already discards instead of pooling, so this
		// is still safe - there's no path here where a healthy connection
		// that might still hold the lock goes back into the pool.
		le.conn.Release()
		le.conn = nil
		return
	}

	le.conn.Release()
	le.conn = nil
}
