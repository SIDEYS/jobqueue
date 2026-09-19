# ADR 0001: Advisory-lock leader election for the cron scheduler

## Context

The scheduler runs as multiple replicas for availability (Phase 7 scales it
to 2 on Fly.io, specifically to prove this works). Every replica polls the
`schedules` table on its own tick loop. If every replica also *fired* every
due schedule, each cron entry would enqueue N duplicate jobs per tick
instead of one. Something has to decide which replica actually does the
firing.

## Options considered

**A. Postgres session-scoped advisory lock (`pg_try_advisory_lock`).**
Each replica holds a dedicated connection and tries to acquire a fixed
lock key. Whichever replica holds the lock treats itself as leader; the
others idle and retry. No new infrastructure - it's a feature of the
database already in use.

**B. A lease table with a heartbeat and TTL** (`SELECT ... FOR UPDATE`
against a `leader` row, or a `leader_id` + `expires_at` column updated on
a heartbeat, with `pg_try_advisory_lock` replaced by an explicit
compare-and-swap). More visible in the schema, and the lease's expiry is
explicit and queryable rather than tied to an opaque session.

**C. An external coordinator** (etcd, Consul, ZooKeeper) with its own
lease/election primitive. The standard answer for leader election at
scale, and arguably the "correct" tool for the job.

## Decision

**A: Postgres advisory lock** - but with a framing that matters more than
the choice itself: **the lock is an optimization, not the correctness
mechanism.** The actual guarantee against a schedule firing twice is
`store.RunSchedule`'s atomic transaction - `UPDATE schedules SET
next_run_at = $2 WHERE id = $1 AND next_run_at <= now()`, in the same
transaction as the job insert. That statement is safe no matter how many
processes call it concurrently: only one `UPDATE` can match a still-due
row, and it does so atomically. The advisory lock's only job is to keep
`N-1` idle replicas from bothering to scan the table and call `RunSchedule`
at all each tick, which is pure waste, not a correctness requirement.

This distinction exists because of a fact worth stating plainly: **no
lock held across a network boundary can be trusted as absolute mutual
exclusion.** A session-scoped advisory lock is tied to one Postgres
backend connection. That connection can die - a network blip, a long GC
pause on the client that makes Postgres think the client went away, the
process getting SIGKILLed before it can clean up - and Postgres releases
the lock the moment the session ends. A second replica can then acquire
it immediately. The first replica has no way to find out its lock is gone
except by noticing on its *next* check - there is an unavoidable window
where two replicas both believe they are leader. Treating the lock as "the
thing that guarantees exactly one leader" would be wrong, and an easy
trap: it looks correct in every manual test (nothing kills a connection
mid-tick on a laptop) and fails exactly under the conditions - a flaky
network, a loaded database, an OOM kill - where correctness actually
matters. The fix isn't a stronger lock; a lease can never be perfectly
safe across a network boundary no matter how it's implemented. The fix is
making the write safe regardless of how many holders the lock currently
has, which is what `RunSchedule` already does independent of this
decision.

A lease table (B) has the identical problem in a different shape: a
heartbeat-and-TTL lease is still just a lock with a time bound, subject to
the same "the holder can't be sure it still holds it" issue, and adds a
second write path (the heartbeat) that itself needs to be reasoned about
under the same failure modes. An external coordinator (C) narrows the
window (purpose-built consensus protocols) but doesn't eliminate it -
and it adds a whole second stateful system to run, deploy, and monitor for
a project whose only source of truth is otherwise Postgres. Given the
actual correctness already lives in the SQL, paying that operational cost
for a marginally shorter (not zero) unsafe window isn't worth it here.

## Consequences

**Accepted:**
- No new infrastructure - one more thing Postgres already does for this
  project, consistent with the broader decision to keep Postgres as the
  only stateful system rather than adding Redis, RabbitMQ, or a
  coordination service alongside it.
- Leadership hand-off after a crash is bounded only by how long a dead
  TCP connection takes Postgres to notice (`tcp_keepalives_*`, or the
  next `pg_try_advisory_lock` attempt from a healthy replica succeeding
  immediately once the old session is gone) - typically fast, but not a
  guaranteed bound the way a short-TTL lease would be.
- A connection that still holds the lock must never be returned to
  `pgxpool`'s general pool - `Release()`ing it back would hand that
  session, lock and all, to some unrelated caller's next query, silently
  leaking the lock until that caller happens to release the same physical
  connection and nobody ever calls `pg_advisory_unlock` on it. This is
  why `LeaderElector` always explicitly `pg_advisory_unlock`s before
  `Release()` on a clean handoff, and only skips straight to `Release()`
  when the connection is already broken (in which case pgx discards it
  instead of pooling it, so there's nothing to leak).
- Every replica capable of leadership must therefore hold one dedicated,
  never-pooled connection for its entire candidacy - a small, fixed
  per-replica connection cost.

**Given up:**
- No fine-grained fairness between candidates - whichever replica happens
  to call `pg_try_advisory_lock` first wins, with no preference by load,
  latency, or priority.
- A single lock key means the whole scheduler is either led or idle - no
  partitioning of schedules across multiple simultaneously-active leaders
  for horizontal throughput. That's fine at this project's scale (one
  scheduler's tick loop is not the bottleneck), but it's a ceiling that a
  lease-table or external-coordinator design could remove by sharding
  lock keys per schedule group.
- The "advisory lock is silently lost" scenario has to be actually held
  in mind by anyone touching this code later, because it's not visible in
  the schema the way a lease table's `expires_at` column would be. That
  cost is paid here, in this ADR and in `RunSchedule`'s doc comment,
  instead of in a queryable column.
