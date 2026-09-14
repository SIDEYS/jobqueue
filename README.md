# jobqueue

A distributed job queue and cron scheduler built on PostgreSQL.

> This README is being filled in phase by phase alongside the build. The
> full picture - quickstart, architecture diagram, benchmarks, API
> reference - lands in the documentation phase; what's below covers what
> exists so far.

## Reliability

### Retries: exponential backoff with full jitter

When a job's handler returns an error and it still has attempts left, it's
returned to `pending` with `run_at` pushed forward by `queue.Backoff`
(`internal/queue/backoff.go`):

```
sleep = random_between(0, min(backoffCap, backoffBase * 2^(attempt-1)))
```

This is **full jitter**, not plain exponential backoff: the entire sleep is
randomized, not just a fixed delay with a little noise added on top. That
distinction matters because of what typically causes failures to cluster
in the first place - a downstream dependency having a bad moment. Every
job that failed for the same reason at the same attempt count shares a
`run_at` clock; if the delay itself weren't randomized, they'd all wake up
and retry at exactly the same instant. Against a dependency that's already
struggling, a synchronized wave of retries (a "thundering herd") is often
worse than the outage that caused it. Full jitter spreads retries across
the whole window instead of re-synchronizing them a moment later.

### At-least-once delivery

Handlers must be idempotent - the same job can, under specific timing
conditions, execute more than once. See
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for why exactly-once isn't
achievable here and a worked example of exactly how a job can end up
running twice.

### Graceful shutdown

On SIGTERM, `internal/worker.Pool.Shutdown` stops claiming new work
immediately, then waits for in-flight jobs to finish - but only up to
`WORKER_DRAIN_TIMEOUT`. If that deadline is hit while jobs are still
running, their claims are released explicitly (back to `pending`,
`claimed_by` cleared, `attempts` left untouched) so they're immediately
claimable by another worker rather than waiting out the full
`JOB_VISIBILITY_TIMEOUT` for the reaper to notice.

**`WORKER_DRAIN_TIMEOUT` must be set shorter than your deployment
platform's SIGTERM-to-SIGKILL grace period** (Fly's `kill_timeout`, ECS's
`stopTimeout`), with margin left over for the release write itself to
reach the database. If it isn't, the platform kills the process before
`Shutdown` gets a chance to release anything, and those jobs sit stuck in
`running` until the reaper's visibility timeout eventually catches them -
correct, but far slower than it needs to be. The exact values get set
together in `fly.toml` in the deploy phase, once both are pinned down for
real.

The abandoned goroutine behind a released job keeps running in the
background after `Shutdown` returns (Go can't force-kill a goroutine) -
that's fine, since the process is about to be killed by the platform
anyway, and if that goroutine does eventually report an outcome, the same
`claimed_by` fencing check the reaper relies on makes it a safe no-op once
someone else has reclaimed the job.

## Limitations and tradeoffs so far

- **Polling, not push.** Workers poll for work at `WORKER_POLL_INTERVAL`,
  backing off (capped at `WORKER_MAX_POLL_INTERVAL`, kept small - 2s by
  default) when the queue is empty. That cap is a direct latency floor: a
  job enqueued right after a worker's most recent empty poll can wait up
  to the cap before anyone asks again. Postgres `LISTEN/NOTIFY` would let
  an idle worker block until woken by an enqueue instead, removing the
  floor entirely - not yet implemented, but the natural next step if
  polling latency ever actually matters more than the simplicity of not
  needing a second connection mode per worker.
