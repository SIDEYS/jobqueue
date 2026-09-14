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
