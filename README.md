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

### Cron scheduling

Schedules fire on their own tick loop (`internal/scheduler`), not
`robfig/cron/v3`'s scheduler - only its expression parser is used. Two
decisions worth knowing about:

- **Missed ticks are not replayed.** A schedule's `next_run_at` is always
  computed forward from the current instant (`NextRun(expr, time.Now())`),
  never from its own `last_run_at` or its previous, possibly long-stale,
  `next_run_at`. If the scheduler is down for an hour and a schedule fires
  every five minutes, it fires exactly **once** on the next tick and jumps
  straight to its next real future occurrence - not twelve backlogged
  runs. This is the standard cron semantic (at most once per interval),
  and it's the same thundering-herd concern the retry jitter above exists
  for: a deploy that takes a few minutes must not produce a burst of
  catch-up jobs the moment the scheduler comes back.
- **UTC only.** Cron expressions are evaluated against UTC wall-clock
  time; there's no per-schedule timezone. Cron plus local time plus DST
  is a well-known way to have a job fire twice in November and zero times
  in March - not worth supporting, so it's explicitly not attempted
  rather than silently wrong.

Leader election (which of possibly several scheduler replicas actually
fires due schedules) uses a Postgres advisory lock, but the lock is an
optimization, not what actually prevents a schedule from firing twice -
see [`docs/adr/0001`](docs/adr/0001-advisory-lock-leader-election.md) for
why, and for a subtle failure mode (a connection that still holds the
lock leaking it into the pool) worth understanding before touching
`internal/scheduler/leader.go`.

## Dashboard

`web/` is a Vite + React + TypeScript app (Tailwind, TanStack Query,
Recharts) that gives the API surface a face: an overview of status counts,
queue depth, and throughput; a filterable job list that doubles as the
dead-letter queue view (`?status=dead` plus a per-row replay button); a form
to submit `sleep`/`flaky` demo jobs directly; and full CRUD on schedules.

```
cd web
npm install
npm run dev
```

The dev server proxies `/api/*` to `cmd/api` on `:8080` (see
`vite.config.ts`), so there's nothing CORS-specific to configure in either
direction - in dev or once the built assets are eventually served from the
same origin as the API (Phase 7).

**Live updates, not polling as the primary path.** The dashboard opens one
`EventSource` connection to `GET /api/v1/events` for its whole lifetime
(`useJobEvents`/`JobEventsProvider`) and reacts to each event by
invalidating the relevant TanStack Query caches - a background refetch
interval exists too, but only as a safety net for the stream's inherent
at-most-once gap (a dropped connection misses whatever happened while it
was down), not as the thing driving normal updates. The part of this with
actual correctness risk - folding a stream of events into state without
losing or reordering anything - is isolated into a pure `(state, event) =>
state` function (`src/lib/sseReducer.ts`) specifically so it's unit
testable without a browser event source or any async machinery; that's
also where the reasoning about why last-write-wins is safe here lives.

**Throughput is a dedicated endpoint, not derived client-side.**
`GET /api/v1/stats/throughput` returns succeeded-job counts bucketed by
minute for the last hour, zero-filled server-side so the chart never has
to guess at gaps. One grouped query beats shipping a raw hour of job rows
to the browser to bucket there.

**`cmd/seed`** populates a fresh database with backdated demo history (a
spread of succeeded jobs, a few dead-lettered ones, two recurring
schedules) so the deployed dashboard is never an empty read against an
idle system. It's a no-op if the `jobs` table already has any rows, so
it's safe to run on every boot rather than needing a one-time setup step.

## Deploy

One Docker image (`Dockerfile`), three Fly.io process groups against it
(`fly.toml`): `api` serves HTTP (REST, the embedded dashboard,
`/healthz`/`/readyz`/`/metrics`), `worker` and `scheduler` are headless.
Postgres is external - Neon or Supabase's free tier, not Fly Postgres -
so the database survives independently of the app and there's no volume
to manage.

**One-time setup:**

1. Create a Postgres database on [Neon](https://neon.tech) or
   [Supabase](https://supabase.com) and copy its connection string
   (`postgres://...?sslmode=require` - the managed providers require TLS,
   unlike the local `docker-compose` Postgres).
2. Install and authenticate `flyctl`:
   ```
   brew install flyctl
   flyctl auth login
   ```
3. Reserve the app name from `fly.toml` (edit the `app` field first if
   `jobqueue-sideys` is already taken - Fly app names are globally unique):
   ```
   fly apps create jobqueue-sideys
   ```
4. Set the database connection as a secret - never committed, never in
   `fly.toml`:
   ```
   fly secrets set DATABASE_URL="postgres://...?sslmode=require"
   ```

**Deploy:**

```
fly deploy
```

This builds the image from `Dockerfile`, runs `cmd/seed` as the release
command (a no-op after the first deploy - see the Dashboard section
above), and starts all three process groups. Confirm it's healthy:

```
fly status
curl https://jobqueue-sideys.fly.dev/healthz
```

The dashboard is at the app's root URL - no separate frontend deploy or
CORS configuration needed, since `cmd/api` serves it directly (embedded
via `web/embed.go`, see the Dashboard section).

**Redeploying** after further changes is just `fly deploy` again - the
release command re-runs (still a no-op once seeded) and Fly does a
rolling restart of all three process groups.

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
