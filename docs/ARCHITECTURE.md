# Architecture

## Delivery guarantees: at-least-once, not exactly-once

This system guarantees **at-least-once** delivery: a job's handler will run
to completion at least once, but under specific failure conditions it can
run more than once. It does not guarantee exactly-once. Every job handler
must therefore be idempotent - safe to execute more than once with the same
payload and end up in the same state as if it had run exactly once (e.g.
`UPDATE balance = 500` rather than `UPDATE balance = balance - 10`).

This isn't a corner someone cut - it's a direct consequence of how the
claim mechanism recovers from a worker that stops responding, and it's
worth understanding exactly why.

### Why exactly-once isn't achievable here

A worker can go quiet mid-job for reasons that have nothing to do with the
job itself: a GC pause, a slow DNS lookup, a container throttled by its CPU
limit, a network partition between the worker and Postgres. From the
system's point of view, "claimed a job and hasn't reported back in a while"
looks identical whether the worker crashed outright or is just running
slow. The system cannot distinguish "dead" from "delayed" without either
waiting forever (unacceptable - a single crashed worker would permanently
wedge every job it touched) or picking a timeout and accepting the
ambiguity.

The **visibility timeout** is that timeout: a job sitting in `running` with
a `claimed_at` older than `JOB_VISIBILITY_TIMEOUT` is presumed abandoned
and reclaimed by the reaper (see `internal/store/reaper.go`) so another
worker can pick it up. Reclaiming is the only reasonable choice once the
timeout is hit - the alternative is jobs that hang forever whenever a
worker happens to crash. But reclaiming while the original worker might
still be alive is exactly what makes at-least-once (rather than
exactly-once) the honest guarantee to offer.

### The scenario this produces, worked through

1. Worker A claims job J. `attempts` becomes 1, `claimed_by = A`.
2. A starts executing J's handler, but hits a slow downstream call (or a GC
   pause, or anything else that isn't a crash) and doesn't report back
   within the visibility timeout. A is still alive and will eventually
   finish.
3. The reaper's next pass sees J's `claimed_at` is older than the
   visibility timeout, increments `attempts` to 2, and sets J back to
   `pending`.
4. Worker B claims J. `attempts` becomes 3, `claimed_by = B`. B runs J's
   handler - the *same* logical unit of work A is still in the middle of.
5. B finishes first and calls `Complete`, which succeeds: `status` becomes
   `succeeded`.
6. A finally finishes its own (redundant) execution of J and calls
   `Complete` too.

Without a safeguard, step 6 would silently overwrite B's result - at best
a no-op, at worst two side effects each execution caused (two emails sent,
two charges made) with the second write masking any evidence of the first.

The safeguard is the **fencing check**: every terminal write
(`CompleteJob`, `MarkDead`, `MarkFailedForRetry` in `internal/store/jobs.go`
and `reaper.go`) is qualified with `WHERE claimed_by = $claimant`, not just
`WHERE id = $id`. By the time A's write in step 6 arrives, J's
`claimed_by` is B, not A - A's `UPDATE` matches zero rows and returns
`store.ErrStaleClaim` instead of touching the row. B's result is what
survives. `internal/store/reliability_integration_test.go`'s
`TestZombieWriterFencing` drives this exact sequence end to end against a
real database and asserts on it.

What the fencing check does *not* do is prevent J's handler from having
run twice - by the time fencing kicks in, both A and B already executed
it. That's the part only the handler itself can make safe, which is why
at-least-once delivery is a contract on handler authors, not just a
property of the queue: **every handler must be idempotent**, because the
system can and will occasionally run one twice.

## Live updates (SSE) are a hint, not a source of truth

`GET /api/v1/events` streams job state transitions over Server-Sent
Events, published via Postgres `NOTIFY`/`LISTEN` (see
`internal/store/notify.go` and `internal/events`). It is convenient for a
UI to feel live, but it is **not a reliable log** of everything that
happened, and building on top of it as if it were is the mistake worth
naming explicitly here.

SSE delivery is at-most-once. A client that hasn't connected yet, that's
mid-reconnect, or that the server dropped for falling behind (see
`events.Hub`'s backpressure handling) simply never sees whatever happened
during that gap - there is no replay, no sequence number, no "catch me
up" mechanism. This isn't a bug to fix; it's inherent to a fan-out
pub/sub built on `NOTIFY`, which itself makes no delivery guarantee to a
listener that wasn't listening at the time.

The correct way to consume this stream, and the way the dashboard
(`web/`, see its `useJobEvents`/`sseReducer`) actually uses it: **fetch
current state first, then treat each event as a signal to refetch, not as
the update itself.** On connect, call the normal REST endpoints (`GET
/api/v1/jobs`, `GET /api/v1/queues`, etc.) to get a real snapshot. After
that, an incoming event means "something changed for this job/queue - go
re-fetch it if you care," not "here is the new state, apply it directly."
A client that treats the stream as authoritative will drift silently out
of sync with reality the first time it misses an event, and have no way
to know it happened.
