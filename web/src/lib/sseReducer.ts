import type { Job, JobEvent } from '../api/types'

export type JobEventState = Record<string, JobEvent>

export const initialJobEventState: JobEventState = {}

// Pure by design, kept separate from the EventSource plumbing in
// useJobEvents so it's testable without a browser event source or any
// asynchronous machinery. Each event just overwrites whatever was known
// about that job id - last-write-wins is correct here because a single SSE
// connection preserves the server's send order, which itself mirrors
// Postgres NOTIFY delivery order within each transaction (see
// internal/store/notify.go), so there's no reordering to guard against
// within one connection's lifetime. A dropped/reconnected connection can
// still miss events entirely - that's the at-most-once gap this state is
// only ever a hint layered on top of a real fetch, never authoritative on
// its own (see docs/ARCHITECTURE.md).
export function sseReducer(state: JobEventState, event: JobEvent): JobEventState {
  return { ...state, [event.id]: event }
}

// Overlays a job's live status onto its last-fetched record, if the stream
// has said anything newer about it since the fetch. Only status ever
// changes this way - queue is fixed at enqueue time, and everything else
// (payload, attempts, last_error) isn't part of the event's minimal
// payload, so a real refetch is still what a UI should do once it notices
// a job it cares about has changed state.
export function applyLiveStatus(job: Job, live: JobEventState): Job {
  const event = live[job.id]
  if (!event) return job
  return { ...job, status: event.status }
}
