import { describe, expect, it } from 'vitest'
import type { Job } from '../api/types'
import { applyLiveStatus, sseReducer, type JobEventState } from './sseReducer'

describe('sseReducer', () => {
  it('adds a new job to empty state', () => {
    const state = sseReducer({}, { id: 'a', status: 'pending', queue: 'default' })
    expect(state).toEqual({ a: { id: 'a', status: 'pending', queue: 'default' } })
  })

  it('overwrites an existing job with its latest status', () => {
    const initial: JobEventState = { a: { id: 'a', status: 'pending', queue: 'default' } }
    const state = sseReducer(initial, { id: 'a', status: 'running', queue: 'default' })
    expect(state.a.status).toBe('running')
  })

  it('leaves unrelated jobs untouched', () => {
    const initial: JobEventState = {
      a: { id: 'a', status: 'pending', queue: 'default' },
      b: { id: 'b', status: 'succeeded', queue: 'default' },
    }
    const state = sseReducer(initial, { id: 'a', status: 'running', queue: 'default' })
    expect(state.b).toEqual(initial.b)
  })

  it('does not mutate the input state', () => {
    const initial: JobEventState = { a: { id: 'a', status: 'pending', queue: 'default' } }
    const frozen = Object.freeze({ ...initial })
    expect(() => sseReducer(frozen, { id: 'a', status: 'running', queue: 'default' })).not.toThrow()
    expect(frozen.a.status).toBe('pending')
  })

  it('tracks multiple distinct jobs independently across a sequence of events', () => {
    let state: JobEventState = {}
    state = sseReducer(state, { id: 'a', status: 'pending', queue: 'default' })
    state = sseReducer(state, { id: 'b', status: 'pending', queue: 'reports' })
    state = sseReducer(state, { id: 'a', status: 'succeeded', queue: 'default' })

    expect(state).toEqual({
      a: { id: 'a', status: 'succeeded', queue: 'default' },
      b: { id: 'b', status: 'pending', queue: 'reports' },
    })
  })
})

const baseJob: Job = {
  id: 'a',
  queue: 'default',
  job_type: 'sleep',
  payload: { duration_ms: 100 },
  status: 'pending',
  priority: 0,
  run_at: '2026-01-01T00:00:00Z',
  attempts: 0,
  max_attempts: 5,
  created_at: '2026-01-01T00:00:00Z',
  updated_at: '2026-01-01T00:00:00Z',
}

describe('applyLiveStatus', () => {
  it('returns the job unchanged when nothing live is known about it', () => {
    expect(applyLiveStatus(baseJob, {})).toEqual(baseJob)
  })

  it('overlays a newer status from the live map', () => {
    const live: JobEventState = { a: { id: 'a', status: 'succeeded', queue: 'default' } }
    expect(applyLiveStatus(baseJob, live).status).toBe('succeeded')
  })

  it('does not touch fields other than status', () => {
    const live: JobEventState = { a: { id: 'a', status: 'running', queue: 'default' } }
    const patched = applyLiveStatus(baseJob, live)
    expect(patched.payload).toEqual(baseJob.payload)
    expect(patched.attempts).toBe(baseJob.attempts)
  })

  it('ignores live events for other job ids', () => {
    const live: JobEventState = { z: { id: 'z', status: 'succeeded', queue: 'default' } }
    expect(applyLiveStatus(baseJob, live)).toEqual(baseJob)
  })
})
