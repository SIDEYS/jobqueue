import { useQueryClient } from '@tanstack/react-query'
import { createContext, useContext, useEffect, useReducer, type ReactNode } from 'react'
import type { JobEvent } from '../api/types'
import { initialJobEventState, sseReducer, type JobEventState } from '../lib/sseReducer'

const JobEventsContext = createContext<JobEventState>(initialJobEventState)

// Owns the single EventSource connection for the whole app - every page
// that wants live status (the jobs table, the overview) reads from this
// same context instead of each opening its own subscription. On every
// event it also invalidates the queries whose data the event payload
// can't fully describe on its own (aggregate counts, throughput) - see
// applyLiveStatus in sseReducer.ts for the narrower per-row patch this
// context also exposes directly.
export function JobEventsProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(sseReducer, initialJobEventState)
  const queryClient = useQueryClient()

  useEffect(() => {
    const source = new EventSource('/api/v1/events')

    source.onmessage = (msg) => {
      try {
        const event = JSON.parse(msg.data) as JobEvent
        dispatch(event)
      } catch {
        // A malformed frame isn't actionable - the next real event still
        // gets through on the same connection.
      }
    }

    return () => source.close()
  }, [dispatch])

  useEffect(() => {
    if (state === initialJobEventState) return
    queryClient.invalidateQueries({ queryKey: ['jobs'] })
    queryClient.invalidateQueries({ queryKey: ['queueStats'] })
    queryClient.invalidateQueries({ queryKey: ['workers'] })
    queryClient.invalidateQueries({ queryKey: ['throughput'] })
  }, [state, queryClient])

  return <JobEventsContext.Provider value={state}>{children}</JobEventsContext.Provider>
}

export function useJobEvents(): JobEventState {
  return useContext(JobEventsContext)
}
