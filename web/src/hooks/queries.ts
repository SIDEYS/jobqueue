import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as api from '../api/client'
import type {
  CreateScheduleRequest,
  EnqueueJobRequest,
  ListJobsParams,
  UpdateScheduleRequest,
} from '../api/types'

// Short poll intervals, not real-time push, back most of these - the SSE
// stream (useJobEvents) already tells the app when *something* changed and
// triggers an immediate invalidation of all of these (see
// LiveEventsProvider in App.tsx); the interval here is just a safety net
// for the at-most-once gap in that stream, not the primary refresh path.
const backgroundRefetchMs = 15_000

export function useJobsQuery(params: ListJobsParams) {
  return useQuery({
    queryKey: ['jobs', params],
    queryFn: () => api.listJobs(params),
    refetchInterval: backgroundRefetchMs,
  })
}

export function useJobQuery(id: string | undefined) {
  return useQuery({
    queryKey: ['job', id],
    queryFn: () => api.getJob(id!),
    enabled: !!id,
  })
}

export function useQueueStatsQuery() {
  return useQuery({
    queryKey: ['queueStats'],
    queryFn: api.queueStats,
    refetchInterval: backgroundRefetchMs,
  })
}

export function useWorkersQuery() {
  return useQuery({
    queryKey: ['workers'],
    queryFn: api.listWorkers,
    refetchInterval: backgroundRefetchMs,
  })
}

export function useThroughputQuery() {
  return useQuery({
    queryKey: ['throughput'],
    queryFn: api.throughput,
    refetchInterval: backgroundRefetchMs,
  })
}

export function useSchedulesQuery() {
  return useQuery({
    queryKey: ['schedules'],
    queryFn: api.listSchedules,
    refetchInterval: backgroundRefetchMs,
  })
}

export function useEnqueueJobMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: EnqueueJobRequest) => api.enqueueJob(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['jobs'] })
      queryClient.invalidateQueries({ queryKey: ['queueStats'] })
    },
  })
}

export function useReplayJobMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => api.replayJob(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['jobs'] })
      queryClient.invalidateQueries({ queryKey: ['queueStats'] })
    },
  })
}

export function useCreateScheduleMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: CreateScheduleRequest) => api.createSchedule(body),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['schedules'] }),
  })
}

export function useUpdateScheduleMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, body }: { id: string; body: UpdateScheduleRequest }) => api.updateSchedule(id, body),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['schedules'] }),
  })
}

export function useDeleteScheduleMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => api.deleteSchedule(id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['schedules'] }),
  })
}
