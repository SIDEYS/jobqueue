import type {
  ApiErrorBody,
  CreateScheduleRequest,
  EnqueueJobRequest,
  Job,
  ListJobsParams,
  ListJobsResponse,
  ListSchedulesResponse,
  ListWorkersResponse,
  QueueStatsResponse,
  Schedule,
  ThroughputResponse,
  UpdateScheduleRequest,
} from './types'

// Relative paths only - the Vite dev server proxies /api to cmd/api
// (:8080, see vite.config.ts), and in production the built assets are
// served by the same origin as the API (see docs/deploy notes, Phase 7).
// There is deliberately no configurable base URL.
class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { 'Content-Type': 'application/json', ...init?.headers },
  })
  if (!res.ok) {
    let message = res.statusText
    try {
      const body = (await res.json()) as ApiErrorBody
      if (body.error) message = body.error
    } catch {
      // Body wasn't JSON (or was empty) - fall back to the status text.
    }
    throw new ApiError(res.status, message)
  }
  if (res.status === 204) return undefined as T
  return (await res.json()) as T
}

export function listJobs(params: ListJobsParams = {}): Promise<ListJobsResponse> {
  const qs = new URLSearchParams()
  if (params.status) qs.set('status', params.status)
  if (params.queue) qs.set('queue', params.queue)
  if (params.job_type) qs.set('job_type', params.job_type)
  if (params.cursor) qs.set('cursor', params.cursor)
  if (params.limit) qs.set('limit', String(params.limit))
  const suffix = qs.toString() ? `?${qs}` : ''
  return request<ListJobsResponse>(`/api/v1/jobs${suffix}`)
}

export function getJob(id: string): Promise<Job> {
  return request<Job>(`/api/v1/jobs/${id}`)
}

export function enqueueJob(body: EnqueueJobRequest): Promise<Job> {
  return request<Job>('/api/v1/jobs', { method: 'POST', body: JSON.stringify(body) })
}

export function replayJob(id: string): Promise<Job> {
  return request<Job>(`/api/v1/jobs/${id}/replay`, { method: 'POST' })
}

export function queueStats(): Promise<QueueStatsResponse> {
  return request<QueueStatsResponse>('/api/v1/queues')
}

export function listWorkers(): Promise<ListWorkersResponse> {
  return request<ListWorkersResponse>('/api/v1/workers')
}

export function throughput(): Promise<ThroughputResponse> {
  return request<ThroughputResponse>('/api/v1/stats/throughput')
}

export function listSchedules(): Promise<ListSchedulesResponse> {
  return request<ListSchedulesResponse>('/api/v1/schedules')
}

export function createSchedule(body: CreateScheduleRequest): Promise<Schedule> {
  return request<Schedule>('/api/v1/schedules', { method: 'POST', body: JSON.stringify(body) })
}

export function updateSchedule(id: string, body: UpdateScheduleRequest): Promise<Schedule> {
  return request<Schedule>(`/api/v1/schedules/${id}`, { method: 'PATCH', body: JSON.stringify(body) })
}

export function deleteSchedule(id: string): Promise<void> {
  return request<void>(`/api/v1/schedules/${id}`, { method: 'DELETE' })
}

export { ApiError }
