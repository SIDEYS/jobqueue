export type JobStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'dead'

export interface Job {
  id: string
  queue: string
  job_type: string
  payload: unknown
  status: JobStatus
  priority: number
  run_at: string
  attempts: number
  max_attempts: number
  last_error?: string
  idempotency_key?: string
  claimed_by?: string
  claimed_at?: string
  created_at: string
  updated_at: string
}

export interface ListJobsResponse {
  jobs: Job[]
  next_cursor?: string
}

export interface ListJobsParams {
  status?: JobStatus
  queue?: string
  job_type?: string
  cursor?: string
  limit?: number
}

export interface QueueStat {
  queue: string
  counts: Partial<Record<JobStatus, number>>
}

export interface QueueStatsResponse {
  queues: QueueStat[]
}

export interface Worker {
  id: string
  hostname: string
  jobs_in_flight: number
  last_heartbeat_at: string
  created_at: string
}

export interface ListWorkersResponse {
  workers: Worker[]
}

export interface Schedule {
  id: string
  queue: string
  job_type: string
  payload: unknown
  priority: number
  max_attempts: number
  cron_expr: string
  next_run_at: string
  last_run_at?: string
  enabled: boolean
  created_at: string
  updated_at: string
}

export interface ListSchedulesResponse {
  schedules: Schedule[]
}

export interface CreateScheduleRequest {
  queue: string
  job_type: string
  payload?: unknown
  priority?: number
  max_attempts?: number
  cron_expr: string
}

export interface UpdateScheduleRequest {
  queue?: string
  job_type?: string
  payload?: unknown
  priority?: number
  max_attempts?: number
  cron_expr?: string
  enabled?: boolean
}

export interface ThroughputBucket {
  minute: string
  succeeded: number
}

export interface ThroughputResponse {
  buckets: ThroughputBucket[]
}

export interface EnqueueJobRequest {
  queue: string
  job_type: string
  payload?: unknown
  priority?: number
  max_attempts?: number
  idempotency_key?: string
}

// The one payload shape every event carries (see internal/events.Event) -
// deliberately minimal, matching the store layer's NOTIFY payload cap.
export interface JobEvent {
  id: string
  status: JobStatus
  queue: string
}

export interface ApiErrorBody {
  error: string
}
