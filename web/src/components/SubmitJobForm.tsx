import { useState } from 'react'
import { useEnqueueJobMutation } from '../hooks/queries'

type DemoJobType = 'sleep' | 'flaky'

// Only the two handlers cmd/worker actually registers (RegisterDemoHandlers,
// internal/worker/handlers.go) - anything else would enqueue a job no
// running worker knows how to execute.
export function SubmitJobForm() {
  const [queue, setQueue] = useState('default')
  const [jobType, setJobType] = useState<DemoJobType>('sleep')
  const [durationMs, setDurationMs] = useState(500)
  const [failRate, setFailRate] = useState(0.3)

  const enqueue = useEnqueueJobMutation()

  const payload = jobType === 'sleep' ? { duration_ms: durationMs } : { fail_rate: failRate }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    enqueue.mutate({ queue, job_type: jobType, payload, max_attempts: 5 })
  }

  return (
    <form onSubmit={handleSubmit} className="flex flex-wrap items-end gap-3">
      <label className="flex flex-col gap-1 text-xs text-slate-400">
        Queue
        <input
          value={queue}
          onChange={(e) => setQueue(e.target.value)}
          className="w-32 rounded border border-slate-700 bg-slate-950 px-2 py-1 text-sm text-slate-100"
          required
        />
      </label>

      <label className="flex flex-col gap-1 text-xs text-slate-400">
        Job type
        <select
          value={jobType}
          onChange={(e) => setJobType(e.target.value as DemoJobType)}
          className="rounded border border-slate-700 bg-slate-950 px-2 py-1 text-sm text-slate-100"
        >
          <option value="sleep">sleep</option>
          <option value="flaky">flaky</option>
        </select>
      </label>

      {jobType === 'sleep' ? (
        <label className="flex flex-col gap-1 text-xs text-slate-400">
          Duration (ms)
          <input
            type="number"
            min={0}
            value={durationMs}
            onChange={(e) => setDurationMs(Number(e.target.value))}
            className="w-28 rounded border border-slate-700 bg-slate-950 px-2 py-1 text-sm text-slate-100"
          />
        </label>
      ) : (
        <label className="flex flex-col gap-1 text-xs text-slate-400">
          Fail rate (0-1)
          <input
            type="number"
            min={0}
            max={1}
            step={0.1}
            value={failRate}
            onChange={(e) => setFailRate(Number(e.target.value))}
            className="w-28 rounded border border-slate-700 bg-slate-950 px-2 py-1 text-sm text-slate-100"
          />
        </label>
      )}

      <button
        type="submit"
        disabled={enqueue.isPending}
        className="rounded bg-blue-600 px-4 py-1.5 text-sm font-medium text-white hover:bg-blue-500 disabled:opacity-50"
      >
        {enqueue.isPending ? 'Submitting...' : 'Submit job'}
      </button>

      {enqueue.isSuccess && (
        <span className="text-xs text-emerald-400">
          Enqueued {enqueue.data.id.slice(0, 8)}
        </span>
      )}
      {enqueue.isError && <span className="text-xs text-red-400">{(enqueue.error as Error).message}</span>}
    </form>
  )
}
