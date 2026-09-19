import { useState } from 'react'
import { JobDetailPanel } from '../components/JobDetailPanel'
import { StatusBadge } from '../components/StatusBadge'
import { SubmitJobForm } from '../components/SubmitJobForm'
import { useJobsQuery, useReplayJobMutation } from '../hooks/queries'
import type { JobStatus } from '../api/types'

const statusOptions: (JobStatus | '')[] = ['', 'pending', 'running', 'succeeded', 'failed', 'dead']

export function JobsPage() {
  const [queue, setQueue] = useState('')
  const [jobType, setJobType] = useState('')
  const [status, setStatus] = useState<JobStatus | ''>('')
  const [cursor, setCursor] = useState<string | undefined>(undefined)
  const [cursorHistory, setCursorHistory] = useState<string[]>([])
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null)

  const { data, isLoading } = useJobsQuery({
    queue: queue || undefined,
    job_type: jobType || undefined,
    status: status || undefined,
    cursor,
  })
  const replay = useReplayJobMutation()

  function resetPaging() {
    setCursor(undefined)
    setCursorHistory([])
  }

  function goNext() {
    if (!data?.next_cursor) return
    setCursorHistory((h) => [...h, cursor ?? ''])
    setCursor(data.next_cursor)
  }

  function goPrev() {
    setCursorHistory((h) => {
      const next = [...h]
      const prev = next.pop()
      setCursor(prev || undefined)
      return next
    })
  }

  return (
    <div className="flex flex-col gap-6">
      <section className="rounded-lg border border-slate-800 bg-slate-900 p-4">
        <h2 className="mb-3 text-sm font-medium text-slate-300">Submit a job</h2>
        <SubmitJobForm />
      </section>

      <section className="rounded-lg border border-slate-800 bg-slate-900 p-4">
        <div className="flex flex-wrap items-end gap-3">
          <label className="flex flex-col gap-1 text-xs text-slate-400">
            Queue
            <input
              value={queue}
              onChange={(e) => {
                setQueue(e.target.value)
                resetPaging()
              }}
              placeholder="any"
              className="w-32 rounded border border-slate-700 bg-slate-950 px-2 py-1 text-sm text-slate-100"
            />
          </label>
          <label className="flex flex-col gap-1 text-xs text-slate-400">
            Job type
            <input
              value={jobType}
              onChange={(e) => {
                setJobType(e.target.value)
                resetPaging()
              }}
              placeholder="any"
              className="w-32 rounded border border-slate-700 bg-slate-950 px-2 py-1 text-sm text-slate-100"
            />
          </label>
          <label className="flex flex-col gap-1 text-xs text-slate-400">
            Status
            <select
              value={status}
              onChange={(e) => {
                setStatus(e.target.value as JobStatus | '')
                resetPaging()
              }}
              className="rounded border border-slate-700 bg-slate-950 px-2 py-1 text-sm text-slate-100"
            >
              {statusOptions.map((s) => (
                <option key={s} value={s}>
                  {s || 'any'}
                </option>
              ))}
            </select>
          </label>
          <button
            onClick={() => {
              setStatus('dead')
              resetPaging()
            }}
            className="rounded border border-red-900 px-3 py-1.5 text-sm text-red-300 hover:bg-red-950/40"
          >
            Dead-letter queue
          </button>
        </div>

        <div className="mt-4 overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-slate-800 text-xs uppercase tracking-wide text-slate-500">
                <th className="py-2 pr-4">Status</th>
                <th className="py-2 pr-4">Queue</th>
                <th className="py-2 pr-4">Job type</th>
                <th className="py-2 pr-4">Attempts</th>
                <th className="py-2 pr-4">Updated</th>
                <th className="py-2 pr-4" />
              </tr>
            </thead>
            <tbody>
              {isLoading && (
                <tr>
                  <td colSpan={6} className="py-6 text-center text-slate-500">
                    Loading...
                  </td>
                </tr>
              )}
              {!isLoading && (data?.jobs.length ?? 0) === 0 && (
                <tr>
                  <td colSpan={6} className="py-6 text-center text-slate-500">
                    No jobs match these filters.
                  </td>
                </tr>
              )}
              {data?.jobs.map((job) => (
                <tr
                  key={job.id}
                  onClick={() => setSelectedJobId(job.id)}
                  className="cursor-pointer border-b border-slate-900 hover:bg-slate-800/50"
                >
                  <td className="py-2 pr-4">
                    <StatusBadge status={job.status} />
                  </td>
                  <td className="py-2 pr-4 text-slate-300">{job.queue}</td>
                  <td className="py-2 pr-4 text-slate-300">{job.job_type}</td>
                  <td className="py-2 pr-4 text-slate-400">
                    {job.attempts}/{job.max_attempts}
                  </td>
                  <td className="py-2 pr-4 text-slate-400">{new Date(job.updated_at).toLocaleTimeString()}</td>
                  <td className="py-2 pr-4">
                    {job.status === 'dead' && (
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          replay.mutate(job.id)
                        }}
                        disabled={replay.isPending}
                        className="rounded border border-amber-800 px-2 py-1 text-xs text-amber-300 hover:bg-amber-950/40 disabled:opacity-50"
                      >
                        Replay
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="mt-4 flex items-center gap-3">
          <button
            onClick={goPrev}
            disabled={cursorHistory.length === 0}
            className="rounded border border-slate-700 px-3 py-1 text-xs text-slate-300 disabled:opacity-30"
          >
            Prev
          </button>
          <button
            onClick={goNext}
            disabled={!data?.next_cursor}
            className="rounded border border-slate-700 px-3 py-1 text-xs text-slate-300 disabled:opacity-30"
          >
            Next
          </button>
        </div>
      </section>

      {selectedJobId && <JobDetailPanel jobId={selectedJobId} onClose={() => setSelectedJobId(null)} />}
    </div>
  )
}
