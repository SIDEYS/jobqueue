import { useState } from 'react'
import {
  useCreateScheduleMutation,
  useDeleteScheduleMutation,
  useSchedulesQuery,
  useUpdateScheduleMutation,
} from '../hooks/queries'

type DemoJobType = 'sleep' | 'flaky'

export function SchedulesPage() {
  const { data, isLoading } = useSchedulesQuery()
  const create = useCreateScheduleMutation()
  const update = useUpdateScheduleMutation()
  const del = useDeleteScheduleMutation()

  const [queue, setQueue] = useState('default')
  const [jobType, setJobType] = useState<DemoJobType>('sleep')
  const [cronExpr, setCronExpr] = useState('*/5 * * * *')

  function handleCreate(e: React.FormEvent) {
    e.preventDefault()
    const payload = jobType === 'sleep' ? { duration_ms: 500 } : { fail_rate: 0.3 }
    create.mutate({ queue, job_type: jobType, cron_expr: cronExpr, payload })
  }

  return (
    <div className="flex flex-col gap-6">
      <section className="rounded-lg border border-slate-800 bg-slate-900 p-4">
        <h2 className="mb-3 text-sm font-medium text-slate-300">New schedule</h2>
        <form onSubmit={handleCreate} className="flex flex-wrap items-end gap-3">
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
          <label className="flex flex-col gap-1 text-xs text-slate-400">
            Cron (5-field, UTC)
            <input
              value={cronExpr}
              onChange={(e) => setCronExpr(e.target.value)}
              className="w-40 rounded border border-slate-700 bg-slate-950 px-2 py-1 font-mono text-sm text-slate-100"
              required
            />
          </label>
          <button
            type="submit"
            disabled={create.isPending}
            className="rounded bg-blue-600 px-4 py-1.5 text-sm font-medium text-white hover:bg-blue-500 disabled:opacity-50"
          >
            {create.isPending ? 'Creating...' : 'Create schedule'}
          </button>
          {create.isError && <span className="text-xs text-red-400">{(create.error as Error).message}</span>}
        </form>
      </section>

      <section className="rounded-lg border border-slate-800 bg-slate-900 p-4">
        <h2 className="mb-3 text-sm font-medium text-slate-300">Schedules</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-slate-800 text-xs uppercase tracking-wide text-slate-500">
                <th className="py-2 pr-4">Queue</th>
                <th className="py-2 pr-4">Job type</th>
                <th className="py-2 pr-4">Cron</th>
                <th className="py-2 pr-4">Next run</th>
                <th className="py-2 pr-4">Enabled</th>
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
              {!isLoading && (data?.schedules.length ?? 0) === 0 && (
                <tr>
                  <td colSpan={6} className="py-6 text-center text-slate-500">
                    No schedules yet.
                  </td>
                </tr>
              )}
              {data?.schedules.map((sc) => (
                <tr key={sc.id} className="border-b border-slate-900">
                  <td className="py-2 pr-4 text-slate-300">{sc.queue}</td>
                  <td className="py-2 pr-4 text-slate-300">{sc.job_type}</td>
                  <td className="py-2 pr-4 font-mono text-xs text-slate-400">{sc.cron_expr}</td>
                  <td className="py-2 pr-4 text-slate-400">{new Date(sc.next_run_at).toLocaleTimeString()}</td>
                  <td className="py-2 pr-4">
                    <button
                      onClick={() => update.mutate({ id: sc.id, body: { enabled: !sc.enabled } })}
                      className={`rounded px-2 py-0.5 text-xs font-medium ${
                        sc.enabled ? 'bg-emerald-900 text-emerald-200' : 'bg-slate-700 text-slate-300'
                      }`}
                    >
                      {sc.enabled ? 'enabled' : 'disabled'}
                    </button>
                  </td>
                  <td className="py-2 pr-4">
                    <button
                      onClick={() => del.mutate(sc.id)}
                      className="rounded border border-red-900 px-2 py-1 text-xs text-red-300 hover:bg-red-950/40"
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}
