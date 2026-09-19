import { ThroughputChart } from '../components/ThroughputChart'
import { useQueueStatsQuery, useThroughputQuery } from '../hooks/queries'
import type { JobStatus } from '../api/types'

const statusOrder: JobStatus[] = ['pending', 'running', 'succeeded', 'failed', 'dead']

const statusLabels: Record<JobStatus, string> = {
  pending: 'Pending',
  running: 'Running',
  succeeded: 'Succeeded',
  failed: 'Failed',
  dead: 'Dead',
}

export function OverviewPage() {
  const { data: stats, isLoading: statsLoading } = useQueueStatsQuery()
  const { data: throughputData, isLoading: throughputLoading } = useThroughputQuery()

  const totals: Record<JobStatus, number> = {
    pending: 0,
    running: 0,
    succeeded: 0,
    failed: 0,
    dead: 0,
  }
  for (const q of stats?.queues ?? []) {
    for (const status of statusOrder) {
      totals[status] += q.counts[status] ?? 0
    }
  }

  const queueDepths = (stats?.queues ?? [])
    .map((q) => ({ queue: q.queue, depth: (q.counts.pending ?? 0) + (q.counts.running ?? 0) }))
    .sort((a, b) => b.depth - a.depth)
  const maxDepth = Math.max(1, ...queueDepths.map((q) => q.depth))

  return (
    <div className="flex flex-col gap-6">
      <section className="grid grid-cols-2 gap-3 sm:grid-cols-5">
        {statusOrder.map((status) => (
          <div key={status} className="rounded-lg border border-slate-800 bg-slate-900 p-4">
            <div className="text-xs font-medium uppercase tracking-wide text-slate-500">
              {statusLabels[status]}
            </div>
            <div className="mt-1 text-2xl font-semibold text-slate-100">
              {statsLoading ? '-' : totals[status]}
            </div>
          </div>
        ))}
      </section>

      <section className="rounded-lg border border-slate-800 bg-slate-900 p-4">
        <h2 className="text-sm font-medium text-slate-300">Throughput (last hour)</h2>
        <p className="text-xs text-slate-500">Jobs succeeded per minute</p>
        <div className="mt-2">
          {throughputLoading ? (
            <div className="flex h-[220px] items-center justify-center text-sm text-slate-500">Loading...</div>
          ) : (
            <ThroughputChart buckets={throughputData?.buckets ?? []} />
          )}
        </div>
      </section>

      <section className="rounded-lg border border-slate-800 bg-slate-900 p-4">
        <h2 className="text-sm font-medium text-slate-300">Queue depth</h2>
        <p className="text-xs text-slate-500">Pending + running jobs per queue</p>
        <div className="mt-3 flex flex-col gap-2">
          {queueDepths.length === 0 && !statsLoading && (
            <p className="text-sm text-slate-500">No queues yet.</p>
          )}
          {queueDepths.map((q) => (
            <div key={q.queue} className="flex items-center gap-3">
              <span className="w-28 shrink-0 truncate text-sm text-slate-300">{q.queue}</span>
              <div className="h-2 flex-1 rounded bg-slate-800">
                <div
                  className="h-2 rounded bg-blue-500"
                  style={{ width: `${(q.depth / maxDepth) * 100}%` }}
                />
              </div>
              <span className="w-8 shrink-0 text-right text-sm text-slate-400">{q.depth}</span>
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}
