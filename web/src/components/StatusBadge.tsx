import type { JobStatus } from '../api/types'

const styles: Record<JobStatus, string> = {
  pending: 'bg-slate-700 text-slate-200',
  running: 'bg-blue-900 text-blue-200',
  succeeded: 'bg-emerald-900 text-emerald-200',
  failed: 'bg-amber-900 text-amber-200',
  dead: 'bg-red-900 text-red-200',
}

export function StatusBadge({ status }: { status: JobStatus }) {
  return (
    <span className={`inline-block rounded px-2 py-0.5 text-xs font-medium ${styles[status]}`}>{status}</span>
  )
}
