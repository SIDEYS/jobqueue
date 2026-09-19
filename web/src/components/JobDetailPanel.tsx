import { useJobQuery, useReplayJobMutation } from '../hooks/queries'
import { StatusBadge } from './StatusBadge'

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-0.5 text-sm text-slate-200">{children}</div>
    </div>
  )
}

export function JobDetailPanel({ jobId, onClose }: { jobId: string; onClose: () => void }) {
  const { data: job, isLoading } = useJobQuery(jobId)
  const replay = useReplayJobMutation()

  return (
    <div className="fixed inset-0 z-20 flex justify-end bg-black/50" onClick={onClose}>
      <div
        className="h-full w-full max-w-lg overflow-y-auto border-l border-slate-800 bg-slate-950 p-6"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold text-slate-100">Job detail</h2>
          <button onClick={onClose} className="text-sm text-slate-400 hover:text-slate-200">
            Close
          </button>
        </div>

        {isLoading || !job ? (
          <p className="mt-6 text-sm text-slate-500">Loading...</p>
        ) : (
          <div className="mt-6 flex flex-col gap-4">
            <Field label="ID">
              <code className="text-xs">{job.id}</code>
            </Field>
            <div className="grid grid-cols-2 gap-4">
              <Field label="Status">
                <StatusBadge status={job.status} />
              </Field>
              <Field label="Queue">{job.queue}</Field>
              <Field label="Job type">{job.job_type}</Field>
              <Field label="Priority">{job.priority}</Field>
              <Field label="Attempts">
                {job.attempts} / {job.max_attempts}
              </Field>
              <Field label="Claimed by">{job.claimed_by ?? '-'}</Field>
              <Field label="Run at">{new Date(job.run_at).toLocaleString()}</Field>
              <Field label="Updated at">{new Date(job.updated_at).toLocaleString()}</Field>
            </div>

            {job.last_error && (
              <Field label="Last error">
                <pre className="mt-1 max-h-40 overflow-auto rounded bg-red-950/40 p-2 text-xs text-red-300">
                  {job.last_error}
                </pre>
              </Field>
            )}

            <Field label="Payload">
              <pre className="mt-1 max-h-60 overflow-auto rounded bg-slate-900 p-2 text-xs text-slate-300">
                {JSON.stringify(job.payload, null, 2)}
              </pre>
            </Field>

            {job.status === 'dead' && (
              <button
                onClick={() => replay.mutate(job.id)}
                disabled={replay.isPending}
                className="self-start rounded bg-amber-600 px-4 py-1.5 text-sm font-medium text-white hover:bg-amber-500 disabled:opacity-50"
              >
                {replay.isPending ? 'Replaying...' : 'Replay'}
              </button>
            )}
            {replay.isError && <p className="text-xs text-red-400">{(replay.error as Error).message}</p>}
          </div>
        )}
      </div>
    </div>
  )
}
