import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { ThroughputBucket } from '../api/types'

function formatMinute(iso: string): string {
  return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

export function ThroughputChart({ buckets }: { buckets: ThroughputBucket[] }) {
  const data = buckets.map((b) => ({ label: formatMinute(b.minute), succeeded: b.succeeded }))

  return (
    <ResponsiveContainer width="100%" height={220}>
      <LineChart data={data} margin={{ top: 8, right: 16, left: -16, bottom: 0 }}>
        <XAxis
          dataKey="label"
          tick={{ fontSize: 11, fill: '#94a3b8' }}
          interval={9}
          axisLine={{ stroke: '#334155' }}
          tickLine={false}
        />
        <YAxis
          allowDecimals={false}
          tick={{ fontSize: 11, fill: '#94a3b8' }}
          axisLine={{ stroke: '#334155' }}
          tickLine={false}
        />
        <Tooltip
          contentStyle={{ background: '#0f172a', border: '1px solid #334155', fontSize: 12 }}
          labelStyle={{ color: '#cbd5e1' }}
          formatter={(value) => [`${value} succeeded`, '']}
        />
        <Line type="monotone" dataKey="succeeded" stroke="#34d399" strokeWidth={2} dot={false} />
      </LineChart>
    </ResponsiveContainer>
  )
}
