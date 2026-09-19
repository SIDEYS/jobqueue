import { useState } from 'react'
import { JobEventsProvider } from './hooks/useJobEvents'
import { JobsPage } from './pages/JobsPage'
import { OverviewPage } from './pages/OverviewPage'
import { SchedulesPage } from './pages/SchedulesPage'

type Tab = 'overview' | 'jobs' | 'schedules'

const tabs: { id: Tab; label: string }[] = [
  { id: 'overview', label: 'Overview' },
  { id: 'jobs', label: 'Jobs' },
  { id: 'schedules', label: 'Schedules' },
]

function App() {
  const [tab, setTab] = useState<Tab>('overview')

  return (
    <JobEventsProvider>
      <div className="min-h-screen bg-slate-950 text-slate-100">
        <header className="border-b border-slate-800 px-6 py-4">
          <h1 className="text-lg font-semibold">jobqueue</h1>
          <nav className="mt-3 flex gap-1">
            {tabs.map((t) => (
              <button
                key={t.id}
                onClick={() => setTab(t.id)}
                className={`rounded px-3 py-1.5 text-sm font-medium transition-colors ${
                  tab === t.id ? 'bg-slate-800 text-white' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                {t.label}
              </button>
            ))}
          </nav>
        </header>
        <main className="mx-auto max-w-5xl p-6">
          {tab === 'overview' && <OverviewPage />}
          {tab === 'jobs' && <JobsPage />}
          {tab === 'schedules' && <SchedulesPage />}
        </main>
      </div>
    </JobEventsProvider>
  )
}

export default App
