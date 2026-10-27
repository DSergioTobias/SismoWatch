import { useEffect, useState } from 'react'
import { api, clearSession, getRole } from './api'
import Login from './components/Login'
import Dashboard from './components/Dashboard'
import Reports from './components/Reports'
import Pipeline from './components/Pipeline'
import Chatbot from './components/Chatbot'

type Tab = 'dashboard' | 'reports' | 'pipeline' | 'chat'

export default function App() {
  const [authed, setAuthed] = useState(false)
  const [role, setRole] = useState<string | null>(null)
  const [tab, setTab] = useState<Tab>('dashboard')
  const [checking, setChecking] = useState(true)

  useEffect(() => {
    // valida la sesión existente consultando un endpoint protegido
    const token = localStorage.getItem('sw_token')
    if (!token) {
      setChecking(false)
      return
    }
    api
      .kpis()
      .then(() => {
        setAuthed(true)
        setRole(getRole())
      })
      .catch(() => clearSession())
      .finally(() => setChecking(false))
  }, [])

  if (checking) return <div className="center-screen">Cargando…</div>

  if (!authed) {
    return (
      <Login
        onSuccess={(r) => {
          setRole(r)
          setAuthed(true)
          setTab('dashboard')
        }}
      />
    )
  }

  const tabs: { id: Tab; label: string }[] = [
    { id: 'dashboard', label: 'Dashboard' },
    { id: 'chat', label: 'Asistente IA' },
    { id: 'reports', label: 'Reportes' },
    { id: 'pipeline', label: 'Pipeline' },
  ]

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="brand-dot" />
          SismoWatch
          <span className="brand-sub">monitoreo sísmico en tiempo real</span>
        </div>
        <nav>
          {tabs.map((t) => (
            <button
              key={t.id}
              className={tab === t.id ? 'tab active' : 'tab'}
              onClick={() => setTab(t.id)}
            >
              {t.label}
            </button>
          ))}
        </nav>
        <div className="userbox">
          <span className={`badge ${role === 'admin' ? 'admin' : 'viewer'}`}>
            {role}
          </span>
          <button
            className="ghost"
            onClick={() => {
              clearSession()
              setAuthed(false)
            }}
          >
            Salir
          </button>
        </div>
      </header>

      <main className="content">
        {tab === 'dashboard' && <Dashboard />}
        {tab === 'chat' && <Chatbot />}
        {tab === 'reports' && <Reports />}
        {tab === 'pipeline' && <Pipeline />}
      </main>
    </div>
  )
}
