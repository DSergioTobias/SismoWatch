import { useCallback, useEffect, useState } from 'react'
import { API_URL, api, authHeaders, getRole, ReportItem } from '../api'

export default function Reports() {
  const [items, setItems] = useState<ReportItem[]>([])
  const [total, setTotal] = useState(0)
  const [state, setState] = useState<'loading' | 'ready' | 'error'>('loading')
  const [error, setError] = useState('')
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(0)
  const [busy, setBusy] = useState(false)
  const limit = 10

  const load = useCallback(async () => {
    setState('loading')
    try {
      const p = new URLSearchParams({ limit: String(limit), offset: String(page * limit) })
      if (search) p.set('search', search)
      const res = await api.reports(p)
      setItems(res.items)
      setTotal(res.total)
      setState('ready')
    } catch (e: any) {
      setError(e.message)
      setState('error')
    }
  }, [page, search])

  useEffect(() => {
    load()
  }, [load])

  async function regenerate() {
    setBusy(true)
    try {
      await api.regenerate()
      setTimeout(load, 4000) // deja tiempo a que Airflow arranque el DAG
    } catch (e: any) {
      alert(`No se pudo lanzar: ${e.message}`)
    } finally {
      setBusy(false)
    }
  }

  function download(format: 'csv' | 'html') {
    const p = new URLSearchParams({ format })
    if (search) p.set('search', search)
    fetch(`${API_URL}/api/reports/export?${p}`, { headers: authHeaders() })
      .then((r) => {
        if (!r.ok) throw new Error(`Error ${r.status}`)
        return r.blob()
      })
      .then((blob) => {
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = `reportes.${format === 'csv' ? 'csv' : 'html'}`
        a.click()
        URL.revokeObjectURL(url)
      })
      .catch((e) => alert(`Exportación fallida: ${e.message}`))
  }

  const pages = Math.max(1, Math.ceil(total / limit))

  return (
    <div className="dashboard">
      <section className="panel">
        <div className="row-between">
          <h2>Reportes diarios (IA)</h2>
          <div className="actions">
            <input
              type="search"
              placeholder="Buscar…"
              value={search}
              onChange={(e) => {
                setSearch(e.target.value)
                setPage(0)
              }}
            />
            <button className="ghost" onClick={() => download('csv')}>Exportar CSV</button>
            <button className="ghost" onClick={() => download('html')}>Exportar HTML</button>
            {getRole() === 'admin' && (
              <button className="primary" onClick={regenerate} disabled={busy}>
                {busy ? 'Lanzando…' : 'Regenerar con IA'}
              </button>
            )}
          </div>
        </div>

        {state === 'loading' && <div className="empty">Cargando reportes…</div>}
        {state === 'error' && (
          <div className="alert error">
            {error}
            <button className="ghost" onClick={load}>Reintentar</button>
          </div>
        )}
        {state === 'ready' && items.length === 0 && (
          <div className="empty">
            Aún no hay reportes. Se generan automáticamente cada día a las 06:30 UTC
            (o usa «Regenerar con IA» si eres admin).
          </div>
        )}

        {items.map((r) => (
          <article key={r.id} className="report card">
            <div className="row-between">
              <h3>{r.title}</h3>
              <span className={`badge ${r.status === 'ok' ? 'viewer' : 'admin'}`}>
                {r.status === 'ok' ? `IA · ${r.model ?? ''}` : 'plan alterno'}
              </span>
            </div>
            <small className="muted">{r.report_date}</small>
            <p>{r.content.summary}</p>
            {r.content.zones?.length > 0 && (
              <p><b>Zonas:</b> {r.content.zones.join(' · ')}</p>
            )}
            {r.content.highlights?.length > 0 && (
              <ul>
                {r.content.highlights.map((h, i) => <li key={i}>{h}</li>)}
              </ul>
            )}
            {r.content.quality_note && (
              <p className="muted small">{r.content.quality_note}</p>
            )}
          </article>
        ))}

        {pages > 1 && (
          <div className="pager">
            <button className="ghost" disabled={page === 0} onClick={() => setPage(page - 1)}>
              ← Anterior
            </button>
            <span className="muted">
              {page + 1} / {pages}
            </span>
            <button
              className="ghost"
              disabled={page + 1 >= pages}
              onClick={() => setPage(page + 1)}
            >
              Siguiente →
            </button>
          </div>
        )}
      </section>
    </div>
  )
}
