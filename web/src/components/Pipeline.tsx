import { useCallback, useEffect, useState } from 'react'
import { api, getRole, PipelineStatus } from '../api'

export default function Pipeline() {
  const [data, setData] = useState<PipelineStatus | null>(null)
  const [state, setState] = useState<'loading' | 'ready' | 'error'>('loading')
  const [error, setError] = useState('')
  const [quarantine, setQuarantine] = useState<any[] | null>(null)
  const [qError, setQError] = useState('')

  const load = useCallback(async () => {
    setState('loading')
    try {
      const res = await api.pipeline()
      setData(res)
      setState('ready')
    } catch (e: any) {
      setError(e.message)
      setState('error')
    }
  }, [])

  useEffect(() => {
    load()
    const id = setInterval(load, 30000) // refresco corto
    return () => clearInterval(id)
  }, [load])

  async function loadQuarantine() {
    try {
      const res = await api.quarantine(new URLSearchParams({ limit: '20' }))
      setQuarantine(res.items)
      setQError('')
    } catch (e: any) {
      setQError(e.message)
    }
  }

  if (state === 'loading' && !data) return <div className="panel">Cargando estado del pipeline…</div>
  if (state === 'error' && !data) {
    return (
      <div className="panel">
        <div className="alert error">
          {error}
          <button className="ghost" onClick={load}>Reintentar</button>
        </div>
      </div>
    )
  }

  const isAdmin = getRole() === 'admin'

  return (
    <div className="dashboard">
      <section className="panel">
        <div className="row-between">
          <h2>Estado del pipeline</h2>
          <button className="ghost" onClick={load}>Actualizar</button>
        </div>

        <div className={`freshness ${data?.freshness.fresh ? 'ok' : 'bad'}`}>
          {data?.freshness.fresh
            ? 'Datos frescos (última ingesta < 30 min)'
            : data?.freshness.last_ingest
              ? `Última ingesta: ${new Date(data.freshness.last_ingest).toLocaleString()} — posiblemente obsoleto`
              : 'Sin ingesta registrada'}
        </div>

        <h3>DAGs</h3>
        {data?.dags.length ? (
          <table className="table">
            <thead>
              <tr><th>DAG</th><th>Último estado</th><th>Run</th><th>Ejecución</th></tr>
            </thead>
            <tbody>
              {data.dags.map((d) => (
                <tr key={d.dag_id}>
                  <td>{d.dag_id}</td>
                  <td>
                    <span className={`badge ${state2class(d.last_state)}`}>
                      {d.last_state ?? 'sin ejecuciones'}
                    </span>
                  </td>
                  <td className="muted">{d.last_run_id ?? '—'}</td>
                  <td className="muted">
                    {d.last_execution ? new Date(d.last_execution).toLocaleString() : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty">No se encontraron DAGs (¿Airflow disponible?)</div>
        )}

        <h3>Resultados de calidad</h3>
        {data?.quality.length ? (
          <table className="table">
            <thead>
              <tr>
                <th>Ejecución</th><th>Puntaje</th><th>Estado</th>
                <th>Total</th><th>Válidos</th><th>Rechazados</th><th>Fecha</th>
              </tr>
            </thead>
            <tbody>
              {data.quality.map((q, i) => (
                <tr key={i}>
                  <td className="muted">{q.dag_run_id}</td>
                  <td><b>{q.score}</b></td>
                  <td>
                    <span className={`badge ${q.status === 'pass' ? 'viewer' : 'admin'}`}>
                      {q.status}
                    </span>
                  </td>
                  <td>{q.records_total}</td>
                  <td>{q.records_valid}</td>
                  <td>{q.records_rejected}</td>
                  <td className="muted">{new Date(q.evaluated_at).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty">Aún no hay resultados de calidad registrados.</div>
        )}
      </section>

      {isAdmin && (
        <section className="panel">
          <div className="row-between">
            <h2>Registros en cuarentena {data?.quarantine_count != null && `(${data.quarantine_count})`}</h2>
            <button className="ghost" onClick={loadQuarantine}>Cargar</button>
          </div>
          {qError && <div className="alert error">{qError}</div>}
          {quarantine === null && <div className="empty">Pulsa «Cargar» para ver los registros rechazados.</div>}
          {quarantine?.length === 0 && <div className="empty">Sin registros en cuarentena 🎉</div>}
          {quarantine?.map((q) => (
            <div key={q.id} className="card quarantine">
              <div className="row-between">
                <b>{q.event_uid}</b>
                <small className="muted">{new Date(q.detected_at).toLocaleString()}</small>
              </div>
              <ul>
                {(q.reasons || []).map((r: any, i: number) => (
                  <li key={i}><code>{r.rule}</code> — {r.detail}</li>
                ))}
              </ul>
            </div>
          ))}
        </section>
      )}
    </div>
  )
}

function state2class(state: string | null) {
  if (state === 'success') return 'viewer'
  if (state === 'running') return 'running'
  if (state === 'failed') return 'admin'
  return ''
}
