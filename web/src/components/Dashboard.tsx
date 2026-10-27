import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer,
  Tooltip, XAxis, YAxis,
} from 'recharts'
import { MapContainer, Marker, Popup, TileLayer } from 'react-leaflet'
import L from 'leaflet'
import { api, authHeaders, EventItem, Kpis, streamUrl } from '../api'

const icon = (mag: number | null) =>
  L.divIcon({
    className: '',
    html: `<div class="quake-dot" style="width:${size(mag)}px;height:${size(mag)}px"></div>`,
    iconSize: [size(mag), size(mag)],
  })

function size(mag: number | null) {
  const m = mag ?? 1
  return Math.max(8, Math.min(34, 8 + m * 4))
}

interface LiveEvent extends EventItem {
  live?: boolean
}

export default function Dashboard() {
  const [kpis, setKpis] = useState<Kpis | null>(null)
  const [stats, setStats] = useState<{ hourly: any[]; by_magnitude: any[]; regions: any[] } | null>(null)
  const [events, setEvents] = useState<LiveEvent[]>([])
  const [state, setState] = useState<'loading' | 'ready' | 'error'>('loading')
  const [errorMsg, setErrorMsg] = useState('')
  const [live, setLive] = useState(false)
  const [lastUpdate, setLastUpdate] = useState<string | null>(null)
  const [reconnecting, setReconnecting] = useState(false)

  const [start, setStart] = useState('')
  const [end, setEnd] = useState('')
  const [minMag, setMinMag] = useState('')
  const [region, setRegion] = useState('')

  const filters = useMemo(() => {
    const p = new URLSearchParams()
    if (start) p.set('start', start)
    if (end) p.set('end', end)
    if (minMag) p.set('min_mag', minMag)
    if (region) p.set('region', region)
    return p
  }, [start, end, minMag, region])

  const load = useCallback(async () => {
    setState('loading')
    try {
      const [k, s, e] = await Promise.all([
        api.kpis(),
        api.timeseries(filters),
        api.mapEvents(filters),
      ])
      setKpis(k)
      setStats(s)
      setEvents(e.items)
      setLastUpdate(k.last_update)
      setState('ready')
    } catch (err: any) {
      setErrorMsg(err.message || 'Error cargando datos')
      setState('error')
    }
  }, [filters])

  useEffect(() => {
    load()
  }, [load])

  // -------- Tiempo real por SSE (EventSource no permite headers; usamos fetch-stream) --------
  useEffect(() => {
    let closed = false
    let controller: AbortController | null = null

    async function connect() {
      if (closed) return
      controller = new AbortController()
      try {
        const res = await fetch(streamUrl(), {
          headers: authHeaders(),
          signal: controller.signal,
        })
        if (!res.ok || !res.body) throw new Error(`stream ${res.status}`)
        setLive(true)
        setReconnecting(false)
        const reader = res.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ''
        for (;;) {
          const { done, value } = await reader.read()
          if (done || closed) break
          buffer += decoder.decode(value, { stream: true })
          const chunks = buffer.split('\n\n')
          buffer = chunks.pop() || ''
          for (const chunk of chunks) {
            const line = chunk.split('\n').find((l) => l.startsWith('data: '))
            if (!line) continue
            try {
              const payload = JSON.parse(line.slice(6))
              if (payload.type === 'events' && payload.items?.length) {
                setEvents((prev) => {
                  const ids = new Set(prev.map((e) => e.event_uid))
                  const fresh = payload.items
                    .filter((i: EventItem) => !ids.has(i.event_uid))
                    .map((i: EventItem) => ({ ...i, live: true }))
                  return [...fresh, ...prev].slice(0, 500)
                })
                setLastUpdate(payload.last_update)
              } else if (payload.last_update) {
                setLastUpdate(payload.last_update)
              }
            } catch {
              /* chunk incompleto */
            }
          }
        }
      } catch {
        /* caída del stream */
      } finally {
        setLive(false)
        if (!closed) {
          setReconnecting(true)
          setTimeout(connect, 5000) // reconexión automática
        }
      }
    }
    connect()
    return () => {
      closed = true
      controller?.abort()
    }
  }, [])

  if (state === 'loading' && !kpis) return <div className="panel">Cargando dashboard…</div>

  if (state === 'error' && !kpis) {
    return (
      <div className="panel">
        <div className="alert error">
          {errorMsg}
          <button className="ghost" onClick={load}>Reintentar</button>
        </div>
      </div>
    )
  }

  return (
    <div className="dashboard">
      {/* ---------- KPIs ---------- */}
      <section className="kpi-grid">
        <KpiCard label="Eventos 24h" value={kpis?.events_24h ?? '—'} />
        <KpiCard label="Magnitud máxima" value={kpis?.max_mag ?? '—'} accent />
        <KpiCard label="Magnitud promedio" value={kpis?.avg_mag ?? '—'} />
        <KpiCard label="Alertas tsunami" value={kpis?.tsunami_events ?? '—'} />
        <KpiCard
          label="Puntaje de calidad"
          value={kpis?.quality_score ?? '—'}
          hint={kpis?.quality_status ?? undefined}
        />
        <KpiCard
          label="Última actualización"
          value={lastUpdate ? new Date(lastUpdate).toLocaleTimeString() : '—'}
          hint={
            <span className={live ? 'live on' : 'live off'}>
              {live ? '● en vivo' : reconnecting ? '◌ reconectando…' : '○ desconectado'}
            </span>
          }
        />
      </section>

      {/* ---------- Filtros ---------- */}
      <section className="panel filters">
        <label>
          Desde
          <input type="datetime-local" value={start} onChange={(e) => setStart(e.target.value)} />
        </label>
        <label>
          Hasta
          <input type="datetime-local" value={end} onChange={(e) => setEnd(e.target.value)} />
        </label>
        <label>
          Mag. mínima
          <input
            type="number"
            step="0.1"
            min="-2"
            max="10"
            placeholder="0"
            value={minMag}
            onChange={(e) => setMinMag(e.target.value)}
          />
        </label>
        <label>
          Región
          <input
            type="text"
            placeholder="p. ej. Chile"
            value={region}
            onChange={(e) => setRegion(e.target.value)}
          />
        </label>
        <button className="ghost" onClick={() => { setStart(''); setEnd(''); setMinMag(''); setRegion('') }}>
          Limpiar
        </button>
        {state === 'loading' && <span className="muted">actualizando…</span>}
      </section>

      {/* ---------- Gráficos ---------- */}
      <section className="charts">
        <div className="panel">
          <h3>Actividad por hora</h3>
          {stats?.hourly.length ? (
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={stats.hourly}>
                <CartesianGrid strokeDasharray="3 3" stroke="#243044" />
                <XAxis
                  dataKey="bucket"
                  tickFormatter={(v) => new Date(v).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  stroke="#8aa0bd"
                  fontSize={11}
                />
                <YAxis stroke="#8aa0bd" fontSize={11} allowDecimals={false} />
                <Tooltip
                  contentStyle={{ background: '#101a2b', border: '1px solid #2c3d58' }}
                  labelFormatter={(v) => new Date(v).toLocaleString()}
                />
                <Legend />
                <Line type="monotone" dataKey="events" name="Eventos" stroke="#4ea1ff" dot={false} />
                <Line type="monotone" dataKey="max_mag" name="Mag. máxima" stroke="#ff9f43" dot={false} />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <Empty text="Sin datos en el rango seleccionado" />
          )}
        </div>

        <div className="panel">
          <h3>Distribución por magnitud</h3>
          {stats?.by_magnitude.length ? (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={stats.by_magnitude}>
                <CartesianGrid strokeDasharray="3 3" stroke="#243044" />
                <XAxis dataKey="bucket" stroke="#8aa0bd" fontSize={11} />
                <YAxis stroke="#8aa0bd" fontSize={11} allowDecimals={false} />
                <Tooltip contentStyle={{ background: '#101a2b', border: '1px solid #2c3d58' }} />
                <Bar dataKey="events" name="Eventos" fill="#4ea1ff" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <Empty text="Sin datos en el rango seleccionado" />
          )}
        </div>

        <div className="panel">
          <h3>Regiones con más actividad</h3>
          {stats?.regions.length ? (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={stats.regions} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#243044" />
                <XAxis type="number" stroke="#8aa0bd" fontSize={11} allowDecimals={false} />
                <YAxis
                  type="category"
                  dataKey="region"
                  width={120}
                  stroke="#8aa0bd"
                  fontSize={10}
                  tickFormatter={(v) => String(v).slice(0, 18)}
                />
                <Tooltip contentStyle={{ background: '#101a2b', border: '1px solid #2c3d58' }} />
                <Bar dataKey="events" name="Eventos" fill="#26d0a5" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <Empty text="Sin datos en el rango seleccionado" />
          )}
        </div>
      </section>

      {/* ---------- Mapa + eventos recientes ---------- */}
      <section className="map-row">
        <div className="panel map-panel">
          <h3>Mapa de eventos</h3>
          {events.length ? (
            <MapContainer center={[15, 0]} zoom={2} style={{ height: 380, width: '100%' }}>
              <TileLayer
                attribution='&copy; OpenStreetMap'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              />
              {events.slice(0, 400).map((e) => (
                <Marker
                  key={e.event_uid}
                  position={[e.latitude, e.longitude]}
                  icon={icon(e.mag)}
                >
                  <Popup>
                    <b>{e.mag ?? 's/d'} Mw</b>
                    <br />
                    {e.place || e.region || 'sin ubicación'}
                    <br />
                    <small>{new Date(e.event_time).toLocaleString()}</small>
                    <br />
                    {e.tsunami && <span className="badge admin">tsunami</span>}
                  </Popup>
                </Marker>
              ))}
            </MapContainer>
          ) : (
            <Empty text="Sin eventos para mostrar" />
          )}
        </div>

        <div className="panel list-panel">
          <h3>Eventos recientes</h3>
          <div className="scroll-list">
            {events.length === 0 && <Empty text="Sin eventos" />}
            {events.slice(0, 40).map((e) => (
              <div key={e.event_uid} className={`event-row ${e.live ? 'fresh' : ''}`}>
                <span className="mag">{e.mag ?? '—'}</span>
                <div>
                  <div>{e.place || e.region || 'sin ubicación'}</div>
                  <small className="muted">
                    {new Date(e.event_time).toLocaleString()}
                    {e.tsunami && ' · tsunami'}
                  </small>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>
    </div>
  )
}

function KpiCard({
  label, value, hint, accent,
}: { label: string; value: string | number; hint?: React.ReactNode; accent?: boolean }) {
  return (
    <div className={`kpi card ${accent ? 'accent' : ''}`}>
      <div className="kpi-label">{label}</div>
      <div className="kpi-value">{value}</div>
      {hint && <div className="kpi-hint">{hint}</div>}
    </div>
  )
}

function Empty({ text }: { text: string }) {
  return <div className="empty">{text}</div>
}
