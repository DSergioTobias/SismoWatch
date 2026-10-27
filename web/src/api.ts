// Cliente HTTP de la plataforma: JWT, manejo de errores y tipos.
const API_URL = (import.meta as any).env?.VITE_API_URL || 'http://localhost:8010'

export interface Kpis {
  events_24h: number
  max_mag: number
  avg_mag: number
  tsunami_events: number
  quality_score: number | null
  quality_status: string | null
  last_event_time: string | null
  last_update: string | null
  generated_at: string
}

export interface EventItem {
  event_uid: string
  mag: number | null
  place: string | null
  region: string | null
  event_time: string
  tsunami: boolean
  status: string | null
  mag_type: string | null
  url: string | null
  longitude: number
  latitude: number
  depth_km: number | null
}

export interface ReportItem {
  id: number
  report_date: string
  title: string
  content: { summary?: string; zones?: string[]; highlights?: string[]; quality_note?: string }
  kpis: Record<string, unknown>
  model: string | null
  status: string
  created_at: string
}

export interface PipelineStatus {
  dags: { dag_id: string; last_state: string | null; last_run_id: string | null; last_execution: string | null }[]
  quality: { dag_run_id: string; score: number; status: string; records_total: number; records_valid: number; records_rejected: number; evaluated_at: string }[]
  freshness: { last_ingest: string | null; fresh: boolean | null }
  quarantine_count: number | null
  generated_at: string
}

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

function getToken(): string | null {
  return localStorage.getItem('sw_token')
}

export function setSession(token: string, role: string, username: string) {
  localStorage.setItem('sw_token', token)
  localStorage.setItem('sw_role', role)
  localStorage.setItem('sw_user', username)
}

export function clearSession() {
  localStorage.removeItem('sw_token')
  localStorage.removeItem('sw_role')
  localStorage.removeItem('sw_user')
}

export function getRole(): string | null {
  return localStorage.getItem('sw_role')
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string>),
  }
  const token = getToken()
  if (token) headers['Authorization'] = `Bearer ${token}`

  let res: Response
  try {
    res = await fetch(`${API_URL}${path}`, { ...options, headers })
  } catch {
    throw new ApiError(0, 'No se pudo conectar con el servidor')
  }

  if (res.status === 401) {
    clearSession()
    throw new ApiError(401, 'Sesión expirada o no autenticada')
  }
  const text = await res.text()
  let data: any = null
  try {
    data = text ? JSON.parse(text) : null
  } catch {
    data = null
  }
  if (!res.ok) {
    const detail = data?.detail
    const msg =
      typeof detail === 'string'
        ? detail
        : detail?.message || `Error ${res.status}`
    throw new ApiError(res.status, msg)
  }
  return data as T
}

export const api = {
  login: (email: string, password: string) =>
    request<{ access_token: string; role: string; username: string }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    }),
  kpis: () => request<Kpis>('/api/kpis'),
  events: (params: URLSearchParams) => request<{ items: EventItem[]; total: number }>(`/api/events?${params}`),
  timeseries: (params: URLSearchParams) =>
    request<{ hourly: any[]; by_magnitude: any[]; regions: any[] }>(`/api/stats/timeseries?${params}`),
  mapEvents: (params: URLSearchParams) => request<{ items: EventItem[] }>(`/api/stats/map?${params}`),
  reports: (params: URLSearchParams) => request<{ items: ReportItem[]; total: number }>(`/api/reports?${params}`),
  pipeline: () => request<PipelineStatus>('/api/pipeline/status'),
  quarantine: (params: URLSearchParams) => request<{ items: any[]; total: number }>(`/api/quality/quarantine?${params}`),
  chat: (question: string, sessionId?: string) =>
    request<{ session_id: string; answer: string; sql: string | null; status: string; rows: any[] | null }>('/api/chat', {
      method: 'POST',
      body: JSON.stringify({ question, session_id: sessionId }),
    }),
  backfill: (days: number) =>
    request<any>('/api/admin/backfill', { method: 'POST', body: JSON.stringify({ days }) }),
  regenerate: () =>
    request<any>('/api/admin/reports/regenerate', { method: 'POST', body: JSON.stringify({ days: 1 }) }),
}

export function streamUrl(): string {
  return `${API_URL}/api/stream`
}

export function authHeaders(): Record<string, string> {
  const token = getToken()
  return token ? { Authorization: `Bearer ${token}` } : {}
}

export { API_URL }
