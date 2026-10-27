import { FormEvent, useState } from 'react'
import { api, setSession } from '../api'

export default function Login({ onSuccess }: { onSuccess: (role: string) => void }) {
  const [email, setEmail] = useState('admin@sismowatch.local')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function submit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    setLoading(true)
    try {
      const res = await api.login(email, password)
      setSession(res.access_token, res.role, res.username)
      onSuccess(res.role)
    } catch (err: any) {
      setError(err.message || 'Error de autenticación')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="center-screen">
      <form className="card login-card" onSubmit={submit}>
        <h1>
          <span className="brand-dot" /> SismoWatch
        </h1>
        <p className="muted">Ingresa para ver el dashboard en tiempo real</p>

        <label>Email</label>
        <input
          type="email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          required
          autoFocus
        />
        <label>Contraseña</label>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
        />

        {error && <div className="alert error">{error}</div>}

        <button className="primary" disabled={loading}>
          {loading ? 'Ingresando…' : 'Ingresar'}
        </button>

        <p className="muted small">
          Usuarios sembrados desde variables de entorno (.env):
          <br />
          admin@sismowatch.local · viewer@sismowatch.local
        </p>
      </form>
    </div>
  )
}
