import { FormEvent, useEffect, useRef, useState } from 'react'
import { api } from '../api'

interface Message {
  role: 'user' | 'assistant'
  text: string
  sql?: string | null
  status?: string
}

const SUGGESTIONS = [
  '¿Cuántos eventos hubo en las últimas 24 horas?',
  '¿Cuál es la magnitud máxima registrada?',
  '¿Qué región tuvo más actividad?',
  '¿Cuántos eventos con alerta de tsunami hay?',
]

export default function Chatbot() {
  const [messages, setMessages] = useState<Message[]>([
    {
      role: 'assistant',
      text: 'Hola 👋 Soy el asistente de SismoWatch. Pregúntame sobre tus datos sísmicos (solo consulto la capa curated).',
    },
  ])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [sessionId, setSessionId] = useState<string | undefined>()
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  async function ask(question: string) {
    if (!question.trim() || loading) return
    setMessages((m) => [...m, { role: 'user', text: question }])
    setInput('')
    setLoading(true)
    try {
      const res = await api.chat(question, sessionId)
      setSessionId(res.session_id)
      setMessages((m) => [
        ...m,
        {
          role: 'assistant',
          text: res.answer,
          sql: res.sql,
          status: res.status,
        },
      ])
    } catch (e: any) {
      setMessages((m) => [
        ...m,
        {
          role: 'assistant',
          text: `⚠ ${e.message || 'Error consultando al asistente'}. Tus datos siguen disponibles en el dashboard.`,
          status: 'error',
        },
      ])
    } finally {
      setLoading(false)
    }
  }

  function submit(e: FormEvent) {
    e.preventDefault()
    ask(input)
  }

  return (
    <div className="chat-layout">
      <section className="panel chat-panel">
        <h2>Asistente IA <span className="muted small">(lenguaje natural → SQL de solo lectura)</span></h2>

        <div className="chat-messages">
          {messages.map((m, i) => (
            <div key={i} className={`msg ${m.role}`}>
              <div className="bubble">
                {m.text}
                {m.sql && (
                  <details className="sql">
                    <summary>ver consulta SQL</summary>
                    <code>{m.sql}</code>
                  </details>
                )}
              </div>
            </div>
          ))}
          {loading && (
            <div className="msg assistant">
              <div className="bubble typing">Consultando datos…</div>
            </div>
          )}
          <div ref={bottomRef} />
        </div>

        <form className="chat-input" onSubmit={submit}>
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Pregunta sobre tus datos…"
            maxLength={1000}
            disabled={loading}
          />
          <button className="primary" disabled={loading || !input.trim()}>
            {loading ? '…' : 'Enviar'}
          </button>
        </form>
      </section>

      <aside className="panel side-panel">
        <h3>Sugerencias</h3>
        {SUGGESTIONS.map((s) => (
          <button key={s} className="suggestion" onClick={() => ask(s)} disabled={loading}>
            {s}
          </button>
        ))}
        <div className="guard-note">
          <b>Controles activos</b>
          <ul>
            <li>El LLM solo genera SELECT sobre <code>curated</code>.</li>
            <li>Guardia SQL: allowlist de tablas/columnas, LIMIT, sin sentencias múltiples.</li>
            <li>Ejecución con rol de base de datos de solo lectura.</li>
            <li>Las cifras salen de la BD, no del modelo.</li>
          </ul>
        </div>
      </aside>
    </div>
  )
}
