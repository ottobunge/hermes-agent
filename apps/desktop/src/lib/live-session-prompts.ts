import { setClarifyRequest } from '@/store/clarify'
import { setApprovalRequest, setSecretRequest, setSudoRequest } from '@/store/prompts'
import type { PendingSessionPrompt } from '@/types/hermes'

const stringValue = (value: unknown): string => (typeof value === 'string' ? value : '')

const stringList = (value: unknown): string[] | null => {
  if (!Array.isArray(value)) {
    return null
  }

  const values = value.filter((item): item is string => typeof item === 'string')

  return values.length > 0 ? values : null
}

export function applyPendingSessionPrompt(sessionId: string | null | undefined, pending: PendingSessionPrompt | undefined) {
  const event = pending?.event
  const payload = pending?.payload

  if (!event || !payload) {
    return false
  }

  if (event === 'clarify.request') {
    const requestId = stringValue(payload.request_id)
    const question = stringValue(payload.question)

    if (!requestId || !question) {
      return true
    }

    setClarifyRequest({
      choices: stringList(payload.choices),
      question,
      requestId,
      sessionId: sessionId ?? null
    })

    return true
  }

  if (event === 'sudo.request') {
    const requestId = stringValue(payload.request_id)

    if (requestId) {
      setSudoRequest({ requestId, sessionId: sessionId ?? null })
    }

    return true
  }

  if (event === 'secret.request') {
    const requestId = stringValue(payload.request_id)

    if (requestId) {
      setSecretRequest({
        envVar: stringValue(payload.env_var),
        prompt: stringValue(payload.prompt),
        requestId,
        sessionId: sessionId ?? null
      })
    }

    return true
  }

  if (event === 'approval.request') {
    setApprovalRequest({
      command: stringValue(payload.command),
      description: stringValue(payload.description) || 'dangerous command',
      sessionId: sessionId ?? null
    })

    return true
  }

  return false
}
