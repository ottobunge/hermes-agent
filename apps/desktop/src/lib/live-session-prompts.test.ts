import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { $clarifyRequest, clearClarifyRequest } from '@/store/clarify'
import { $secretRequest, $sudoRequest, clearAllPrompts } from '@/store/prompts'
import { $activeSessionId } from '@/store/session'

import { applyPendingSessionPrompt } from './live-session-prompts'

beforeEach(() => {
  $activeSessionId.set('runtime-1')
})

afterEach(() => {
  clearClarifyRequest()
  clearAllPrompts()
  $activeSessionId.set(null)
})

describe('applyPendingSessionPrompt', () => {
  it('hydrates a pending clarify request from a live session snapshot', () => {
    const applied = applyPendingSessionPrompt('runtime-1', {
      event: 'clarify.request',
      payload: {
        choices: ['Fix first', 'Include in stream'],
        question: 'How should we handle the prod bugs?',
        request_id: 'req-1'
      }
    })

    expect(applied).toBe(true)
    expect($clarifyRequest.get()).toEqual({
      choices: ['Fix first', 'Include in stream'],
      question: 'How should we handle the prod bugs?',
      requestId: 'req-1',
      sessionId: 'runtime-1'
    })
  })

  it('hydrates modal prompt requests with request ids', () => {
    expect(
      applyPendingSessionPrompt('runtime-1', { event: 'sudo.request', payload: { request_id: 'sudo-1' } })
    ).toBe(true)
    expect($sudoRequest.get()).toEqual({ requestId: 'sudo-1', sessionId: 'runtime-1' })

    expect(
      applyPendingSessionPrompt('runtime-1', {
        event: 'secret.request',
        payload: { env_var: 'OPENAI_API_KEY', prompt: 'Paste key', request_id: 'secret-1' }
      })
    ).toBe(true)
    expect($secretRequest.get()).toEqual({
      envVar: 'OPENAI_API_KEY',
      prompt: 'Paste key',
      requestId: 'secret-1',
      sessionId: 'runtime-1'
    })
  })
})
