import { cleanup, render, waitFor } from '@testing-library/react'
import type { MutableRefObject } from 'react'
import { useEffect } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { ChatMessage } from '@/lib/chat-messages'
import { textPart } from '@/lib/chat-messages'
import { $messages, setMessages, setSessions } from '@/store/session'
import type { SessionInfo } from '@/types/hermes'

import type { ClientSessionState } from '../../types'
import { useSessionActions } from './use-session-actions'

const mocks = vi.hoisted(() => ({
  getSessionMessages: vi.fn()
}))

vi.mock('@/hermes', () => ({
  deleteSession: vi.fn(),
  getSessionMessages: (...args: unknown[]) => mocks.getSessionMessages(...args),
  setApiRequestProfile: vi.fn(),
  setSessionArchived: vi.fn()
}))

vi.mock('@/store/profile', async () => {
  const actual = await vi.importActual<typeof import('@/store/profile')>('@/store/profile')

  return {
    ...actual,
    ensureGatewayProfile: vi.fn(async () => undefined)
  }
})

interface HarnessHandle {
  resumeSession: (storedSessionId: string) => Promise<void>
}

const state = (messages: ChatMessage[] = []): ClientSessionState => ({
  awaitingResponse: false,
  branch: '',
  busy: false,
  cwd: '',
  interrupted: false,
  messages,
  needsInput: false,
  pendingBranchGroup: null,
  sawAssistantPayload: false,
  storedSessionId: null,
  streamId: null
})

function sessionInfo(id: string): SessionInfo {
  return {
    ended_at: null,
    id,
    input_tokens: 0,
    is_active: true,
    last_active: 0,
    message_count: 1,
    model: null,
    output_tokens: 0,
    preview: null,
    source: null,
    started_at: 0,
    title: id,
    tool_call_count: 0
  }
}

function Harness({ onReady }: { onReady: (handle: HarnessHandle) => void }) {
  const activeSessionIdRef: MutableRefObject<string | null> = { current: 'rt-previous' }
  const selectedStoredSessionIdRef: MutableRefObject<string | null> = { current: 'stored-previous' }
  const sessionStateByRuntimeIdRef: MutableRefObject<Map<string, ClientSessionState>> = { current: new Map() }
  const runtimeIdByStoredSessionIdRef: MutableRefObject<Map<string, string>> = { current: new Map() }
  const busyRef = { current: false }
  const creatingSessionRef = { current: false }

  const syncSessionStateToView = (sessionId: string, next: ClientSessionState) => {
    if (sessionId === activeSessionIdRef.current) {
      setMessages(next.messages)
    }
  }

  const ensureSessionState = (sessionId: string, storedSessionId?: string | null) => {
    const existing = sessionStateByRuntimeIdRef.current.get(sessionId)

    if (existing) {
      if (storedSessionId !== undefined) {
        existing.storedSessionId = storedSessionId
      }

      return existing
    }

    const created = state()
    created.storedSessionId = storedSessionId ?? null
    sessionStateByRuntimeIdRef.current.set(sessionId, created)

    return created
  }

  const updateSessionState = (
    sessionId: string,
    updater: (current: ClientSessionState) => ClientSessionState,
    storedSessionId?: string | null
  ) => {
    const current = ensureSessionState(sessionId, storedSessionId)
    const next = updater({ ...current, messages: current.messages })
    sessionStateByRuntimeIdRef.current.set(sessionId, next)
    syncSessionStateToView(sessionId, next)

    return next
  }

  const actions = useSessionActions({
    activeSessionId: 'rt-previous',
    activeSessionIdRef,
    busyRef,
    creatingSessionRef,
    ensureSessionState,
    getRouteToken: () => 'route-token',
    navigate: vi.fn(),
    requestGateway: vi.fn(async (method: string) => {
      if (method === 'session.resume') {
        return {
          info: {},
          messages: [{ content: 'target prompt', role: 'user', timestamp: 1 }],
          running: false,
          session_id: 'rt-target',
          status: 'idle'
        }
      }

      return {}
    }) as never,
    runtimeIdByStoredSessionIdRef,
    selectedStoredSessionId: 'stored-previous',
    selectedStoredSessionIdRef,
    sessionStateByRuntimeIdRef,
    syncSessionStateToView,
    updateSessionState
  })

  useEffect(() => {
    onReady({ resumeSession: actions.resumeSession })
  }, [actions.resumeSession, onReady])

  return null
}

describe('useSessionActions resumeSession', () => {
  beforeEach(() => {
    mocks.getSessionMessages.mockResolvedValue({
      messages: [{ content: 'target prompt', role: 'user', timestamp: 1 }]
    })
    setSessions([sessionInfo('stored-target')])
    setMessages([
      { id: 'previous-user', parts: [textPart('prompt that timed out')], role: 'user' },
      { error: 'agent initialization timed out', id: 'previous-error', parts: [], role: 'assistant' }
    ])
  })

  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
    setMessages([])
    setSessions([])
  })

  it('does not preserve the previous chat local error when switching sessions', async () => {
    let handle: HarnessHandle | null = null

    render(<Harness onReady={h => (handle = h)} />)

    await handle!.resumeSession('stored-target')

    await waitFor(() => {
      expect($messages.get().map(message => message.id)).toEqual(['1-0-user'])
    })
    expect($messages.get()[0]?.parts).toEqual([textPart('target prompt')])
  })
})
