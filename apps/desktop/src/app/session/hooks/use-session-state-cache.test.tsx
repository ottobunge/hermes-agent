import { cleanup, render, waitFor } from '@testing-library/react'
import type { ReactNode } from 'react'
import { useEffect } from 'react'
import { afterEach, describe, expect, it } from 'vitest'

import type { ChatMessage } from '@/lib/chat-messages'
import { textPart } from '@/lib/chat-messages'
import { $messages, setAwaitingResponse, setBusy, setMessages } from '@/store/session'

import type { ClientSessionState } from '../../types'
import { useSessionStateCache } from './use-session-state-cache'

interface HarnessHandle {
  updateSessionState: ReturnType<typeof useSessionStateCache>['updateSessionState']
}

function targetState(messages: ChatMessage[]): ClientSessionState {
  return {
    awaitingResponse: false,
    branch: '',
    busy: false,
    cwd: '',
    interrupted: false,
    messages,
    needsInput: false,
    pendingBranchGroup: null,
    sawAssistantPayload: false,
    storedSessionId: 'stored-target',
    streamId: null
  }
}

function Harness({
  activeSessionId,
  children,
  onReady,
  selectedStoredSessionId
}: {
  activeSessionId: string | null
  children?: ReactNode
  onReady: (handle: HarnessHandle) => void
  selectedStoredSessionId: string | null
}) {
  const cache = useSessionStateCache({
    activeSessionId,
    busyRef: { current: false },
    selectedStoredSessionId,
    setAwaitingResponse,
    setBusy,
    setMessages
  })

  useEffect(() => {
    onReady({ updateSessionState: cache.updateSessionState })
  }, [cache.updateSessionState, onReady])

  return <>{children}</>
}

describe('useSessionStateCache', () => {
  afterEach(() => {
    cleanup()
    setMessages([])
    setBusy(false)
    setAwaitingResponse(false)
  })

  it('does not preserve previous rendered chat errors when flushing a newly selected cached session', async () => {
    let handle: HarnessHandle | null = null
    const previousMessages: ChatMessage[] = [
      { id: 'previous-user', parts: [textPart('prompt that timed out')], role: 'user' },
      { error: 'agent initialization timed out', id: 'previous-error', parts: [], role: 'assistant' }
    ]
    const targetMessages: ChatMessage[] = [{ id: 'target-user', parts: [textPart('target prompt')], role: 'user' }]

    setMessages(previousMessages)

    const view = render(
      <Harness activeSessionId="rt-previous" onReady={h => (handle = h)} selectedStoredSessionId="stored-previous" />
    )

    view.rerender(
      <Harness activeSessionId="rt-target" onReady={h => (handle = h)} selectedStoredSessionId="stored-target" />
    )

    handle!.updateSessionState('rt-target', () => targetState(targetMessages), 'stored-target')

    await waitFor(() => {
      expect($messages.get().map(message => message.id)).toEqual(['target-user'])
    })
  })
})
