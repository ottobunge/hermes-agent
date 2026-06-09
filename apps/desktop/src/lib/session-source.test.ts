import { describe, expect, it } from 'vitest'

import { sessionSourceLabel } from './session-source'

describe('sessionSourceLabel', () => {
  it('hides default desktop/terminal session sources', () => {
    expect(sessionSourceLabel(null)).toBeNull()
    expect(sessionSourceLabel('')).toBeNull()
    expect(sessionSourceLabel('cli')).toBeNull()
    expect(sessionSourceLabel('tui')).toBeNull()
  })

  it('labels messaging and automation session sources', () => {
    expect(sessionSourceLabel('mattermost')).toBe('Mattermost')
    expect(sessionSourceLabel('cron')).toBe('Cron')
    expect(sessionSourceLabel('api_server')).toBe('API')
  })

  it('formats unknown custom sources without dropping them', () => {
    expect(sessionSourceLabel('custom_gateway')).toBe('Custom Gateway')
    expect(sessionSourceLabel('remote:worker')).toBe('Remote Worker')
  })
})
