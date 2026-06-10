/**
 * Focused utilities for remote-backend reconnect behavior used by
 * electron/main.cjs: bounded remote startup probing and recovery-state
 * accounting for transient /api/status misses.
 */

const DEFAULT_REMOTE_PROBE_TIMEOUT_MS = 90_000
const DEFAULT_REMOTE_PROBE_INITIAL_DELAY_MS = 500
const DEFAULT_REMOTE_PROBE_MAX_DELAY_MS = 2_000
const DEFAULT_REMOTE_PROBE_MAX_ATTEMPTS = 16

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function toPositiveInteger(value, fallback) {
  return Number.isFinite(value) && value > 0 ? Math.trunc(value) : fallback
}

function backoffDelay(attempt, initialDelayMs, maxDelayMs) {
  const capped = Math.pow(2, Math.max(0, attempt - 1))
  return Math.min(initialDelayMs * capped, maxDelayMs)
}

async function waitForRemoteHermes(baseUrl, token, options = {}) {
  const fetcher = options.fetcher
  if (typeof fetcher !== 'function') {
    throw new Error('waitForRemoteHermes requires a fetcher function')
  }

  const timeoutMs = toPositiveInteger(options.timeoutMs, DEFAULT_REMOTE_PROBE_TIMEOUT_MS)
  const maxAttempts = toPositiveInteger(options.maxAttempts, DEFAULT_REMOTE_PROBE_MAX_ATTEMPTS)
  const initialDelayMs = toPositiveInteger(options.initialDelayMs, DEFAULT_REMOTE_PROBE_INITIAL_DELAY_MS)
  const maxDelayMs = toPositiveInteger(options.maxDelayMs, DEFAULT_REMOTE_PROBE_MAX_DELAY_MS)
  const deadline = Date.now() + timeoutMs
  const wait = options.sleep || sleep
  let attempts = 0
  let lastError = null

  while (Date.now() < deadline) {
    try {
      await fetcher(`${baseUrl}/api/status`, token, options.fetcherOptions)
      return
    } catch (error) {
      lastError = error
      attempts += 1

      if (attempts >= maxAttempts) {
        break
      }

      const delay = backoffDelay(attempts, initialDelayMs, maxDelayMs)
      const remainingMs = deadline - Date.now()
      if (remainingMs <= delay) {
        break
      }
      await wait(delay)
    }
  }

  const message = lastError?.message || 'timeout'
  throw new Error(`Remote Hermes backend did not become ready after ${attempts} attempts: ${message}`)
}

module.exports = {
  DEFAULT_REMOTE_PROBE_INITIAL_DELAY_MS,
  DEFAULT_REMOTE_PROBE_MAX_ATTEMPTS,
  DEFAULT_REMOTE_PROBE_MAX_DELAY_MS,
  DEFAULT_REMOTE_PROBE_TIMEOUT_MS,
  waitForRemoteHermes
}
