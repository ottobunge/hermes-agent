const HIDDEN_SESSION_SOURCES = new Set(['', 'cli', 'tui'])

const SESSION_SOURCE_LABELS: Record<string, string> = {
  api_server: 'API',
  cron: 'Cron',
  discord: 'Discord',
  mattermost: 'Mattermost',
  slack: 'Slack',
  telegram: 'Telegram',
  whatsapp: 'WhatsApp'
}

export function sessionSourceLabel(source: null | string | undefined): null | string {
  const normalized = (source ?? '').trim().toLowerCase()

  if (HIDDEN_SESSION_SOURCES.has(normalized)) {
    return null
  }

  return (
    SESSION_SOURCE_LABELS[normalized] ??
    normalized
      .split(/[_:-]+/)
      .filter(Boolean)
      .map(part => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ')
  )
}
