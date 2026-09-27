import { getCredentials } from './auth'
import { API_BASE_URL } from '../config'

/**
 * Fetch public runtime configuration from the server.
 * When the server wasn't started witha public NATS URL, falls back to default.
 */
export async function getConfig(): Promise<{ natsUrl: string }> {
  const response = await fetch(`${API_BASE_URL}/api/config`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to fetch config' }))
    throw new Error(error.error || 'Failed to fetch config')
  }

  return response.json()
}

/**
 * Fetch all configured check names from the server (NATS KV `check.*` keys).
 * GET /api/checks — public; sends Authorization when credentials exist.
 */
export async function getChecks(): Promise<string[]> {
  const creds = getCredentials()
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  }
  if (creds) {
    headers.Authorization = `Bearer ${creds.jwt}`
  }

  const response = await fetch(`${API_BASE_URL}/api/checks`, {
    method: 'GET',
    headers,
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to fetch checks' }))
    throw new Error(error.error || 'Failed to fetch checks')
  }

  return response.json()
}

/**
 * Fetch mutable fields for all checks from the server
 * Returns a map of check names to their mutable field arrays
 */
export async function getMutableFields(): Promise<Record<string, string[]>> {
  const creds = getCredentials()
  if (!creds) {
    throw new Error('Not authenticated')
  }

  const response = await fetch(`${API_BASE_URL}/api/checks/mutable-fields`, {
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${creds.jwt}`,
      'Content-Type': 'application/json',
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to fetch mutable fields' }))
    throw new Error(error.error || 'Failed to fetch mutable fields')
  }

  return response.json()
}

/**
 * Fetch team settings via the server API
 * The team number is determined from the user's JWT role on the server
 */
export async function getTeamSettings(): Promise<Record<string, Record<string, string>>> {
  const creds = getCredentials()
  if (!creds) {
    throw new Error('Not authenticated')
  }

  const response = await fetch(`${API_BASE_URL}/api/settings`, {
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${creds.jwt}`,
      'Content-Type': 'application/json',
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to fetch settings' }))
    throw new Error(error.error || 'Failed to fetch settings')
  }

  return response.json()
}

/**
 * Update team settings via the server API
 * The team number is determined from the user's JWT role on the server
 * @param settings - Map of check names to field values
 */
export async function updateTeamSettings(settings: Record<string, Record<string, string>>): Promise<{ success: boolean; team: string }> {
  const creds = getCredentials()
  if (!creds) {
    throw new Error('Not authenticated')
  }

  const response = await fetch(`${API_BASE_URL}/api/settings`, {
    method: 'PUT',
    headers: {
      'Authorization': `Bearer ${creds.jwt}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(settings),
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to update settings' }))
    throw new Error(error.error || 'Failed to update settings')
  }

  return response.json()
}

/**
 * Start scoring (admin only)
 */
export async function startScoring(): Promise<{ success: boolean }> {
  const creds = getCredentials()
  if (!creds) {
    throw new Error('Not authenticated')
  }

  const response = await fetch(`${API_BASE_URL}/api/admin/cron/start`, {
    method: 'PUT',
    headers: {
      'Authorization': `Bearer ${creds.jwt}`,
      'Content-Type': 'application/json',
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to start scoring' }))
    throw new Error(error.error || 'Failed to start scoring')
  }

  return response.json()
}

/**
 * Stop scoring (admin only)
 */
export async function stopScoring(): Promise<{ success: boolean }> {
  const creds = getCredentials()
  if (!creds) {
    throw new Error('Not authenticated')
  }

  const response = await fetch(`${API_BASE_URL}/api/admin/cron/stop`, {
    method: 'PUT',
    headers: {
      'Authorization': `Bearer ${creds.jwt}`,
      'Content-Type': 'application/json',
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to stop scoring' }))
    throw new Error(error.error || 'Failed to stop scoring')
  }

  return response.json()
}

/**
 * Get global settings (admin only)
 */
export async function getGlobalSettings(): Promise<any> {
  const creds = getCredentials()
  if (!creds) {
    throw new Error('Not authenticated')
  }

  const response = await fetch(`${API_BASE_URL}/api/admin/settings`, {
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${creds.jwt}`,
      'Content-Type': 'application/json',
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Failed to fetch global settings' }))
    throw new Error(error.error || 'Failed to fetch global settings')
  }

  return response.json()
}
