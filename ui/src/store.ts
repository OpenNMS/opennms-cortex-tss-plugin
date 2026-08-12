// Shared singleton state for the plugin UI: health/preflight gate, the config
// form (edited from both the blocked-state panel and the Settings tab), the
// backend readiness tests, and the system-wide integration enable/disable flow.
// Module-scoped reactive state instead of prop drilling — this app mounts once.

import { computed, reactive, ref } from 'vue'
import { useOnmsToast } from '@opennms/onms-ui'
import { apiFetch, ConfigDto, Health, TestConnectionResult } from './api'

const { showToast } = useOnmsToast()

// ── Health / preflight gate ───────────────────────────────────────────────────
//
// Nothing in this UI is shown until we have verified the plugin can actually do
// its job. If any precondition fails we render a clean instruction page instead
// of the configuration UI — never empty/broken tabs, never an action that can't
// succeed.

export const health = ref<Health | null>(null)
export const healthChecked = ref(false)      // have we completed at least one /health attempt?
export const healthError = ref<string | null>(null)  // set when /health itself can't be reached
export const checkingHealth = ref(false)

export async function loadHealth() {
  checkingHealth.value = true
  try {
    health.value = await apiFetch<Health>('/health')
    healthError.value = null
  } catch (e: unknown) {
    // The endpoint itself failing is the most severe diagnostic of all.
    health.value = null
    healthError.value = (e as Error).message || 'The plugin REST API did not respond.'
  } finally {
    healthChecked.value = true
    checkingHealth.value = false
  }
}

// Top-level gate for the whole UI.
export const uiState = computed<'loading' | 'unreachable' | 'blocked' | 'ready'>(() => {
  if (!healthChecked.value) return 'loading'
  if (healthError.value !== null) return 'unreachable'
  if (!health.value || !health.value.functional) return 'blocked'
  return 'ready'
})

// ── Integration strategy file / restart commands ─────────────────────────────

// Exact file + content the backend writes (shown in the confirmation for
// transparency). Uses the real opennms.home from /health so the path is
// concrete for this install.
export const opennmsHome = computed(() => health.value?.opennmsHome || '$OPENNMS_HOME')
export const managedFilePath = computed(
  () => `${opennmsHome.value}/etc/opennms.properties.d/prometheus-remotewrite.properties`)
export const MANAGED_FILE_CONTENT =
  'org.opennms.timeseries.strategy=integration\n' +
  'org.opennms.timeseries.tin.metatags.tag.node=${node:label}\n' +
  'org.opennms.timeseries.tin.metatags.tag.location=${node:location}\n' +
  'org.opennms.timeseries.tin.metatags.tag.geohash=${node:geohash}\n' +
  'org.opennms.timeseries.tin.metatags.tag.ifDescr=${interface:if-description}'
// Copy-pasteable restart commands for both common install types.
export const restartCommands = computed(() =>
  'sudo systemctl restart opennms          # packaged (systemd) install\n' +
  `${opennmsHome.value}/bin/opennms restart   # manual / tarball install`)

// ── Enable / disable the integration strategy (system-wide, restart-required) ─

export const confirmVisible = ref(false)
export const confirmMode = ref<'enable' | 'disable'>('enable')
export const applyingIntegration = ref(false)

export function openConfirm(mode: 'enable' | 'disable') {
  confirmMode.value = mode
  confirmVisible.value = true
}

export async function runConfirm() {
  applyingIntegration.value = true
  const path = confirmMode.value === 'enable' ? '/integration/enable' : '/integration/disable'
  try {
    const r = await apiFetch<{ ok: boolean; message: string }>(path, { method: 'POST' })
    showToast({
      message: r.message,
      severity: r.ok ? 'success' : 'error',
    })
    confirmVisible.value = false
    await loadHealth()
  } catch (e: unknown) {
    showToast({ message: (e as Error).message, severity: 'error' })
  } finally {
    applyingIntegration.value = false
  }
}

// ── Config form ───────────────────────────────────────────────────────────────

export const form = reactive<ConfigDto>({
  writeUrl: 'http://localhost:9009/api/prom/push',
  readUrl: 'http://localhost:9009/prometheus/api/v1',
  maxConcurrentHttpConnections: 100,
  writeTimeoutInMs: 5000,
  readTimeoutInMs: 5000,
  metricCacheSize: 1000,
  externalTagsCacheSize: 1000,
  bulkheadMaxWaitDuration: 0,
  bulkheadUnlimited: true,
  maxSeriesLookback: 7776000,
  organizationId: '',
})
export const loadingConfig = ref(false)
export const savingConfig = ref(false)
export const formErrors = reactive<Record<string, string>>({ writeUrl: '', readUrl: '' })

// Lookback shown as a friendly days field, stored as seconds
export const lookbackDays = ref(90)
export function syncLookbackFromSeconds() { lookbackDays.value = Math.round(form.maxSeriesLookback / 86400) }
export function syncLookbackToSeconds() { form.maxSeriesLookback = Math.max(0, Math.round(lookbackDays.value * 86400)) }

export async function loadConfig() {
  loadingConfig.value = true
  try {
    Object.assign(form, await apiFetch<ConfigDto>('/config'))
    savedEndpointKey.value = endpointKey.value
    syncLookbackFromSeconds()
  } catch (e: unknown) {
    showToast({ message: 'Failed to load config: ' + (e as Error).message, severity: 'error' })
  } finally { loadingConfig.value = false }
}

export function validateConfig(): boolean {
  formErrors.writeUrl = form.writeUrl.trim() ? '' : 'Write URL is required'
  formErrors.readUrl = form.readUrl.trim() ? '' : 'Read URL is required'
  return !formErrors.writeUrl && !formErrors.readUrl
}

export async function saveConfig() {
  if (!validateConfig()) return
  savingConfig.value = true
  try {
    await apiFetch('/config', { method: 'PUT', body: JSON.stringify(form) })
    savedEndpointKey.value = endpointKey.value
    showToast({ message: 'Configuration updated. Changes apply on the next config reload.' })
  } catch (e: unknown) {
    showToast({ message: (e as Error).message, severity: 'error' })
  } finally { savingConfig.value = false }
}

// ── Enable-integration gate ───────────────────────────────────────────────────
// Enabling is a system-wide switch, so it is only allowed once we have PROVEN
// the backend accepts metrics — a successful test WRITE — for the exact
// endpoints that are currently SAVED. Editing any endpoint (or not saving it)
// invalidates the proof and re-locks the button.

export const endpointKey = computed(() => `${form.writeUrl} ${form.readUrl} ${form.organizationId ?? ''}`)
export const verifiedEndpointKey = ref<string | null>(null)  // set when a test write succeeds
export const savedEndpointKey = ref<string | null>(null)     // set on load and on save
export const backendVerified = computed(
  () => verifiedEndpointKey.value !== null && verifiedEndpointKey.value === endpointKey.value)
export const endpointsSaved = computed(
  () => savedEndpointKey.value !== null && savedEndpointKey.value === endpointKey.value)
export const canEnableIntegration = computed(() => backendVerified.value && endpointsSaved.value)
export const enableBlockedReason = computed(() => {
  if (!backendVerified.value) return 'Run “Send test write” and confirm the backend accepts a sample (for the current endpoints) before enabling.'
  if (!endpointsSaved.value) return 'Save the endpoints first, so integration uses the URLs you just verified.'
  return ''
})

// ── Backend readiness tests ───────────────────────────────────────────────────

// Connection test (reachability + read-API validity; no side effects)
export const testing = ref(false)
export const testResults = ref<TestConnectionResult[] | null>(null)

export async function testConnection() {
  testing.value = true
  testResults.value = null
  writeTestResult.value = null
  try {
    testResults.value = await apiFetch<TestConnectionResult[]>('/test-connection', {
      method: 'POST',
      body: JSON.stringify({ writeUrl: form.writeUrl, readUrl: form.readUrl, organizationId: form.organizationId }),
    })
  } catch (e: unknown) {
    showToast({ message: 'Connection test failed: ' + (e as Error).message, severity: 'error' })
  } finally { testing.value = false }
}

// Test write — actually pushes ONE synthetic sample (side effect: a test metric
// lands in the TSDB)
export const testingWrite = ref(false)
export const writeTestResult = ref<TestConnectionResult | null>(null)

export async function sendTestWrite() {
  testingWrite.value = true
  writeTestResult.value = null
  try {
    writeTestResult.value = await apiFetch<TestConnectionResult>('/test-write', {
      method: 'POST',
      body: JSON.stringify({ writeUrl: form.writeUrl, readUrl: form.readUrl, organizationId: form.organizationId }),
    })
    // A successful write proves THESE endpoints accept metrics — unlocks Enable (once saved).
    if (writeTestResult.value?.reachable) verifiedEndpointKey.value = endpointKey.value
  } catch (e: unknown) {
    showToast({ message: 'Test write failed: ' + (e as Error).message, severity: 'error' })
  } finally { testingWrite.value = false }
}
