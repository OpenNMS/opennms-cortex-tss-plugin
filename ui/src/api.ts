// REST API client + DTOs for /opennms/rest/prometheus-remotewrite.

export interface ConfigDto {
  writeUrl: string
  readUrl: string
  maxConcurrentHttpConnections: number
  writeTimeoutInMs: number
  readTimeoutInMs: number
  metricCacheSize: number
  externalTagsCacheSize: number
  bulkheadMaxWaitDuration: number
  bulkheadUnlimited: boolean
  maxSeriesLookback: number
  organizationId: string
}

export interface TestConnectionResult {
  endpoint: string
  url: string
  reachable: boolean
  statusCode: number
  durationMs: number
  detail: string
}

export interface GaugeStat { name: string; value: unknown }

export interface MeterStat {
  name: string
  count: number
  meanRate: number
  oneMinuteRate: number
  fiveMinuteRate: number
  fifteenMinuteRate: number
}

export interface Stats { gauges: GaugeStat[]; meters: MeterStat[] }

export interface NodeSeriesCount { node: string; series: number }

export interface SeriesCount {
  totalSeries: number
  nodesWritingData: number
  seriesWithoutNodeTag: number
  matchingNodes: number
  offset: number
  limit: number
  byNode: NodeSeriesCount[]
}

export interface ValueSuggest { values: string[]; totalMatching: number }

export interface TagMatcher { id: number; key: string; type: string; value: string }

export interface MetricDto {
  key: string
  name: string
  intrinsicTags: Record<string, string>
  metaTags: Record<string, string>
  externalTags: Record<string, string>
}

export interface HealthIssue { severity: string; title: string; detail: string; remedy: string }

export interface Health {
  functional: boolean
  timeseriesStrategy: string
  runningStrategy: string
  configuredStrategy: string
  strategySource: string
  integrationStrategyActive: boolean
  pendingRestart: boolean
  integrationManaged: boolean
  storageServiceActive: boolean
  opennmsHome: string
  issues: HealthIssue[]
}

const API = '/opennms/rest/prometheus-remotewrite'

export async function apiFetch<T>(path: string, options: RequestInit = {}): Promise<T> {
  const resp = await fetch(API + path, {
    headers: { 'Content-Type': 'application/json', ...options.headers as Record<string, string> },
    ...options,
  })
  if (resp.status === 204) return null as T
  const text = await resp.text()
  let body: unknown
  try { body = JSON.parse(text) } catch { body = text }
  if (!resp.ok) throw new Error(typeof body === 'string' ? body : JSON.stringify(body))
  return body as T
}
