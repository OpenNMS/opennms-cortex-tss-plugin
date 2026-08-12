<template>
  <div v-if="health && !health.storageServiceActive" class="stats__inactive">
    Live statistics are unavailable because the Prometheus storage service isn't active in this OpenNMS
    (strategy is <code>{{ health.timeseriesStrategy }}</code>). See the banner above for how to enable it.
  </div>

  <template v-else>
    <div class="stats__toolbar">
      <OnmsButton
        :label="loadingStats ? 'Refreshing…' : 'Refresh'"
        variant="outlined"
        :disabled="loadingStats"
        :loading="loadingStats"
        @click="refresh"
      />
      <div class="stats__auto-refresh">
        <OnmsToggleSwitch :modelValue="autoRefresh" inputId="prw-auto-refresh"
                          @update:modelValue="setAutoRefresh" />
        <label for="prw-auto-refresh">Auto-refresh (5s)</label>
      </div>
    </div>

    <div v-if="!stats" class="stats__loading">
      <OnmsSpinner size="1.5rem" /> Loading live statistics…
    </div>

    <template v-else>
      <!-- Stored series — how many metrics are being collected, and by which nodes -->
      <section class="stats__section">
        <h2>Stored series</h2>
        <p class="stats__blurb">
          Distinct time series currently receiving samples (within Prometheus' ~5&nbsp;min staleness window).
          A node listed here <strong>is</strong> getting data written. Use the
          Metric Explorer's node filter to drill into its series.
        </p>
        <div v-if="loadingSeriesCount && !seriesCount" class="stats__loading">
          <OnmsSpinner size="1.25rem" /> Counting series…
        </div>
        <div v-else-if="!seriesCount" class="stats__blurb">
          Series counts are unavailable (the read API did not answer the count query).
        </div>
        <template v-else>
          <div class="stats__tiles">
            <StatTile :value="seriesCount.totalSeries.toLocaleString()" label="Active series (total)" />
            <StatTile :value="seriesCount.nodesWritingData.toLocaleString()" label="Nodes writing data" />
            <StatTile :value="seriesCount.seriesWithoutNodeTag.toLocaleString()" label="Series without a node tag" />
          </div>

          <!-- Server-side search + pagination: usable with 50k nodes -->
          <div class="stats__node-table">
            <div class="stats__node-search">
              <OnmsSearchInput
                :modelValue="nodeTableSearch"
                placeholder="Search nodes…"
                ariaLabel="Search nodes"
                @update:modelValue="onNodeTableSearch"
              />
            </div>
            <OnmsTable
              :value="seriesCount.byNode"
              dataKey="node"
              size="small"
              lazy
              paginator
              :rows="NODE_PAGE_SIZE"
              :first="seriesCount.offset"
              :totalRecords="seriesCount.matchingNodes"
              @page="onNodeTablePage"
            >
              <OnmsColumn field="node" header="Node" />
              <OnmsColumn header="Active series">
                <template #body="{ data }">
                  <span class="stats__num">{{ data.series.toLocaleString() }}</span>
                </template>
              </OnmsColumn>
              <template #empty>
                <div class="stats__empty">No nodes match “{{ nodeTableSearch }}”.</div>
              </template>
            </OnmsTable>
          </div>
        </template>
      </section>

      <!-- Gauges -->
      <section class="stats__section">
        <h2>HTTP client &amp; bulkhead</h2>
        <div class="stats__tiles stats__tiles--wide">
          <StatTile v-for="g in stats.gauges" :key="g.name" :value="fmtNum(g.value)" :label="gaugeLabel(g.name)" />
        </div>
      </section>

      <!-- Meters -->
      <section class="stats__section">
        <h2>Throughput</h2>
        <OnmsTable :value="stats.meters" dataKey="name" size="small">
          <OnmsColumn header="Meter">
            <template #body="{ data }">{{ meterLabel(data.name) }}</template>
          </OnmsColumn>
          <OnmsColumn header="Total count">
            <template #body="{ data }"><span class="stats__num">{{ data.count.toLocaleString() }}</span></template>
          </OnmsColumn>
          <OnmsColumn header="Mean / s">
            <template #body="{ data }"><span class="stats__num">{{ data.meanRate.toFixed(3) }}</span></template>
          </OnmsColumn>
          <OnmsColumn header="1 min / s">
            <template #body="{ data }"><span class="stats__num">{{ data.oneMinuteRate.toFixed(3) }}</span></template>
          </OnmsColumn>
          <OnmsColumn header="5 min / s">
            <template #body="{ data }"><span class="stats__num">{{ data.fiveMinuteRate.toFixed(3) }}</span></template>
          </OnmsColumn>
          <OnmsColumn header="15 min / s">
            <template #body="{ data }"><span class="stats__num">{{ data.fifteenMinuteRate.toFixed(3) }}</span></template>
          </OnmsColumn>
        </OnmsTable>
      </section>
    </template>
  </template>
</template>

<script setup lang="ts">
import { onUnmounted, ref, watch } from 'vue'
import {
  OnmsButton, OnmsColumn, OnmsSearchInput, OnmsSpinner, OnmsTable, OnmsToggleSwitch, useOnmsToast,
} from '@opennms/onms-ui'
import type { OnmsTablePageEvent } from '@opennms/onms-ui'
import { apiFetch, SeriesCount, Stats } from '../api'
import { health } from '../store'
import StatTile from './StatTile.vue'

// `active` tracks tab activation: data loads (and reloads) when the tab is
// shown, so there is no "click Refresh to see anything" state.
const props = defineProps<{
  active: boolean
}>()

const { showToast } = useOnmsToast()

const stats = ref<Stats | null>(null)
const loadingStats = ref(false)
const autoRefresh = ref(false)
let statsTimer: ReturnType<typeof setInterval> | null = null

async function loadStats() {
  loadingStats.value = true
  try {
    stats.value = await apiFetch<Stats>('/stats')
  } catch (e: unknown) {
    showToast({ message: 'Failed to load stats: ' + (e as Error).message, severity: 'error' })
  } finally { loadingStats.value = false }
}

// Stored-series counts (total + per node) — the "is data actually being written,
// and for which nodes?" view. Search + pagination are SERVER-side, so this stays
// responsive with tens of thousands of nodes.
const NODE_PAGE_SIZE = 25
const seriesCount = ref<SeriesCount | null>(null)
const loadingSeriesCount = ref(false)
const nodeTableSearch = ref('')
const nodeTableOffset = ref(0)
let nodeTableDebounce: ReturnType<typeof setTimeout> | null = null

async function loadSeriesCount() {
  loadingSeriesCount.value = true
  try {
    const params = `contains=${encodeURIComponent(nodeTableSearch.value.trim())}`
      + `&offset=${nodeTableOffset.value}&limit=${NODE_PAGE_SIZE}`
    seriesCount.value = await apiFetch<SeriesCount>('/series-count?' + params)
  } catch {
    // non-fatal: counters still render without the series section
    seriesCount.value = null
  } finally { loadingSeriesCount.value = false }
}

function onNodeTableSearch(v: string | undefined) {
  nodeTableSearch.value = v ?? ''
  if (nodeTableDebounce) clearTimeout(nodeTableDebounce)
  nodeTableDebounce = setTimeout(() => { nodeTableOffset.value = 0; loadSeriesCount() }, 300)
}

function onNodeTablePage(e: OnmsTablePageEvent) {
  nodeTableOffset.value = e.first
  loadSeriesCount()
}

function refresh() { loadStats(); loadSeriesCount() }

function setAutoRefresh(on: boolean) {
  autoRefresh.value = on
  if (statsTimer) { clearInterval(statsTimer); statsTimer = null }
  if (on) statsTimer = setInterval(refresh, 5000)
}

function fmtNum(n: unknown): string {
  if (typeof n !== 'number') return String(n)
  if (Number.isInteger(n)) return n.toLocaleString()
  return n.toFixed(3)
}

function gaugeLabel(name: string): string {
  const map: Record<string, string> = {
    connectionCount: 'Open connections',
    idleConnectionCount: 'Idle connections',
    queuedCallsCount: 'Queued calls',
    runningCallsCount: 'Running calls',
    availableConcurrentCalls: 'Available concurrent calls',
    maxAllowedConcurrentCalls: 'Max allowed concurrent calls',
  }
  return map[name] || name
}

function meterLabel(name: string): string {
  const map: Record<string, string> = {
    samplesWritten: 'Samples written',
    samplesLost: 'Samples lost',
    extTagsModified: 'External tags modified',
    extTagsCacheUsed: 'External tags cache hits',
    extTagsCacheMissed: 'External tags cache misses',
    extTagPutTransactionFailed: 'External tag write failures',
  }
  return map[name] || name
}

watch(() => props.active, (active) => { if (active) refresh() }, { immediate: true })
onUnmounted(() => { if (statsTimer) clearInterval(statsTimer) })
</script>

<style scoped>
.stats__inactive {
  border: 1px solid var(--p-red-200, #f6aea9);
  background: var(--p-red-50, #fdf3f2);
  border-radius: 6px;
  padding: 1rem 1.25rem;
  font-size: 0.8125rem;
  color: var(--p-red-800, #7a1c17);
}
.stats__toolbar {
  display: flex;
  gap: 0.75rem;
  align-items: center;
  margin-bottom: 1.25rem;
}
.stats__auto-refresh {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  font-size: 0.8125rem;
}
.stats__loading {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
  font-size: 0.875rem;
}
.stats__section {
  margin-bottom: 1.75rem;
}
.stats__section h2 {
  font-size: 1rem;
  font-weight: 600;
  margin: 0 0 0.25rem;
}
.stats__blurb {
  margin: 0 0 0.75rem;
  font-size: 0.8125rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
}
.stats__tiles {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 0.75rem;
  margin-bottom: 0.75rem;
  max-width: 720px;
}
.stats__tiles--wide {
  max-width: none;
}
.stats__node-table {
  max-width: 640px;
}
.stats__node-search {
  margin-bottom: 0.5rem;
}
.stats__num {
  font-variant-numeric: tabular-nums;
}
.stats__empty {
  padding: 0.6rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.55));
}
</style>
