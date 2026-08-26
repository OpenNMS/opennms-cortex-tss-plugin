<template>
  <div class="explorer__intro">
    <p>
      Find stored series by matching on tags. Use <code>name</code> for the metric name.
      Matchers are combined with AND. Focus a field to get suggestions from your stored data.
    </p>
    <OnmsIconButton :icon="InfoIcon" iconSize="1.1rem" title="How do I use this?"
                    @click="helpOpen = !helpOpen" />
  </div>

  <HelpPanel v-if="helpOpen" class="explorer__help">
    <div class="explorer__help-title">How to explore your metrics</div>
    <ul>
      <li>Every series is stored as a set of <strong>tags</strong> (key=value pairs). Each row below is one
        condition on a tag; a series must satisfy <strong>all</strong> rows (AND) to be listed.</li>
      <li>The <strong>Node</strong> field is a shortcut for the <code>node</code> meta tag — pick a node to
        list exactly the series being written for it. Meta tags (<code>node</code>, <code>location</code>, …)
        are matchable like any other tag key.</li>
      <li><code>name</code> is the metric name (e.g. <code>CpuUser</code>). Other common keys:
        <code>node</code>, <code>location</code>, <code>resourceId</code>, plus any meta tags you configured.</li>
      <li>Type into the <em>key</em> or <em>value</em> field and pick from the suggestions — they are read live
        from your backend, so only things that actually exist are offered. Type to narrow the list.</li>
      <li>Operators: <code>=</code> exact match, <code>!=</code> exclude,
        <code>=~</code> / <code>!~</code> regular expression. A regex must match the
        <strong>whole</strong> value — use <code>.*Cpu.*</code> to search for “contains Cpu”.</li>
      <li>Click a result row's arrow to see all of its tags.</li>
    </ul>
    <OnmsButton label="Try an example: name =~ .*Cpu.*" variant="outlined" @click="tryExample" />
  </HelpPanel>

  <!-- Node quick filter: the most common question is "is node X writing data?" -->
  <div class="explorer__node-filter" @keydown.enter="applyTypedNodeFilter">
    <label for="prw-node-filter">Node</label>
    <OnmsAutoComplete
      v-model="nodeSearch"
      inputId="prw-node-filter"
      :suggestions="nodeSuggestions"
      placeholder="(any node) — type to search"
      @complete="onNodeComplete"
      @optionSelect="applyNodeFilter"
    >
      <template v-if="nodeSuggestTruncated" #footer>
        <div class="explorer__suggest-note">
          Showing {{ nodeSuggestions.length }} of {{ nodeSuggestTotal.toLocaleString() }} nodes —
          keep typing to narrow down
        </div>
      </template>
    </OnmsAutoComplete>
    <OnmsIconButton
      v-if="appliedNodeFilter"
      :icon="CloseIcon"
      severity="danger"
      title="Clear the node filter"
      @click="applyNodeFilter('')"
    />
    <span class="explorer__node-filter-note">
      Searches the <code>node</code> meta tag live from the backend and runs the query — the fastest way to
      check whether a specific node is getting data written. Combine with the matcher rows below to narrow further.
    </span>
  </div>

  <div class="explorer__matchers">
    <div v-for="m in matchers" :key="m.id" class="explorer__matcher-row">
      <div class="explorer__matcher-key">
        <OnmsAutoComplete
          v-model="m.key"
          :suggestions="keySuggestions"
          placeholder="tag key (e.g. name)"
          :ariaLabel="'Tag key'"
          fluid
          @complete="onKeyComplete"
        />
      </div>
      <div class="explorer__matcher-type">
        <OnmsSelect v-model="m.type" :options="MATCHER_TYPES" optionLabel="label" optionValue="value" fluid />
      </div>
      <div class="explorer__matcher-value">
        <OnmsAutoComplete
          v-model="m.value"
          :suggestions="valueSuggestions"
          placeholder="value"
          :ariaLabel="'Tag value'"
          fluid
          @complete="(q: string) => onValueComplete(m, q)"
        >
          <template v-if="valueSuggestTruncated" #footer>
            <div class="explorer__suggest-note">
              Showing {{ valueSuggestions.length }} of {{ valueSuggestTotal.toLocaleString() }} —
              keep typing to narrow down
            </div>
          </template>
        </OnmsAutoComplete>
      </div>
      <OnmsIconButton :icon="DeleteIcon" severity="danger" title="Remove this matcher"
                      @click="removeMatcher(m.id)" />
    </div>
  </div>

  <div class="explorer__actions">
    <OnmsButton label="Add matcher" variant="text" @click="addMatcher" />
    <OnmsButton
      :label="querying ? 'Querying…' : 'Run query'"
      :disabled="querying"
      :loading="querying"
      @click="runQuery"
    />
  </div>

  <div v-if="queryResults !== null">
    <div v-if="queryResults.length === 0" class="explorer__no-results">
      No matching series found. If you expected results, check the spelling of key and value
      (suggestions only offer what is actually stored), or switch the operator to
      <code>=~ (regex)</code> with a pattern like <code>.*Cpu.*</code> for a broader search.
    </div>
    <template v-else>
      <div class="explorer__result-count">{{ queryResults.length }} series found</div>
      <OnmsTable
        v-model:expandedRows="expandedRows"
        :value="queryResults"
        dataKey="key"
        size="small"
      >
        <OnmsColumn expander class="explorer__expander-col" />
        <OnmsColumn header="Metric">
          <template #body="{ data }">
            <span class="explorer__metric-name">{{ data.name }}</span>
          </template>
        </OnmsColumn>
        <OnmsColumn header="Key">
          <template #body="{ data }">
            <code class="explorer__metric-key">{{ data.key }}</code>
          </template>
        </OnmsColumn>
        <OnmsColumn header="Tags">
          <template #body="{ data }">{{ tagCount(data) }}</template>
        </OnmsColumn>
        <template #expansion="{ data }">
          <div class="explorer__tags">
            <div
              v-for="group in tagGroups(data)"
              :key="group.label"
              class="explorer__tag-group"
            >
              <template v-if="Object.keys(group.tags).length">
                <div class="explorer__tag-group-label">{{ group.label }}</div>
                <div class="explorer__tag-chips">
                  <span v-for="(val, k) in group.tags" :key="k" class="explorer__tag-chip">
                    {{ k }}=<strong>{{ val }}</strong>
                  </span>
                </div>
              </template>
            </div>
          </div>
        </template>
      </OnmsTable>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import {
  OnmsAutoComplete, OnmsButton, OnmsColumn, OnmsIconButton, OnmsSelect, OnmsTable, useOnmsToast,
} from '@opennms/onms-ui'
import { apiFetch, MetricDto, TagMatcher, ValueSuggest } from '../api'
import HelpPanel from './HelpPanel.vue'
import CloseIcon from './icons/Close.vue'
import DeleteIcon from './icons/Delete.vue'
import InfoIcon from './icons/Info.vue'

// `active` tracks tab activation: the node quick filter's first suggestion page
// is warmed when the tab is shown.
const props = defineProps<{
  active: boolean
}>()

const { showToast } = useOnmsToast()

const helpOpen = ref(false)

const MATCHER_TYPES = [
  { value: 'EQUALS', label: '= (equals)' },
  { value: 'NOT_EQUALS', label: '!= (not equals)' },
  { value: 'EQUALS_REGEX', label: '=~ (regex)' },
  { value: 'NOT_EQUALS_REGEX', label: '!~ (not regex)' },
]
let matcherSeq = 1
const matchers = ref<TagMatcher[]>([{ id: matcherSeq++, key: 'name', type: 'EQUALS', value: '' }])
const querying = ref(false)
const queryResults = ref<MetricDto[] | null>(null)
const expandedRows = ref<Record<string, boolean>>({})

function addMatcher() { matchers.value.push({ id: matcherSeq++, key: '', type: 'EQUALS', value: '' }) }
function removeMatcher(id: number) {
  matchers.value = matchers.value.filter(m => m.id !== id)
  if (matchers.value.length === 0) addMatcher()
}
function tagCount(m: MetricDto): number {
  return Object.keys(m.intrinsicTags).length + Object.keys(m.metaTags).length + Object.keys(m.externalTags).length
}
function tagGroups(m: MetricDto) {
  return [
    { label: 'Intrinsic tags', tags: m.intrinsicTags },
    { label: 'Meta tags', tags: m.metaTags },
    { label: 'External tags', tags: m.externalTags },
  ]
}

async function runQuery() {
  const valid = matchers.value.filter(m => m.key.trim())
  if (valid.length === 0) {
    showToast({ message: 'Add at least one tag matcher with a key.', severity: 'error' })
    return
  }
  querying.value = true
  queryResults.value = null
  expandedRows.value = {}
  try {
    queryResults.value = await apiFetch<MetricDto[]>('/query', {
      method: 'POST',
      body: JSON.stringify(valid.map(m => ({ key: m.key.trim(), type: m.type, value: m.value }))),
    })
  } catch (e: unknown) {
    showToast({ message: 'Query failed: ' + (e as Error).message, severity: 'error' })
  } finally { querying.value = false }
}

// One-click starter query for the "I don't know what to type" moment.
function tryExample() {
  matchers.value = [{ id: matcherSeq++, key: 'name', type: 'EQUALS_REGEX', value: '.*Cpu.*' }]
  runQuery()
}

// ── Node quick filter ─────────────────────────────────────────────────────────
// One searchable typeahead to answer "is node X getting data written?" without
// knowing the tag grammar. Suggestions are filtered SERVER-side (contains +
// limit), so it works with tens of thousands of nodes. Picking a node
// sets/replaces the `node =` matcher (node is the meta tag written by the
// integration layer) and runs the query; editing the matcher rows updates the box.

const SUGGEST_LIMIT = 50

const appliedNodeFilter = computed<string>(
  () => matchers.value.find(m => m.key.trim() === 'node' && m.type === 'EQUALS')?.value ?? '')
const nodeSearch = ref('')
watch(appliedNodeFilter, (v) => { nodeSearch.value = v })

const nodeSuggest = ref<ValueSuggest | null>(null)
const nodeSuggestions = computed<string[]>(() => nodeSuggest.value?.values ?? [])
const nodeSuggestTotal = computed<number>(() => nodeSuggest.value?.totalMatching ?? 0)
const nodeSuggestTruncated = computed(
  () => nodeSuggestTotal.value > nodeSuggestions.value.length)

async function fetchNodeSuggestions(query: string) {
  try {
    nodeSuggest.value = await apiFetch<ValueSuggest>(
      `/suggest/values?key=node&contains=${encodeURIComponent(query.trim())}&limit=${SUGGEST_LIMIT}`)
  } catch { nodeSuggest.value = null }
}

function onNodeComplete(query: string) { fetchNodeSuggestions(query) }

function applyNodeFilter(v: unknown) {
  const value = typeof v === 'string' ? v : ''
  matchers.value = matchers.value.filter(m => m.key.trim() !== 'node')
  if (value) matchers.value.push({ id: matcherSeq++, key: 'node', type: 'EQUALS', value })
  if (matchers.value.length === 0) addMatcher()
  nodeSearch.value = value
  if (matchers.value.some(m => m.key.trim())) runQuery()
}

// Enter with hand-typed text (no suggestion picked) applies it as an exact match.
function applyTypedNodeFilter() {
  const typed = nodeSearch.value.trim()
  if (typed === appliedNodeFilter.value) return // selection already applied it
  applyNodeFilter(typed)
}

// ── Matcher autocomplete ──────────────────────────────────────────────────────
// Suggestions come from the backend's own label index (via /suggest/*), so they
// always reflect what is actually stored. Keys are few and fetched once; VALUES
// are searched server-side (contains + limit) so keys with huge cardinality
// (e.g. `node` with 50k values) never ship their full list to the browser.
// Fetch failures are silent — typing must keep working without suggestions.

const tagKeys = ref<string[] | null>(null)
const keySuggestions = ref<string[]>([])

async function onKeyComplete(query: string) {
  if (tagKeys.value === null) {
    try { tagKeys.value = await apiFetch<string[]>('/suggest/keys') }
    catch { tagKeys.value = [] } // silent — no suggestions, typing still works
  }
  const q = query.trim().toLowerCase()
  keySuggestions.value = (q
    ? tagKeys.value.filter(s => s.toLowerCase().includes(q))
    : [...tagKeys.value]).slice(0, SUGGEST_LIMIT)
}

const valueSuggest = ref<ValueSuggest | null>(null)
const valueSuggestions = computed<string[]>(() => valueSuggest.value?.values ?? [])
const valueSuggestTotal = computed<number>(() => valueSuggest.value?.totalMatching ?? 0)
const valueSuggestTruncated = computed(
  () => valueSuggestTotal.value > valueSuggestions.value.length)

async function onValueComplete(m: TagMatcher, query: string) {
  if (!m.key.trim()) { valueSuggest.value = null; return }
  try {
    valueSuggest.value = await apiFetch<ValueSuggest>(
      `/suggest/values?key=${encodeURIComponent(m.key.trim())}&contains=${encodeURIComponent(query.trim())}&limit=${SUGGEST_LIMIT}`)
  } catch { valueSuggest.value = null }
}

// Warm the node quick filter's first page when the tab is activated.
watch(() => props.active, (active) => { if (active) fetchNodeSuggestions('') }, { immediate: true })
</script>

<style scoped>
.explorer__intro {
  display: flex;
  align-items: flex-start;
  gap: 0.25rem;
  margin-bottom: 1rem;
}
.explorer__intro p {
  font-size: 0.875rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
  margin: 0;
}
.explorer__help {
  margin-bottom: 1rem;
}
.explorer__help-title {
  font-weight: 600;
  margin-bottom: 0.35rem;
}
.explorer__node-filter {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  margin-bottom: 1rem;
  padding: 0.6rem 0.8rem;
  border: 1px solid var(--p-primary-200, rgba(39, 49, 128, 0.2));
  background: var(--p-highlight-background, rgba(39, 49, 128, 0.03));
  border-radius: 6px;
}
.explorer__node-filter > label {
  font-size: 0.8125rem;
  font-weight: 600;
  white-space: nowrap;
}
.explorer__node-filter-note {
  font-size: 0.75rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.55));
}
.explorer__suggest-note {
  padding: 0.4rem 0.75rem;
  font-size: 0.72rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.5));
}
.explorer__matchers {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  margin-bottom: 1rem;
}
.explorer__matcher-row {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}
.explorer__matcher-key {
  flex: 0 0 30%;
}
.explorer__matcher-type {
  flex: 0 0 170px;
}
.explorer__matcher-value {
  flex: 1;
}
.explorer__actions {
  display: flex;
  gap: 0.75rem;
  align-items: center;
  margin-bottom: 1.5rem;
}
.explorer__no-results {
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
  font-size: 0.875rem;
}
.explorer__result-count {
  font-size: 0.8125rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
  margin-bottom: 0.5rem;
}
.explorer__metric-name {
  font-weight: 600;
}
.explorer__metric-key {
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
  font-size: 0.8125rem;
  word-break: break-all;
}
.explorer__tags {
  padding: 0.25rem 0.6rem 0.5rem;
}
.explorer__tag-group {
  margin-bottom: 0.5rem;
}
.explorer__tag-group-label {
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.5));
  margin-bottom: 0.25rem;
}
.explorer__tag-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 0.35rem;
}
.explorer__tag-chip {
  background: var(--p-highlight-background, rgba(39, 49, 128, 0.08));
  border-radius: 3px;
  padding: 0.15rem 0.45rem;
  font-family: monospace;
  font-size: 0.75rem;
}
</style>
