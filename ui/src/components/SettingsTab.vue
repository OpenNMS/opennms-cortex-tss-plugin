<template>
  <div v-if="loadingConfig" class="settings__loading">
    <OnmsSpinner size="1.5rem" /> Loading configuration…
  </div>

  <template v-else>
    <!-- Connection -->
    <section class="settings__section">
      <h2>Connection</h2>

      <EndpointFields />

      <div class="settings__test-actions">
        <OnmsButton
          :label="testing ? 'Testing…' : 'Test connection'"
          variant="outlined"
          :disabled="testing || testingWrite"
          :loading="testing"
          @click="testConnection"
        />
        <OnmsButton
          :label="testingWrite ? 'Writing…' : 'Send test write'"
          variant="outlined"
          :disabled="testing || testingWrite"
          :loading="testingWrite"
          title="Pushes one synthetic sample to confirm the backend accepts writes"
          @click="sendTestWrite"
        />
        <span class="settings__test-note">
          “Test connection” only probes reachability &amp; the read API. “Send test write” actually pushes one
          sample (<code>opennms_remotewrite_healthcheck</code>) into the backend.
        </span>
      </div>

      <TestResultList :results="testResults" :writeResult="writeTestResult" />
    </section>

    <!-- HTTP / Performance -->
    <section class="settings__section">
      <h2>HTTP &amp; Performance</h2>
      <div class="settings__grid">
        <FormField label="Max concurrent HTTP connections" for="prw-max-conn">
          <OnmsInputNumber v-model="form.maxConcurrentHttpConnections" inputId="prw-max-conn" :min="1" fluid />
        </FormField>
        <div></div>
        <FormField label="Write timeout (ms)" for="prw-write-timeout">
          <OnmsInputNumber v-model="form.writeTimeoutInMs" inputId="prw-write-timeout" :min="0" fluid />
        </FormField>
        <FormField label="Read timeout (ms)" for="prw-read-timeout">
          <OnmsInputNumber v-model="form.readTimeoutInMs" inputId="prw-read-timeout" :min="0" fluid />
        </FormField>
      </div>

      <FormField label="Bulkhead max wait" for="prw-bulkhead-unlimited">
        <template #help>
          How long a write may wait for a free slot in the async HTTP bulkhead before being rejected.
          "Unlimited" (the default) means writes block until a slot frees up rather than being dropped.
        </template>
        <div class="settings__toggle-row">
          <OnmsToggleSwitch v-model="form.bulkheadUnlimited" inputId="prw-bulkhead-unlimited" />
          <label for="prw-bulkhead-unlimited">Unlimited (block until a slot is free)</label>
        </div>
        <div v-if="!form.bulkheadUnlimited" class="settings__unit-row">
          <OnmsInputNumber v-model="form.bulkheadMaxWaitDuration" :min="0" aria-label="Bulkhead max wait in milliseconds" />
          <span class="settings__unit">ms</span>
        </div>
      </FormField>
    </section>

    <!-- Caching -->
    <section class="settings__section">
      <h2>Caching</h2>
      <div class="settings__grid">
        <FormField label="Metric cache size" for="prw-metric-cache">
          <template #help>
            Number of metric definitions cached in memory to avoid re-fetching metadata on reads.
          </template>
          <OnmsInputNumber v-model="form.metricCacheSize" inputId="prw-metric-cache" :min="0" fluid />
        </FormField>
        <FormField label="External tags cache size" for="prw-ext-cache">
          <template #help>
            Number of external-tag sets cached to reduce key-value-store lookups when persisting metadata.
          </template>
          <OnmsInputNumber v-model="form.externalTagsCacheSize" inputId="prw-ext-cache" :min="0" fluid />
        </FormField>
      </div>
    </section>

    <!-- Query -->
    <section class="settings__section">
      <h2>Query</h2>
      <FormField label="Max series lookback" for="prw-lookback"
                 :hint="`Stored as seconds (${form.maxSeriesLookback.toLocaleString()} s).`">
        <template #help>
          How far back metric/series lookups search.
        </template>
        <div class="settings__unit-row">
          <OnmsInputNumber
            :modelValue="lookbackDays"
            inputId="prw-lookback"
            :min="0"
            @update:modelValue="onLookbackChange"
          />
          <span class="settings__unit">days</span>
        </div>
      </FormField>
    </section>

    <!-- Multi-tenancy -->
    <section class="settings__section">
      <h2>Multi-tenancy</h2>
      <FormField label="Organization ID" for="prw-settings-org">
        <template #help>
          Sent as the <code>X-Scope-OrgID</code> header on reads &amp; writes for multi-tenant backends like
          Cortex/Mimir. Leave blank for single-tenant.
        </template>
        <OnmsInputText id="prw-settings-org" v-model="form.organizationId" placeholder="(none)" />
      </FormField>
    </section>

    <!-- Save bar -->
    <div class="settings__save-bar">
      <OnmsButton
        :label="savingConfig ? 'Saving…' : 'Save configuration'"
        :disabled="savingConfig"
        :loading="savingConfig"
        @click="saveConfig"
      />
      <OnmsButton label="Reset" variant="text" :disabled="loadingConfig" @click="loadConfig" />
    </div>

    <!-- Integration control — this plugin is the active store; allow reverting -->
    <section class="settings__integration">
      <h2>Time-series integration</h2>
      <p>
        This plugin is currently the <strong>active OpenNMS time-series store</strong>
        (strategy <code>{{ health?.runningStrategy }}</code>). To stop using Prometheus, revert to the previous
        strategy. New metrics will go back to RRD; data already written to Prometheus stays in Prometheus and
        won't be merged back. <strong>Requires an OpenNMS restart</strong> after reverting:
      </p>
      <pre class="settings__commands">{{ restartCommands }}</pre>
      <OnmsButton
        label="Disable integration / revert…"
        variant="outlined"
        :disabled="applyingIntegration"
        @click="openConfirm('disable')"
      />
      <p v-if="health && !health.integrationManaged" class="settings__integration-note">
        Note: integration wasn't enabled via this plugin (no managed file), so it's set elsewhere
        (source: {{ health.strategySource }}). Revert will tell you where to remove it.
      </p>
    </section>
  </template>
</template>

<script setup lang="ts">
import {
  OnmsButton, OnmsInputNumber, OnmsInputText, OnmsSpinner, OnmsToggleSwitch,
} from '@opennms/onms-ui'
import {
  applyingIntegration, form, health, loadConfig, loadingConfig, lookbackDays, openConfirm,
  restartCommands, saveConfig, savingConfig, sendTestWrite, syncLookbackToSeconds, testConnection,
  testing, testingWrite, testResults, writeTestResult,
} from '../store'
import EndpointFields from './EndpointFields.vue'
import FormField from './FormField.vue'
import TestResultList from './TestResultList.vue'

function onLookbackChange(v: number | null) {
  lookbackDays.value = v ?? 0
  syncLookbackToSeconds()
}
</script>

<style scoped>
.settings__loading {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
  font-size: 0.875rem;
}
.settings__section {
  margin-bottom: 1.75rem;
}
.settings__section h2 {
  font-size: 1rem;
  font-weight: 600;
  margin: 0 0 0.75rem;
  padding-bottom: 0.4rem;
  border-bottom: 1px solid var(--p-content-border-color, rgba(10, 12, 27, 0.08));
}
.settings__grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0 1rem;
}
.settings__test-actions {
  display: flex;
  gap: 0.75rem;
  align-items: center;
  margin-top: 0.75rem;
  flex-wrap: wrap;
}
.settings__test-note {
  font-size: 0.75rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.5));
}
.settings__toggle-row {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.8125rem;
  margin-bottom: 0.4rem;
}
.settings__unit-row {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  max-width: 240px;
}
.settings__unit {
  font-size: 0.8125rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
}
.settings__save-bar {
  display: flex;
  gap: 0.75rem;
  padding-top: 1rem;
  border-top: 1px solid var(--p-content-border-color, rgba(10, 12, 27, 0.1));
}
.settings__integration {
  margin-top: 2rem;
  padding: 1rem 1.25rem;
  border: 1px solid var(--p-orange-200, #f6d0a2);
  background: var(--p-orange-50, #fdf6ee);
  border-radius: 6px;
}
.settings__integration h2 {
  font-size: 0.95rem;
  font-weight: 600;
  margin: 0 0 0.4rem;
  color: var(--p-orange-800, #7a5000);
}
.settings__integration p {
  margin: 0 0 0.5rem;
  font-size: 0.8125rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.7));
}
.settings__commands {
  margin: 0 0 0.75rem;
  padding: 0.6rem 0.75rem;
  background: #0d1117;
  color: #e6edf3;
  border-radius: 4px;
  font-size: 0.72rem;
  overflow: auto;
  white-space: pre-wrap;
}
.settings__integration-note {
  margin-top: 0.6rem !important;
  font-size: 0.75rem !important;
  color: var(--p-orange-700, #9a7400) !important;
}
</style>
