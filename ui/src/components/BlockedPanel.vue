<template>
  <section class="blocked">
    <div class="blocked__head">
      <OnmsTag value="NOT ACTIVE" severity="warn" />
      <strong>This plugin isn't active yet</strong>
    </div>
    <p class="blocked__intro">
      Configuration is locked until the checks below pass — there's no point editing settings the plugin
      can't use yet. Current OpenNMS time-series strategy:
      <code>{{ health?.timeseriesStrategy }}</code>
      <span class="blocked__source">(from {{ health?.strategySource }})</span>.
    </p>

    <IssueList :issues="health?.issues ?? []" />

    <div class="blocked__recheck">
      <OnmsButton
        :label="checkingHealth ? 'Checking…' : 'Re-check'"
        variant="outlined"
        :disabled="checkingHealth"
        :loading="checkingHealth"
        @click="loadHealth"
      />
    </div>

    <!-- Backend readiness — verify the target backend BEFORE flipping to integration -->
    <div class="blocked__section">
      <div class="blocked__section-title">Verify your backend now</div>
      <p class="blocked__section-text">
        You run the backend yourself — any Prometheus <code>remote_write</code>-compatible store
        (<strong>Cortex, Mimir, Thanos, or VictoriaMetrics</strong>) — and this plugin writes to it. Point the
        URLs below at your running backend, confirm it's reachable and accepting metrics, then save. If a check
        fails, fix the endpoint here and re-test; you don't need to enable integration first.
      </p>

      <!-- Editable connection endpoints (the rest of the config stays locked until integration is on) -->
      <EndpointFields includeOrg />

      <div class="blocked__actions">
        <OnmsButton
          :label="testing ? 'Checking…' : 'Check backend readiness'"
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
          title="Pushes one synthetic sample into the backend"
          @click="sendTestWrite"
        />
        <OnmsButton
          :label="savingConfig ? 'Saving…' : 'Save endpoints'"
          :disabled="savingConfig"
          :loading="savingConfig"
          @click="saveConfig"
        />
        <span class="blocked__actions-note">
          Tests use the values above (unsaved). “Send test write” pushes one real sample.
        </span>
      </div>

      <TestResultList :results="testResults" :writeResult="writeTestResult" />
    </div>

    <!-- Enable integration (or, if already enabled on disk, prompt for the restart + offer revert) -->
    <div class="blocked__section">
      <template v-if="health?.pendingRestart && health?.configuredStrategy === 'integration'">
        <div class="blocked__pending-head">
          <OnmsTag value="PENDING RESTART" severity="success" />
          <strong>Integration is enabled on disk — restart OpenNMS to apply</strong>
        </div>
        <p class="blocked__section-text">
          Wrote <code>{{ managedFilePath }}</code>. OpenNMS is still running
          <code>{{ health?.runningStrategy }}</code> until you restart — nothing flows through this plugin yet.
          Run one of these to apply:
        </p>
        <pre class="blocked__commands">{{ restartCommands }}</pre>
        <OnmsButton
          label="Revert (undo)"
          variant="outlined"
          :disabled="applyingIntegration"
          @click="openConfirm('disable')"
        />
      </template>
      <template v-else>
        <div class="blocked__section-title">Enable integration</div>
        <p class="blocked__section-text">
          Switch OpenNMS to this plugin. This writes a dedicated, reversible file and
          <strong>requires an OpenNMS restart</strong>. It changes how <strong>all</strong> OpenNMS metrics are
          stored — read the confirmation carefully. Enabled only after a test write to the saved endpoints succeeds.
        </p>
        <OnmsButton
          label="Enable integration…"
          :disabled="applyingIntegration || !canEnableIntegration"
          :title="canEnableIntegration ? '' : enableBlockedReason"
          @click="openConfirm('enable')"
        />
        <p v-if="!canEnableIntegration" class="blocked__locked-reason">{{ enableBlockedReason }}</p>
      </template>
    </div>
  </section>
</template>

<script setup lang="ts">
import { OnmsButton, OnmsTag } from '@opennms/onms-ui'
import {
  applyingIntegration, canEnableIntegration, checkingHealth, enableBlockedReason, health,
  loadHealth, managedFilePath, openConfirm, restartCommands, saveConfig, savingConfig,
  sendTestWrite, testConnection, testing, testingWrite, testResults, writeTestResult,
} from '../store'
import EndpointFields from './EndpointFields.vue'
import IssueList from './IssueList.vue'
import TestResultList from './TestResultList.vue'
</script>

<style scoped>
.blocked {
  border: 1px solid var(--p-orange-200, #f5e0a2);
  background: var(--p-orange-50, #fef9e7);
  border-radius: 8px;
  padding: 1.5rem 1.75rem;
}
.blocked__head,
.blocked__pending-head {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.6rem;
  font-size: 1.05rem;
  color: var(--p-orange-800, #7a6000);
}
.blocked__pending-head {
  font-size: 0.875rem;
  color: var(--p-text-color, rgba(10, 12, 27, 0.85));
  margin-bottom: 0.4rem;
}
.blocked__intro {
  margin: 0 0 1rem;
  font-size: 0.875rem;
  color: var(--p-orange-800, #7a6000);
}
.blocked__source {
  opacity: 0.75;
}
.blocked__recheck {
  margin-top: 1rem;
}
.blocked__section {
  margin-top: 1.5rem;
  padding-top: 1.1rem;
  border-top: 1px solid var(--p-orange-300, rgba(154, 116, 0, 0.3));
}
.blocked__section-title {
  font-weight: 600;
  font-size: 0.875rem;
  color: var(--p-orange-800, #7a6000);
  margin-bottom: 0.35rem;
}
.blocked__section-text {
  margin: 0 0 0.7rem;
  font-size: 0.8125rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.7));
}
.blocked__actions {
  display: flex;
  gap: 0.5rem;
  flex-wrap: wrap;
  align-items: center;
}
.blocked__actions-note {
  font-size: 0.72rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.5));
}
.blocked__commands {
  margin: 0 0 0.7rem;
  padding: 0.6rem 0.75rem;
  background: #0d1117;
  color: #e6edf3;
  border-radius: 4px;
  font-size: 0.72rem;
  overflow: auto;
  white-space: pre-wrap;
}
.blocked__locked-reason {
  margin: 0.5rem 0 0;
  font-size: 0.75rem;
  color: var(--p-orange-700, #9a7400);
}
</style>
