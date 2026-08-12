<template>
  <div class="prw">
    <OnmsToastHost />

    <!-- Header -->
    <div class="prw__header">
      <h1>Prometheus RemoteWrite</h1>
      <p>
        Time-series storage that writes &amp; reads metrics via the Prometheus
        <code>remote_write</code> / <code>remote_read</code> protocol.
      </p>
    </div>

    <!-- ░░ Preflight: still checking ░░ -->
    <div v-if="uiState === 'loading'" class="prw__loading">
      <OnmsSpinner size="1.5rem" /> Checking plugin status…
    </div>

    <!-- ░░ Check failed: REST API unreachable → clean error page, no config ░░ -->
    <UnreachablePanel
      v-else-if="uiState === 'unreachable'"
      :error="healthError"
      :checking="checkingHealth"
      @recheck="loadHealth"
    />

    <!-- ░░ Check failed: prerequisites not met → instruction page, config is LOCKED ░░ -->
    <BlockedPanel v-else-if="uiState === 'blocked'" />

    <!-- ░░ All checks passed → the actual configuration UI ░░ -->
    <template v-else>
      <!-- Non-blocking warnings (plugin works, but something else will bite — e.g. wrong graph engine) -->
      <IssueList v-if="health?.issues?.length" class="prw__warnings" :issues="health!.issues" />

      <OnmsTabs v-model:value="activeTab">
        <OnmsTabList>
          <OnmsTab value="settings">Settings</OnmsTab>
          <OnmsTab value="stats">Statistics</OnmsTab>
          <OnmsTab value="explorer">Metric Explorer</OnmsTab>
        </OnmsTabList>
        <OnmsTabPanels>
          <OnmsTabPanel value="settings">
            <SettingsTab />
          </OnmsTabPanel>
          <OnmsTabPanel value="stats">
            <StatsTab :active="activeTab === 'stats'" />
          </OnmsTabPanel>
          <OnmsTabPanel value="explorer">
            <ExplorerTab :active="activeTab === 'explorer'" />
          </OnmsTabPanel>
        </OnmsTabPanels>
      </OnmsTabs>
    </template>

    <!-- Confirmation dialog for enabling / reverting the system-wide integration strategy -->
    <IntegrationConfirmDialog />
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import {
  OnmsSpinner, OnmsTab, OnmsTabList, OnmsTabPanel, OnmsTabPanels, OnmsTabs, OnmsToastHost,
} from '@opennms/onms-ui'
import { checkingHealth, health, healthError, loadConfig, loadHealth, uiState } from './store'
import BlockedPanel from './components/BlockedPanel.vue'
import ExplorerTab from './components/ExplorerTab.vue'
import IntegrationConfirmDialog from './components/IntegrationConfirmDialog.vue'
import IssueList from './components/IssueList.vue'
import SettingsTab from './components/SettingsTab.vue'
import StatsTab from './components/StatsTab.vue'
import UnreachablePanel from './components/UnreachablePanel.vue'

const activeTab = ref<string | number>('settings')

onMounted(async () => {
  // Preflight first. Load the saved config whenever the REST API is reachable — in the
  // 'ready' state it populates the editable form, and in the 'blocked' state it lets the
  // operator verify the saved backend endpoints before turning integration on.
  await loadHealth()
  if (uiState.value === 'ready' || uiState.value === 'blocked') {
    await loadConfig()
  }
})
</script>

<style scoped>
.prw {
  font-family: var(--p-font-family, 'Open Sans', system-ui, sans-serif);
  color: var(--p-text-color, rgba(10, 12, 27, 0.9));
  padding: 1.5rem;
  max-width: 1100px;
}
.prw__header {
  margin-bottom: 1.25rem;
}
.prw__header h1 {
  font-size: 1.5rem;
  font-weight: 600;
  margin: 0 0 0.25rem;
}
.prw__header p {
  margin: 0;
  font-size: 0.875rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
}
.prw__loading {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
  font-size: 0.9rem;
  padding: 2rem 0;
}
.prw__warnings {
  margin-bottom: 1.25rem;
}
</style>
