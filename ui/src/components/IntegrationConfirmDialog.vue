<template>
  <OnmsDialog
    :visible="confirmVisible"
    :header="confirmMode === 'enable' ? 'Enable integration strategy?' : 'Disable integration / revert?'"
    width="560px"
    :closable="!applyingIntegration"
    @update:visible="confirmVisible = $event"
  >
    <div class="confirm">
      <div class="confirm__warning">
        <OnmsTag value="SYSTEM-WIDE" severity="warn" />
        <div>
          <strong>This is a system-wide OpenNMS change and requires a restart.</strong>
          <ul>
            <li>It changes how <strong>all</strong> OpenNMS metrics are stored — not just this plugin.</li>
            <li>Existing data isn't migrated: after enabling, new metrics go to Prometheus; after reverting,
              they go back to RRD. The two are <strong>not merged</strong>, so expect a gap in historical
              graphs around the switch.</li>
            <li>Collection/monitoring pauses briefly during the OpenNMS restart.</li>
            <li v-if="confirmMode === 'enable'">Make sure your Prometheus backend is running and the readiness
              check passes first.</li>
          </ul>
        </div>
      </div>

      <template v-if="confirmMode === 'enable'">
        <p><strong>1.</strong> Creates this file:</p>
        <pre class="confirm__pre">{{ managedFilePath }}</pre>
        <p><strong>2.</strong> With exactly these lines:</p>
        <pre class="confirm__pre">{{ MANAGED_FILE_CONTENT }}</pre>
        <p><strong>3.</strong> Nothing changes until <u>you</u> restart OpenNMS — run one of:</p>
        <pre class="confirm__pre confirm__pre--terminal">{{ restartCommands }}</pre>
        <p class="confirm__footnote">Revert any time by removing that file (or using “Disable / revert”).</p>
      </template>
      <template v-else>
        <p><strong>1.</strong> Removes this file:</p>
        <pre class="confirm__pre">{{ managedFilePath }}</pre>
        <p><strong>2.</strong> Then restart OpenNMS to revert to the previous strategy — run one of:</p>
        <pre class="confirm__pre confirm__pre--terminal">{{ restartCommands }}</pre>
        <p class="confirm__footnote">If integration was also set in another file, you'll be told exactly where
          to remove it.</p>
      </template>
    </div>

    <template #footer>
      <OnmsButton label="Cancel" variant="text" :disabled="applyingIntegration"
                  @click="confirmVisible = false" />
      <OnmsButton
        :label="applyingIntegration ? 'Working…' : (confirmMode === 'enable' ? 'Enable integration' : 'Disable / revert')"
        :disabled="applyingIntegration"
        :loading="applyingIntegration"
        @click="runConfirm"
      />
    </template>
  </OnmsDialog>
</template>

<script setup lang="ts">
import { OnmsButton, OnmsDialog, OnmsTag } from '@opennms/onms-ui'
import {
  applyingIntegration, confirmMode, confirmVisible, MANAGED_FILE_CONTENT, managedFilePath,
  restartCommands, runConfirm,
} from '../store'
</script>

<style scoped>
.confirm {
  font-size: 0.8125rem;
  color: var(--p-text-color, rgba(10, 12, 27, 0.85));
  line-height: 1.5;
}
.confirm p {
  margin: 0 0 0.3rem;
}
.confirm__warning {
  display: flex;
  align-items: flex-start;
  gap: 0.6rem;
  padding: 0.6rem 0.75rem;
  background: var(--p-content-hover-background, rgba(10, 12, 27, 0.03));
  border: 1px solid var(--p-content-border-color, rgba(10, 12, 27, 0.12));
  border-left: 4px solid var(--p-orange-400, #cc8925);
  border-radius: 4px;
  margin-bottom: 0.9rem;
}
.confirm__warning ul {
  margin: 0.4rem 0 0;
  padding-left: 1.1rem;
}
.confirm__pre {
  margin: 0 0 0.5rem;
  padding: 0.4rem 0.6rem;
  background: var(--p-content-hover-background, rgba(10, 12, 27, 0.05));
  border-radius: 4px;
  font-size: 0.72rem;
  overflow: auto;
  white-space: pre-wrap;
}
.confirm__pre--terminal {
  background: #0d1117;
  color: #e6edf3;
  padding: 0.6rem 0.75rem;
}
.confirm__footnote {
  margin-top: 0.6rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
}
</style>
