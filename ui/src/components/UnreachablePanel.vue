<template>
  <section class="unreachable">
    <div class="unreachable__head">
      <OnmsTag value="UNREACHABLE" severity="danger" />
      <strong>Can't reach the plugin's REST API</strong>
    </div>
    <p class="unreachable__text">
      This screen can't load because its backend
      (<code>/opennms/rest/prometheus-remotewrite</code>) did not respond — so we won't pretend the
      configuration is usable. This usually means the plugin bundle isn't fully started.
    </p>
    <pre class="unreachable__error">{{ error }}</pre>
    <p class="unreachable__text">
      <strong>How to fix:</strong> confirm the <code>opennms-plugins-prometheus-remotewrite</code> feature is
      installed and its bundle is Active (Karaf: <code>feature:list -i | grep prometheus</code>), then re-check.
    </p>
    <OnmsButton
      :label="checking ? 'Checking…' : 'Re-check'"
      variant="outlined"
      :disabled="checking"
      :loading="checking"
      @click="emit('recheck')"
    />
  </section>
</template>

<script setup lang="ts">
import { OnmsButton, OnmsTag } from '@opennms/onms-ui'

defineProps<{
  error: string | null
  checking: boolean
}>()

const emit = defineEmits<{
  recheck: []
}>()
</script>

<style scoped>
.unreachable {
  border: 1px solid var(--p-red-200, #f6aea9);
  background: var(--p-red-50, #fdf3f2);
  border-radius: 8px;
  padding: 1.5rem 1.75rem;
}
.unreachable__head {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.6rem;
  font-size: 1.05rem;
  color: var(--p-red-800, #7a1c17);
}
.unreachable__text {
  margin: 0 0 0.75rem;
  font-size: 0.875rem;
  color: var(--p-text-color, rgba(10, 12, 27, 0.75));
}
.unreachable__error {
  margin: 0 0 1rem;
  font-size: 0.8125rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
  font-family: monospace;
  background: var(--p-content-hover-background, rgba(10, 12, 27, 0.04));
  padding: 0.5rem 0.7rem;
  border-radius: 4px;
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
