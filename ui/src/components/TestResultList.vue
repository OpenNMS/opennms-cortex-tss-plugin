<template>
  <div v-if="results || writeResult" class="test-results">
    <div v-for="r in results ?? []" :key="r.endpoint" class="result" :class="{ 'result--fail': !r.reachable }">
      <div class="result__head">
        <OnmsTag :value="r.reachable ? 'OK' : 'FAIL'" :severity="r.reachable ? 'success' : 'danger'" />
        <strong class="result__endpoint">{{ r.endpoint }} endpoint</strong>
        <span class="result__ms">{{ r.durationMs }} ms</span>
      </div>
      <div class="result__url">{{ r.url }}</div>
      <div class="result__detail">{{ r.detail }}</div>
      <div v-if="r.endpoint === 'write' && r.reachable && r.statusCode >= 400" class="result__note">
        A {{ r.statusCode }} from a write endpoint is normal for a GET — it expects POST, so reachability is
        confirmed. Use “Send test write” to verify it actually accepts metrics.
      </div>
    </div>

    <div v-if="writeResult" class="result" :class="{ 'result--fail': !writeResult.reachable }">
      <div class="result__head">
        <OnmsTag :value="writeResult.reachable ? 'OK' : 'FAIL'"
                 :severity="writeResult.reachable ? 'success' : 'danger'" />
        <strong>{{ writeResult.reachable ? 'Backend accepted a test write' : 'Test write rejected' }}</strong>
        <span class="result__ms">{{ writeResult.durationMs }} ms</span>
      </div>
      <div class="result__url">{{ writeResult.url }}</div>
      <div class="result__detail">{{ writeResult.detail }}</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { OnmsTag } from '@opennms/onms-ui'
import { TestConnectionResult } from '../api'

defineProps<{
  results?: TestConnectionResult[] | null
  writeResult?: TestConnectionResult | null
}>()
</script>

<style scoped>
.test-results {
  margin-top: 0.75rem;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}
.result {
  border: 1px solid var(--p-content-border-color, rgba(10, 12, 27, 0.12));
  border-left: 4px solid var(--p-green-400, #137333);
  background: var(--p-content-background, #fff);
  border-radius: 4px;
  padding: 0.625rem 0.875rem;
  font-size: 0.8125rem;
}
.result--fail {
  border-left-color: var(--p-red-400, #c5221f);
}
.result__head {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}
.result__endpoint {
  text-transform: capitalize;
}
.result__ms {
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.5));
}
.result__url {
  margin-top: 0.25rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.7));
  word-break: break-all;
}
.result__detail {
  margin-top: 0.15rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.7));
}
.result__note {
  margin-top: 0.15rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
}
</style>
