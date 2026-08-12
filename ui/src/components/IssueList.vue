<template>
  <div class="issue-list">
    <div v-for="(iss, i) in issues" :key="i" class="issue" :class="`issue--${iss.severity}`">
      <div class="issue__title">
        <OnmsTag :value="iss.severity === 'error' ? 'ERROR' : 'WARNING'"
                 :severity="iss.severity === 'error' ? 'danger' : 'warn'" />
        {{ iss.title }}
      </div>
      <div class="issue__detail">{{ iss.detail }}</div>
      <div class="issue__remedy"><strong>How to fix:</strong> {{ iss.remedy }}</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { OnmsTag } from '@opennms/onms-ui'
import { HealthIssue } from '../api'

defineProps<{
  issues: HealthIssue[]
}>()
</script>

<style scoped>
.issue-list {
  display: flex;
  flex-direction: column;
  gap: 0.6rem;
}
.issue {
  border-left: 4px solid var(--p-orange-500, #9a7400);
  padding: 0.6rem 0.9rem;
  border-radius: 0 6px 6px 0;
  background: var(--p-content-hover-background, rgba(10, 12, 27, 0.03));
}
.issue--error {
  border-left-color: var(--p-red-500, #c5221f);
}
.issue__title {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-weight: 600;
  font-size: 0.875rem;
  color: var(--p-text-color, rgba(10, 12, 27, 0.85));
}
.issue__detail {
  font-size: 0.8125rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.7));
  margin-top: 0.25rem;
}
.issue__remedy {
  font-size: 0.8125rem;
  color: var(--p-text-color, rgba(10, 12, 27, 0.85));
  margin-top: 0.4rem;
}
</style>
