<template>
  <div class="stat-tile">
    <div class="stat-tile__value">{{ value }}</div>
    <div class="stat-tile__label">{{ label }}</div>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  value: string
  label: string
}>()
</script>

<style scoped>
.stat-tile {
  border: 1px solid var(--p-content-border-color, rgba(10, 12, 27, 0.12));
  border-radius: 6px;
  padding: 0.875rem 1rem;
}
.stat-tile__value {
  font-size: 1.5rem;
  font-weight: 600;
  color: var(--p-primary-color, #273180);
}
.stat-tile__label {
  font-size: 0.75rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
  margin-top: 0.2rem;
}
</style>
