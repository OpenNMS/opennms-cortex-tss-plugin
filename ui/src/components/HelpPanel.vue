<template>
  <div class="help-panel">
    <slot />
  </div>
</template>

<script setup lang="ts">
// Inline help box shown when a FormField / section Info icon is toggled open.
</script>

<style scoped>
.help-panel {
  margin-bottom: 0.5rem;
  padding: 0.5rem 0.875rem;
  border-left: 3px solid var(--p-primary-color, #273180);
  border-radius: 0 4px 4px 0;
  background: var(--p-highlight-background, rgba(39, 49, 128, 0.06));
  font-size: 0.8125rem;
  line-height: 1.55;
  color: var(--p-text-color, inherit);
}
.help-panel :deep(ul) {
  margin: 0.3rem 0;
  padding-left: 1.1rem;
}
</style>
