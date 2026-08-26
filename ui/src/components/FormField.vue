<template>
  <div class="form-field">
    <div v-if="label" class="form-field__label-row">
      <label :for="controlId" class="form-field__label">
        {{ label }}<span v-if="required" class="form-field__required" aria-hidden="true">*</span>
      </label>
      <OnmsIconButton
        v-if="$slots.help"
        :icon="InfoIcon"
        iconSize="1rem"
        :title="`About ${label}`"
        @click="helpOpen = !helpOpen"
      />
    </div>
    <HelpPanel v-if="helpOpen && $slots.help">
      <slot name="help" />
    </HelpPanel>
    <slot :invalid="invalid" />
    <small v-if="error" class="form-field__error" role="alert">{{ error }}</small>
    <small v-else-if="hint || $slots.hint" class="form-field__hint"><slot name="hint">{{ hint }}</slot></small>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { OnmsIconButton } from '@opennms/onms-ui'
import HelpPanel from './HelpPanel.vue'
import InfoIcon from './icons/Info.vue'

// Local adaptation of core ui/src/components/Common/FormField.vue: standard
// label / hint / error wrapper, plus an optional Info-icon toggled help panel
// (the core "Info icon, not question mark" affordance).
const props = withDefaults(defineProps<{
  label?: string
  for?: string
  required?: boolean
  error?: string
  hint?: string
}>(), {
  label: undefined,
  for: undefined,
  required: false,
  error: undefined,
  hint: undefined,
})

const helpOpen = ref(false)
const controlId = computed(() => props.for)
const invalid = computed(() => !!props.error)
</script>

<style scoped>
.form-field {
  display: flex;
  flex-direction: column;
  margin-bottom: 1rem;
}
.form-field__label-row {
  display: flex;
  align-items: center;
  gap: 0.25rem;
  margin-bottom: 0.375rem;
}
.form-field__label {
  font-size: 0.875rem;
  font-weight: 700;
  color: var(--p-text-color, inherit);
}
.form-field__required {
  margin-left: 0.125rem;
  color: var(--p-error-color, #b3261e);
}
.form-field__error {
  margin-top: 0.25rem;
  font-size: 0.875rem;
  color: var(--p-error-color, #b3261e);
}
.form-field__hint {
  margin-top: 0.25rem;
  font-size: 0.8125rem;
  color: var(--p-text-muted-color, rgba(10, 12, 27, 0.6));
}
</style>
