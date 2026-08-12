<template>
  <div>
    <FormField label="Write URL" for="prw-write-url" required :error="formErrors.writeUrl">
      <template #help>
        The Prometheus <code>remote_write</code> push endpoint (e.g. Cortex / Mimir / Thanos /
        VictoriaMetrics). Samples are POSTed here.
      </template>
      <OnmsInputText
        id="prw-write-url"
        v-model="form.writeUrl"
        fluid
        :invalid="!!formErrors.writeUrl"
        placeholder="http://localhost:9009/api/prom/push"
      />
    </FormField>

    <FormField label="Read URL" for="prw-read-url" required :error="formErrors.readUrl">
      <template #help>
        The Prometheus query API base used for reads (<code>/series</code>, <code>/query_range</code>).
        Usually the querier endpoint.
      </template>
      <OnmsInputText
        id="prw-read-url"
        v-model="form.readUrl"
        fluid
        :invalid="!!formErrors.readUrl"
        placeholder="http://localhost:9009/prometheus/api/v1"
      />
    </FormField>

    <FormField v-if="includeOrg" label="Organization ID" for="prw-org-id"
               hint="Optional — sent as the X-Scope-OrgID header. Leave blank for single-tenant.">
      <template #help>
        Sent as the <code>X-Scope-OrgID</code> header on reads &amp; writes for multi-tenant backends
        like Cortex/Mimir. Leave blank for single-tenant.
      </template>
      <OnmsInputText id="prw-org-id" v-model="form.organizationId" fluid placeholder="(none)" />
    </FormField>
  </div>
</template>

<script setup lang="ts">
import { OnmsInputText } from '@opennms/onms-ui'
import { form, formErrors } from '../store'
import FormField from './FormField.vue'

withDefaults(defineProps<{
  includeOrg?: boolean
}>(), {
  includeOrg: false,
})
</script>
