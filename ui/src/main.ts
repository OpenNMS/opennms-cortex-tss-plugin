import App from './App.vue'
import { createApp } from 'vue'
import PrimeVue from 'primevue/config'

// Expose the root component on window so OpenNMS can find and mount it.
// OpenNMS reads UIExtension.getExtensionId() = "prometheusremotewrite" to locate it here.
;(window as unknown as Record<string, unknown>)['prometheusremotewrite'] = App

// Full standalone mount only when running the Vite dev server
if (import.meta.env.MODE === 'development') {
  const app = createApp(App)
  app.use(PrimeVue, { ripple: true })
  app.mount('#app')
}
