import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import cssInjectedByJs from 'vite-plugin-css-injected-by-js'
import { resolve } from 'path'

export default defineConfig({
  // The OpenNMS UIExtension host loads only the JS module (no <link> for
  // style.css), so component CSS must ship inside the bundle.
  plugins: [vue(), cssInjectedByJs()],
  resolve: {
    // @opennms/onms-ui is consumed as linked source — make sure it compiles
    // against this package's vue/primevue instances, not its own resolution.
    dedupe: ['vue', 'primevue'],
  },
  build: {
    lib: {
      entry: resolve(__dirname, 'src/main.ts'),
      name: 'prometheusremotewrite',
      // IIFE format so Rollup replaces 'import from vue' with window.Vue.*
      // (ES module format generates bare 'import from "vue"' which browsers
      // can't resolve without an import map — OpenNMS provides Vue as window.Vue)
      formats: ['iife'],
      fileName: () => 'prometheusremotewrite.es.js',
    },
    rollupOptions: {
      external: ['vue'],
      output: {
        globals: { vue: 'Vue' },
      },
    },
    outDir: '../plugin/src/main/resources/prometheusremotewrite',
    emptyOutDir: true,
  },
})
