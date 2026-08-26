import eslint from '@eslint/js'
import globals from 'globals'
import pluginVue from 'eslint-plugin-vue'
import tseslint from 'typescript-eslint'

// Mirrors the OpenNMS core ui/eslint.config.js seam enforcement (NMS-20029):
// plugin UIs consume Onms-* components from @opennms/onms-ui instead of
// importing PrimeVue directly, so the underlying framework can be swapped
// without rewriting consumers.
export default tseslint.config(
  { ignores: ['dist/**', 'node_modules/**', '**/*.d.ts'] },
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  ...pluginVue.configs['flat/essential'],

  // Tell vue-eslint-parser to use @typescript-eslint/parser for <script lang="ts">
  {
    files: ['**/*.vue'],
    languageOptions: {
      parserOptions: { parser: tseslint.parser },
    },
    // TS type-checking (vue-tsc) owns undefined-identifier detection in
    // TS script blocks; base no-undef false-positives on DOM lib types.
    // multi-word-component-names is off to match core (vendored icons are
    // single-word: Info.vue, Delete.vue, …).
    rules: {
      'no-undef': 'off',
      'vue/multi-word-component-names': 'off',
    },
  },
  {
    files: ['src/**/*.ts', 'src/**/*.vue'],
    languageOptions: {
      globals: { ...globals.browser },
    },
  },

  // NMS-20029 seam: no direct PrimeVue imports outside @opennms/onms-ui
  {
    files: ['src/**/*.ts', 'src/**/*.vue'],
    rules: {
      'no-restricted-imports': ['error', {
        patterns: [{
          group: ['primevue', 'primevue/*'],
          message: 'Use the Onms-* components from @opennms/onms-ui (NMS-20029 seam).',
        }],
      }],
    },
  },

  // Sanctioned exception, mirroring core's theme/primevue-setup.ts: installing
  // the PrimeVue plugin for the standalone dev-server mount is a host bootstrap
  // concern, not a seam-wrapped component.
  {
    files: ['src/main.ts'],
    rules: { 'no-restricted-imports': 'off' },
  },
)
