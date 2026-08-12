# AGENTS.md

Guidance for AI coding agents working in this repository.

## Build & test

Requires JDK 17 and Maven.

```bash
mvn clean install                         # compile + unit tests
mvn -DskipITs=false clean verify          # + integration tests (needs Docker)
mvn -Dtest=CortexTSSTest test             # run a single unit test
mvn -Dit.test=NMS16271_IT -DskipITs=false verify   # run a single integration test
```

## Architecture

An OpenNMS Time Series Storage (TSS) plugin. It implements the Integration API
`TimeSeriesStorage` interface (`CortexTSS`) and translates OpenNMS samples to the
Prometheus data model, delegating writes and reads over the Prometheus
`remote_write` / `remote_read` protocol to any compatible backend (Cortex, Mimir,
Thanos, VictoriaMetrics, Prometheus).

Maven modules:

- `plugin/` — the implementation. Key classes under
  `org.opennms.timeseries.cortex`: `CortexTSS` (storage impl), `CortexTSSConfig`
  (OSGi config), `ResultMapper` (Prometheus ↔ OpenNMS mapping), `shell/` (Karaf
  commands).
- `karaf-features/` — Karaf feature descriptor.
- `assembly/` — builds the deployable `.kar` archive.
- `wrap/` — OSGi-wraps non-OSGi dependencies.

## Conventions & gotchas

- Java package/class names still use `cortex`; the plugin is backend-agnostic
  despite the name. Do not rename packages — it is intentional for compatibility.
- OSGi config PID is `org.opennms.plugins.tss.prometheus`.
- Develop against `main`; releases are cut from tags (see [RELEASING.md](RELEASING.md)).
- Sign off every commit (`git commit -s`) and add an `Assisted-by` trailer for
  AI-assisted work (see [CONTRIBUTING.md](CONTRIBUTING.md)).
- New/edited source files need the OpenNMS SPDX license header (AGPL-3.0).
