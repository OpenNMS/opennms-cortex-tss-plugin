/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2026 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2026 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *******************************************************************************/

package org.opennms.timeseries.cortex.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;
import org.opennms.timeseries.cortex.CortexTSS;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * REST API backing the Prometheus RemoteWrite plugin UI. Exposed by OpenNMS's
 * JAX-RS whiteboard at {@code /opennms/rest/prometheus-remotewrite/...}
 * (registered as an OSGi service with the {@code application-path=/rest} property).
 */
@Path("/prometheus-remotewrite")
public class PrometheusRemoteWriteRestService {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusRemoteWriteRestService.class);

    /** OSGi configuration PID — must match the blueprint property-placeholder. */
    private static final String PID = "org.opennms.plugins.tss.prometheus";

    // Defaults mirror CortexTSSConfig.Builder / blueprint.xml
    private static final String DEF_WRITE_URL = "http://localhost:9009/api/prom/push";
    private static final String DEF_READ_URL = "http://localhost:9009/prometheus/api/v1";
    private static final int DEF_MAX_CONNS = 100;
    private static final long DEF_WRITE_TIMEOUT = 5000;
    private static final long DEF_READ_TIMEOUT = 5000;
    private static final long DEF_METRIC_CACHE = 1000;
    private static final long DEF_EXT_TAGS_CACHE = 1000;
    private static final long DEF_BULKHEAD_WAIT = Long.MAX_VALUE;
    private static final long DEF_MAX_LOOKBACK = 7776000;

    private static final int PROBE_TIMEOUT_MS = 5000;

    private static final String STRATEGY_PROP = "org.opennms.timeseries.strategy";
    private static final String INTEGRATION = "integration";

    /** Dedicated, self-contained file we manage under etc/opennms.properties.d/ so enable/disable is cleanly reversible. */
    private static final String MANAGED_FILE_NAME = "prometheus-remotewrite.properties";

    /** Exactly what {@code enable} writes — the strategy plus the metatag mappings the integration layer needs. */
    private static final String MANAGED_FILE_CONTENT =
            "# Managed by the OpenNMS Prometheus RemoteWrite plugin UI.\n"
            + "# Enables the integration time-series strategy so metrics are stored via this plugin.\n"
            + "# Delete this file (or use the plugin UI's Revert) and restart OpenNMS to go back.\n"
            + "org.opennms.timeseries.strategy=integration\n"
            + "org.opennms.timeseries.tin.metatags.tag.node=${node:label}\n"
            + "org.opennms.timeseries.tin.metatags.tag.location=${node:location}\n"
            + "org.opennms.timeseries.tin.metatags.tag.geohash=${node:geohash}\n"
            + "org.opennms.timeseries.tin.metatags.tag.ifDescr=${interface:if-description}\n";

    private final ConfigurationAdmin configAdmin;
    /**
     * The storage service. Injected as an <em>optional</em> blueprint reference, so it may be an
     * unavailable proxy if the {@link CortexTSS} bean failed to come up (e.g. no KeyValueStore).
     * Never assume it is callable — guard every use via {@link #isTssActive()}.
     */
    private final CortexTSS tss;

    /**
     * The strategy OpenNMS is ACTUALLY running, captured when this bundle started (≈ OpenNMS boot).
     * The strategy is a boot-time setting, so editing it on disk does not change what's running until a
     * restart. We compare this against the on-disk value to detect a "pending restart" state honestly,
     * rather than flipping the UI to "ready" the instant the file is written.
     */
    private final String runningStrategy;

    public PrometheusRemoteWriteRestService(ConfigurationAdmin configAdmin, CortexTSS tss) {
        this.configAdmin = Objects.requireNonNull(configAdmin);
        // tss may be an optional-reference proxy that throws when no backing service exists; do not requireNonNull.
        this.tss = tss;
        this.runningStrategy = detectTimeseriesStrategy()[0];
    }

    /** True only if the storage service is actually reachable (not just a dangling optional proxy). */
    private boolean isTssActive() {
        if (tss == null) {
            return false;
        }
        try {
            return tss.getMetrics() != null;
        } catch (RuntimeException e) {
            // ServiceUnavailableException (optional ref with no backing service) or similar
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    @GET
    @Path("/config")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConfig() {
        try {
            Configuration cfg = configAdmin.getConfiguration(PID, null);
            Dictionary<String, Object> props = cfg.getProperties();
            PrometheusConfigDto dto = new PrometheusConfigDto();
            dto.setWriteUrl(str(props, "writeUrl", DEF_WRITE_URL));
            dto.setReadUrl(str(props, "readUrl", DEF_READ_URL));
            dto.setMaxConcurrentHttpConnections((int) lng(props, "maxConcurrentHttpConnections", DEF_MAX_CONNS));
            dto.setWriteTimeoutInMs(lng(props, "writeTimeoutInMs", DEF_WRITE_TIMEOUT));
            dto.setReadTimeoutInMs(lng(props, "readTimeoutInMs", DEF_READ_TIMEOUT));
            dto.setMetricCacheSize(lng(props, "metricCacheSize", DEF_METRIC_CACHE));
            dto.setExternalTagsCacheSize(lng(props, "externalTagsCacheSize", DEF_EXT_TAGS_CACHE));
            long bulkhead = lng(props, "bulkheadMaxWaitDuration", DEF_BULKHEAD_WAIT);
            // Long.MAX_VALUE can't be represented exactly as a JS number, so expose it
            // as an "unlimited" flag instead of round-tripping the raw value.
            if (bulkhead >= Long.MAX_VALUE) {
                dto.setBulkheadUnlimited(true);
                dto.setBulkheadMaxWaitDuration(0);
            } else {
                dto.setBulkheadUnlimited(false);
                dto.setBulkheadMaxWaitDuration(bulkhead);
            }
            dto.setMaxSeriesLookback(lng(props, "maxSeriesLookback", DEF_MAX_LOOKBACK));
            dto.setOrganizationId(str(props, "organizationId", ""));
            return Response.ok(dto).build();
        } catch (IOException e) {
            LOG.error("Failed to read plugin config", e);
            return Response.serverError().entity("Failed to read config: " + e.getMessage()).build();
        }
    }

    @PUT
    @Path("/config")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateConfig(PrometheusConfigDto dto) {
        if (dto == null) {
            return Response.status(Response.Status.BAD_REQUEST).entity("Request body required").build();
        }
        if (dto.getWriteUrl() == null || dto.getWriteUrl().trim().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("writeUrl is required").build();
        }
        if (dto.getReadUrl() == null || dto.getReadUrl().trim().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("readUrl is required").build();
        }
        try {
            Configuration cfg = configAdmin.getConfiguration(PID, null);
            Dictionary<String, Object> props = cfg.getProperties();
            if (props == null) {
                props = new Hashtable<>();
            }
            props.put("writeUrl", dto.getWriteUrl().trim());
            props.put("readUrl", dto.getReadUrl().trim());
            props.put("maxConcurrentHttpConnections", String.valueOf(dto.getMaxConcurrentHttpConnections()));
            props.put("writeTimeoutInMs", String.valueOf(dto.getWriteTimeoutInMs()));
            props.put("readTimeoutInMs", String.valueOf(dto.getReadTimeoutInMs()));
            props.put("metricCacheSize", String.valueOf(dto.getMetricCacheSize()));
            props.put("externalTagsCacheSize", String.valueOf(dto.getExternalTagsCacheSize()));
            long bulkhead = dto.isBulkheadUnlimited() ? Long.MAX_VALUE : dto.getBulkheadMaxWaitDuration();
            props.put("bulkheadMaxWaitDuration", String.valueOf(bulkhead));
            props.put("maxSeriesLookback", String.valueOf(dto.getMaxSeriesLookback()));
            props.put("organizationId", dto.getOrganizationId() != null ? dto.getOrganizationId().trim() : "");
            cfg.update(props);
            LOG.info("Updated Prometheus RemoteWrite config via REST: write={} read={}", dto.getWriteUrl(), dto.getReadUrl());
            return Response.ok(dto).build();
        } catch (IOException e) {
            LOG.error("Failed to update plugin config", e);
            return Response.serverError().entity("Failed to update config: " + e.getMessage()).build();
        }
    }

    // -------------------------------------------------------------------------
    // Health / readiness — what's wrong and how to fix it
    // -------------------------------------------------------------------------

    /**
     * Reports whether this plugin is actually doing anything in this OpenNMS, and if not, why,
     * with concrete remediation. The UI renders this so the operator is never left staring at
     * empty tabs wondering whether the plugin is broken.
     */
    @GET
    @Path("/health")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHealth() {
        Map<String, Object> out = new LinkedHashMap<>();

        String[] configuredAndSource = detectTimeseriesStrategy();
        String configuredStrategy = configuredAndSource[0];
        String strategySource = configuredAndSource[1];

        boolean runningIntegration = INTEGRATION.equalsIgnoreCase(runningStrategy);
        boolean configuredIntegration = INTEGRATION.equalsIgnoreCase(configuredStrategy);
        boolean pendingRestart = !configuredStrategy.equalsIgnoreCase(runningStrategy);
        boolean integrationManaged = managedFileExists();
        boolean tssActive = isTssActive();
        // "functional" tracks what's ACTUALLY running, so writing the file does not falsely flip the UI to ready.
        boolean functional = runningIntegration && tssActive;

        List<Map<String, Object>> issues = new ArrayList<>();
        if (pendingRestart && configuredIntegration) {
            issues.add(issue("info",
                    "Integration enabled — restart OpenNMS to apply",
                    "'org.opennms.timeseries.strategy=integration' has been written to " + homeDir()
                            + "/etc/opennms.properties.d/" + MANAGED_FILE_NAME
                            + ", but the time-series strategy is only read at startup, so OpenNMS is still running '"
                            + runningStrategy + "'. Nothing flows through this plugin until you restart.",
                    "Now " + restartHint() + ". Make sure your Prometheus backend is running and the readiness check passed first."));
        } else if (pendingRestart) {
            issues.add(issue("info",
                    "Strategy change pending — restart OpenNMS to apply",
                    "The configured strategy on disk ('" + configuredStrategy + "') differs from what OpenNMS is "
                            + "running ('" + runningStrategy + "'). The strategy is only read at startup.",
                    "Now " + restartHint() + "."));
        } else if (!runningIntegration) {
            issues.add(issue("warning",
                    "OpenNMS is using the '" + runningStrategy + "' time-series strategy",
                    "This plugin is installed, but OpenNMS is not routing metric samples through it, so nothing is "
                            + "being written to or read from your Prometheus remote_write backend. The plugin is idle.",
                    "Click “Enable integration” below — it creates the file " + homeDir()
                            + "/etc/opennms.properties.d/" + MANAGED_FILE_NAME
                            + " containing 'org.opennms.timeseries.strategy=integration' plus the "
                            + "'org.opennms.timeseries.tin.metatags.tag.*' mappings (you can also create that file by hand). "
                            + "Then " + restartHint() + ". This changes how ALL OpenNMS metrics are stored — make sure a "
                            + "Prometheus remote_write-compatible backend (Cortex/Mimir/Thanos/VictoriaMetrics) is running first."));
        }
        if (runningIntegration && !tssActive) {
            issues.add(issue("error",
                    "The Prometheus storage service is not active",
                    "OpenNMS is running the 'integration' strategy, but this plugin's storage service did not "
                            + "come up — so metric storage will fail.",
                    "Check karaf.log for this bundle. The most common cause is a missing KeyValueStore service. "
                            + "Verify the 'opennms-plugins-prometheus-remotewrite' feature is fully installed."));
        }

        // Graphs are rendered by the webapp's graph engine. Anything but 'backshift' draws PNGs from
        // local RRD files, which stop receiving data under the integration strategy — the classic
        // "my data is stored but every graph is a broken image" trap.
        String graphsEngine = System.getProperty("org.opennms.web.graphs.engine", "backshift");
        if (runningIntegration && !"backshift".equalsIgnoreCase(graphsEngine)) {
            issues.add(issue("warning",
                    "Resource graphs will render broken images (graph engine is '" + graphsEngine + "')",
                    "'org.opennms.web.graphs.engine' is set to '" + graphsEngine + "', which renders graph images "
                            + "from local RRD files. With the integration strategy, samples are stored in your "
                            + "Prometheus backend instead, so those images have no data — even though storage "
                            + "and the measurements API work fine.",
                    "Set 'org.opennms.web.graphs.engine=backshift' (the default) wherever it is overridden under "
                            + homeDir() + "/etc/opennms.properties(.d/) and restart OpenNMS. Backshift renders "
                            + "graphs in the browser via the measurements API, which reads from this plugin."));
        }

        out.put("functional", functional);
        out.put("timeseriesStrategy", runningStrategy);       // what is actually running (drives the gate)
        out.put("runningStrategy", runningStrategy);
        out.put("configuredStrategy", configuredStrategy);    // what's on disk (applies after restart)
        out.put("strategySource", strategySource);
        out.put("integrationStrategyActive", runningIntegration);
        out.put("pendingRestart", pendingRestart);
        out.put("integrationManaged", integrationManaged);    // our dedicated file is present
        out.put("storageServiceActive", tssActive);
        out.put("opennmsHome", homeDir());                    // lets the UI show exact paths + restart commands
        out.put("graphsEngine", graphsEngine);
        out.put("issues", issues);
        return Response.ok(out).build();
    }

    // -------------------------------------------------------------------------
    // Enable / disable the integration strategy (writes a dedicated, reversible file)
    // -------------------------------------------------------------------------

    @POST
    @Path("/integration/enable")
    @Produces(MediaType.APPLICATION_JSON)
    public Response enableIntegration() {
        java.nio.file.Path file = managedFilePath();
        if (file == null) {
            return Response.serverError().entity(result(false, "Could not locate the OpenNMS etc directory.")).build();
        }
        try {
            Files.createDirectories(file.getParent());
            Files.write(file, MANAGED_FILE_CONTENT.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            LOG.warn("Prometheus RemoteWrite plugin wrote {} to enable the integration time-series strategy. "
                    + "An OpenNMS restart is required to apply it.", file);
            return Response.ok(result(true,
                    "Wrote " + file + ". Nothing changes until you " + restartHint() + ".")).build();
        } catch (IOException e) {
            LOG.error("Failed to write {}", file, e);
            return Response.serverError().entity(result(false, "Failed to write " + file + ": " + e.getMessage())).build();
        }
    }

    @POST
    @Path("/integration/disable")
    @Produces(MediaType.APPLICATION_JSON)
    public Response disableIntegration() {
        java.nio.file.Path file = managedFilePath();
        if (file == null) {
            return Response.serverError().entity(result(false, "Could not locate the OpenNMS etc directory.")).build();
        }
        try {
            boolean existed = Files.deleteIfExists(file);
            // After removing our file, is integration STILL configured somewhere else (main opennms.properties, etc.)?
            String stillConfigured = detectTimeseriesStrategy()[0];
            if (INTEGRATION.equalsIgnoreCase(stillConfigured)) {
                return Response.ok(result(true,
                        (existed ? "Removed " + file + ", but " : "")
                        + "'integration' is still configured elsewhere (source: " + detectTimeseriesStrategy()[1]
                        + "). Remove the line 'org.opennms.timeseries.strategy=integration' from that file, then "
                        + restartHint() + ".")).build();
            }
            LOG.warn("Prometheus RemoteWrite plugin removed {} to revert the integration time-series strategy. "
                    + "An OpenNMS restart is required to apply it.", file);
            return Response.ok(result(true, existed
                    ? "Removed " + file + ". Now " + restartHint() + " to revert to the previous strategy."
                    : "No managed file was present; nothing to revert.")).build();
        } catch (IOException e) {
            LOG.error("Failed to remove {}", file, e);
            return Response.serverError().entity(result(false, "Failed to remove " + file + ": " + e.getMessage())).build();
        }
    }

    private static Map<String, Object> result(boolean ok, String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ok", ok);
        m.put("message", message);
        return m;
    }

    private static java.nio.file.Path managedFilePath() {
        String home = System.getProperty("opennms.home", System.getProperty("karaf.base"));
        if (home == null) {
            return null;
        }
        return Paths.get(home, "etc", "opennms.properties.d", MANAGED_FILE_NAME);
    }

    private static boolean managedFileExists() {
        java.nio.file.Path p = managedFilePath();
        return p != null && Files.isRegularFile(p);
    }

    /** The OpenNMS home directory, used to make file paths and restart commands concrete. */
    private static String homeDir() {
        return System.getProperty("opennms.home", System.getProperty("karaf.base", "$OPENNMS_HOME"));
    }

    /** Human-readable, copy-pasteable restart instruction for both common install types. */
    private static String restartHint() {
        return "restart OpenNMS to apply — packaged (systemd) install: 'sudo systemctl restart opennms'; "
                + "manual/tarball install: '" + homeDir() + "/bin/opennms restart'";
    }

    private static Map<String, Object> issue(String severity, String title, String detail, String remedy) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("severity", severity);
        m.put("title", title);
        m.put("detail", detail);
        m.put("remedy", remedy);
        return m;
    }

    /**
     * Resolves the active time-series strategy by reading OpenNMS's properties from disk
     * (it is not exposed as a JVM system property). Returns {@code [strategy, source]}.
     * Defaults to "rrd" when unset — that is OpenNMS's own default.
     */
    private static String[] detectTimeseriesStrategy() {
        String home = System.getProperty("opennms.home", System.getProperty("karaf.base"));
        if (home == null) {
            return new String[]{"rrd", "default (opennms.home unknown)"};
        }
        java.nio.file.Path etc = Paths.get(home, "etc");
        String found = null;
        String source = "default";

        // Files in opennms.properties.d/ override opennms.properties; read base first, then overrides.
        java.nio.file.Path base = etc.resolve("opennms.properties");
        String v = readStrategyFrom(base);
        if (v != null) { found = v; source = "etc/opennms.properties"; }

        java.nio.file.Path d = etc.resolve("opennms.properties.d");
        if (Files.isDirectory(d)) {
            try (DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(d, "*.properties")) {
                for (java.nio.file.Path p : stream) {
                    String pv = readStrategyFrom(p);
                    if (pv != null) { found = pv; source = "etc/opennms.properties.d/" + p.getFileName(); }
                }
            } catch (IOException e) {
                LOG.debug("Could not scan opennms.properties.d", e);
            }
        }
        if (found == null) {
            return new String[]{"rrd", "default (property not set)"};
        }
        return new String[]{found, source};
    }

    /** Returns the uncommented strategy value from a properties file, or null if absent. */
    private static String readStrategyFrom(java.nio.file.Path file) {
        if (file == null || !Files.isRegularFile(file)) {
            return null;
        }
        try (InputStream in = Files.newInputStream(file)) {
            Properties props = new Properties();
            props.load(in);
            String v = props.getProperty(STRATEGY_PROP);
            return v != null && !v.trim().isEmpty() ? v.trim() : null;
        } catch (IOException e) {
            LOG.debug("Could not read {}", file, e);
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Connection test
    // -------------------------------------------------------------------------

    @POST
    @Path("/test-connection")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response testConnection(PrometheusConfigDto dto) {
        String writeUrl, readUrl, orgId;
        try {
            String[] resolved = resolveUrls(dto);
            writeUrl = resolved[0];
            readUrl = resolved[1];
            orgId = resolved[2];
        } catch (IOException e) {
            return Response.serverError().entity("Failed to read config: " + e.getMessage()).build();
        }
        List<TestConnectionResult> results = new ArrayList<>();
        // Write side: reachability only (a real write test is /test-write — it has a side effect).
        results.add(probe("write", writeUrl));
        // Read side: validate it actually answers as a Prometheus query API, not just any web server.
        results.add(probeReadApi(readUrl, orgId));
        return Response.ok(results).build();
    }

    /** Resolves [writeUrl, readUrl, organizationId] from the request body, falling back to saved config. */
    private String[] resolveUrls(PrometheusConfigDto dto) throws IOException {
        String writeUrl = dto != null && dto.getWriteUrl() != null ? dto.getWriteUrl() : null;
        String readUrl = dto != null && dto.getReadUrl() != null ? dto.getReadUrl() : null;
        String orgId = dto != null ? dto.getOrganizationId() : null;
        if (writeUrl == null || readUrl == null || orgId == null) {
            Dictionary<String, Object> props = configAdmin.getConfiguration(PID, null).getProperties();
            if (writeUrl == null) writeUrl = str(props, "writeUrl", DEF_WRITE_URL);
            if (readUrl == null) readUrl = str(props, "readUrl", DEF_READ_URL);
            if (orgId == null) orgId = str(props, "organizationId", "");
        }
        return new String[]{writeUrl, readUrl, orgId};
    }

    private TestConnectionResult probe(String endpoint, String urlStr) {
        long start = System.currentTimeMillis();
        HttpURLConnection conn = null;
        try {
            URL url = new URL(urlStr);
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(PROBE_TIMEOUT_MS);
            conn.setReadTimeout(PROBE_TIMEOUT_MS);
            conn.setRequestMethod("GET");
            conn.setInstanceFollowRedirects(false);
            int code = conn.getResponseCode();
            long dur = System.currentTimeMillis() - start;
            // Any HTTP status proves the host is reachable. The write endpoint
            // typically answers a GET with 405 (it wants POST) — still reachable.
            String detail = "HTTP " + code + " " + safeMessage(conn);
            return new TestConnectionResult(endpoint, urlStr, true, code, dur, detail);
        } catch (Exception e) {
            long dur = System.currentTimeMillis() - start;
            return new TestConnectionResult(endpoint, urlStr, false, 0, dur,
                    e.getClass().getSimpleName() + ": " + e.getMessage());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private static String safeMessage(HttpURLConnection conn) {
        try {
            String m = conn.getResponseMessage();
            return m != null ? m : "";
        } catch (IOException e) {
            return "";
        }
    }

    /**
     * Validates that {@code readUrl} is a working Prometheus-compatible query API by issuing a trivial
     * instant query (<code>query=1</code>) and confirming the response is <code>status:"success"</code>.
     * A plain 200 from some unrelated web server is NOT enough — this catches wrong URLs, auth/tenant
     * problems on the read path, and non-Prometheus endpoints.
     */
    private TestConnectionResult probeReadApi(String readUrl, String orgId) {
        String urlStr = joinPath(readUrl, "query") + "?query=1";
        long start = System.currentTimeMillis();
        HttpURLConnection conn = null;
        try {
            URL url = new URL(urlStr);
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(PROBE_TIMEOUT_MS);
            conn.setReadTimeout(PROBE_TIMEOUT_MS);
            conn.setRequestMethod("GET");
            conn.setInstanceFollowRedirects(false);
            if (orgId != null && !orgId.trim().isEmpty()) {
                conn.setRequestProperty("X-Scope-OrgID", orgId.trim());
            }
            int code = conn.getResponseCode();
            long dur = System.currentTimeMillis() - start;
            String body = readBody(conn, code >= 400);
            boolean looksPrometheus = code == 200 && body != null && body.contains("\"status\"") && body.contains("success");
            String detail;
            boolean ok;
            if (looksPrometheus) {
                ok = true;
                detail = "HTTP " + code + " — valid Prometheus query API (status:success)";
            } else if (code == 200) {
                ok = false;
                detail = "HTTP 200 but the response is not a Prometheus query API result — check the Read URL "
                        + "(expected base ending in /api/v1). Got: " + snippet(body);
            } else {
                ok = false;
                detail = "HTTP " + code + " " + safeMessage(conn) + ": " + snippet(body);
            }
            return new TestConnectionResult("read", urlStr, ok, code, dur, detail);
        } catch (Exception e) {
            long dur = System.currentTimeMillis() - start;
            return new TestConnectionResult("read", urlStr, false, 0, dur,
                    e.getClass().getSimpleName() + ": " + e.getMessage());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * Actually pushes ONE synthetic sample via the Prometheus remote_write protocol and reports the result.
     * This is the only check that proves the backend will accept metrics (it exercises auth, tenant header,
     * timestamp acceptance, ingester readiness, snappy+protobuf encoding — none of which a GET can verify).
     * <p>
     * SIDE EFFECT: writes the series {@code opennms_remotewrite_healthcheck{source="opennms-plugin"} = 1}
     * into the target TSDB at the current time.
     */
    @POST
    @Path("/test-write")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response testWrite(PrometheusConfigDto dto) {
        String writeUrl, orgId;
        try {
            String[] resolved = resolveUrls(dto);
            writeUrl = resolved[0];
            orgId = resolved[2];
        } catch (IOException e) {
            return Response.serverError().entity("Failed to read config: " + e.getMessage()).build();
        }

        long start = System.currentTimeMillis();
        HttpURLConnection conn = null;
        try {
            long now = System.currentTimeMillis();
            // Labels must be sorted by name; "__name__" (0x5F) sorts before "source".
            prometheus.PrometheusTypes.TimeSeries ts = prometheus.PrometheusTypes.TimeSeries.newBuilder()
                    .addLabels(prometheus.PrometheusTypes.Label.newBuilder().setName("__name__").setValue("opennms_remotewrite_healthcheck"))
                    .addLabels(prometheus.PrometheusTypes.Label.newBuilder().setName("source").setValue("opennms-plugin"))
                    .addSamples(prometheus.PrometheusTypes.Sample.newBuilder().setTimestamp(now).setValue(1.0d))
                    .build();
            prometheus.PrometheusRemote.WriteRequest req = prometheus.PrometheusRemote.WriteRequest.newBuilder()
                    .addTimeseries(ts).build();
            byte[] compressed = org.xerial.snappy.Snappy.compress(req.toByteArray());

            URL url = new URL(writeUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(PROBE_TIMEOUT_MS);
            conn.setReadTimeout(PROBE_TIMEOUT_MS);
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setInstanceFollowRedirects(false);
            conn.setRequestProperty("Content-Type", "application/x-protobuf");
            conn.setRequestProperty("Content-Encoding", "snappy");
            conn.setRequestProperty("X-Prometheus-Remote-Write-Version", "0.1.0");
            conn.setRequestProperty("User-Agent", "opennms-prometheus-remotewrite-plugin");
            if (orgId != null && !orgId.trim().isEmpty()) {
                conn.setRequestProperty("X-Scope-OrgID", orgId.trim());
            }
            try (OutputStream os = conn.getOutputStream()) {
                os.write(compressed);
            }
            int code = conn.getResponseCode();
            long dur = System.currentTimeMillis() - start;
            boolean ok = code >= 200 && code < 300;
            String detail = ok
                    ? "HTTP " + code + " — backend accepted a synthetic sample (opennms_remotewrite_healthcheck)"
                    : "HTTP " + code + " " + safeMessage(conn) + ": " + snippet(readBody(conn, true));
            return Response.ok(new TestConnectionResult("write-test", writeUrl, ok, code, dur, detail)).build();
        } catch (Exception e) {
            long dur = System.currentTimeMillis() - start;
            return Response.ok(new TestConnectionResult("write-test", writeUrl, false, 0, dur,
                    e.getClass().getSimpleName() + ": " + e.getMessage())).build();
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /** Reads the response (or error) body, capped, tolerant of failures. */
    private static String readBody(HttpURLConnection conn, boolean errorStream) {
        try (InputStream in = errorStream && conn.getErrorStream() != null ? conn.getErrorStream() : conn.getInputStream()) {
            if (in == null) {
                return "";
            }
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            int total = 0;
            while ((n = in.read(buf)) != -1 && total < 8192) {
                bos.write(buf, 0, n);
                total += n;
            }
            return new String(bos.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "";
        }
    }

    private static String snippet(String s) {
        if (s == null) {
            return "";
        }
        s = s.trim().replaceAll("\\s+", " ");
        return s.length() > 200 ? s.substring(0, 200) + "…" : s;
    }

    // -------------------------------------------------------------------------
    // Runtime statistics (from the Codahale MetricRegistry)
    // -------------------------------------------------------------------------

    @GET
    @Path("/stats")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStats() {
        if (!isTssActive()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("The Prometheus storage service is not active — see the Status tab for details.").build();
        }
        MetricRegistry registry = tss.getMetrics();

        List<Map<String, Object>> gauges = new ArrayList<>();
        for (Map.Entry<String, Gauge> e : registry.getGauges().entrySet()) {
            Map<String, Object> g = new LinkedHashMap<>();
            g.put("name", e.getKey());
            Object value;
            try {
                value = e.getValue().getValue();
            } catch (Exception ex) {
                value = null;
            }
            g.put("value", value);
            gauges.add(g);
        }

        List<Map<String, Object>> meters = new ArrayList<>();
        for (Map.Entry<String, Meter> e : registry.getMeters().entrySet()) {
            Meter m = e.getValue();
            Map<String, Object> mm = new LinkedHashMap<>();
            mm.put("name", e.getKey());
            mm.put("count", m.getCount());
            mm.put("meanRate", m.getMeanRate());
            mm.put("oneMinuteRate", m.getOneMinuteRate());
            mm.put("fiveMinuteRate", m.getFiveMinuteRate());
            mm.put("fifteenMinuteRate", m.getFifteenMinuteRate());
            meters.add(mm);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("gauges", gauges);
        out.put("meters", meters);
        return Response.ok(out).build();
    }

    /** Short-lived cache of the per-node series counts, so search/pagination doesn't re-query Prometheus per keystroke/page. */
    private volatile Map<String, Long> seriesCountCache;
    private volatile long seriesCountCacheAt;
    private static final long SERIES_COUNT_CACHE_MS = 5000;

    private Map<String, Long> cachedSeriesCounts() throws Exception {
        Map<String, Long> cached = seriesCountCache;
        if (cached != null && System.currentTimeMillis() - seriesCountCacheAt < SERIES_COUNT_CACHE_MS) {
            return cached;
        }
        Map<String, Long> fresh = tss.countActiveSeriesBy("node");
        seriesCountCache = fresh;
        seriesCountCacheAt = System.currentTimeMillis();
        return fresh;
    }

    /**
     * How many distinct series are actively receiving data, in total and per node (via the
     * {@code node} meta tag). Backs the "Stored series" section of the Statistics tab — the
     * quickest way to answer "is node X actually getting data written?".
     * <p>
     * Filtering ({@code contains}, case-insensitive) and pagination ({@code offset}/{@code limit})
     * happen server-side so the response stays small with tens of thousands of nodes. Series
     * without a {@code node} tag are reported separately, not as a table row.
     */
    @GET
    @Path("/series-count")
    @Produces(MediaType.APPLICATION_JSON)
    public Response seriesCount(@QueryParam("contains") String contains,
                                @QueryParam("offset") @DefaultValue("0") int offset,
                                @QueryParam("limit") @DefaultValue("25") int limit) {
        if (!isTssActive()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("The Prometheus storage service is not active — see the Status tab for details.").build();
        }
        try {
            Map<String, Long> byNode = cachedSeriesCounts();
            long total = byNode.values().stream().mapToLong(Long::longValue).sum();
            long noTagSeries = byNode.getOrDefault("", 0L);

            String q = contains == null ? "" : contains.trim().toLowerCase();
            List<Map.Entry<String, Long>> nodes = byNode.entrySet().stream()
                    .filter(e -> !e.getKey().isEmpty())
                    .filter(e -> q.isEmpty() || e.getKey().toLowerCase().contains(q))
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                            .thenComparing(Map.Entry.comparingByKey()))
                    .collect(java.util.stream.Collectors.toList());

            int safeLimit = Math.max(1, Math.min(limit, 500));
            int safeOffset = Math.max(0, offset);
            List<Map<String, Object>> page = new ArrayList<>();
            nodes.stream().skip(safeOffset).limit(safeLimit).forEach(e -> {
                Map<String, Object> n = new LinkedHashMap<>();
                n.put("node", e.getKey());
                n.put("series", e.getValue());
                page.add(n);
            });

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("totalSeries", total);
            out.put("nodesWritingData", byNode.size() - (byNode.containsKey("") ? 1 : 0));
            out.put("seriesWithoutNodeTag", noTagSeries);
            out.put("matchingNodes", nodes.size());
            out.put("offset", safeOffset);
            out.put("limit", safeLimit);
            out.put("byNode", page);
            return Response.ok(out).build();
        } catch (Exception e) {
            LOG.warn("Series count failed", e);
            return Response.serverError().entity("Series count failed: " + e.getMessage()).build();
        }
    }

    // -------------------------------------------------------------------------
    // Metric explorer
    // -------------------------------------------------------------------------

    @POST
    @Path("/query")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response query(List<TagMatcherDto> matchers) {
        if (!isTssActive()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("The Prometheus storage service is not active — see the Status tab for details.").build();
        }
        if (matchers == null || matchers.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("At least one tag matcher is required").build();
        }
        List<TagMatcher> tagMatchers = new ArrayList<>();
        for (TagMatcherDto dto : matchers) {
            if (dto.getKey() == null || dto.getKey().trim().isEmpty()) {
                continue;
            }
            ImmutableTagMatcher.TagMatcherBuilder b = ImmutableTagMatcher.builder()
                    .key(dto.getKey().trim())
                    .value(dto.getValue() != null ? dto.getValue() : "");
            TagMatcher.Type type = parseType(dto.getType());
            if (type != null) {
                b.type(type);
            }
            tagMatchers.add(b.build());
        }
        if (tagMatchers.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("At least one tag matcher with a key is required").build();
        }
        try {
            List<Metric> metrics = tss.findMetrics(tagMatchers);
            List<MetricDto> dtos = new ArrayList<>();
            for (Metric m : metrics) {
                dtos.add(MetricDto.from(m));
            }
            return Response.ok(dtos).build();
        } catch (Exception e) {
            LOG.warn("Metric query failed", e);
            return Response.serverError().entity("Query failed: " + e.getMessage()).build();
        }
    }

    /** Tag keys present in the backend (for Metric Explorer autocomplete). {@code name} = metric name. */
    @GET
    @Path("/suggest/keys")
    @Produces(MediaType.APPLICATION_JSON)
    public Response suggestKeys() {
        if (!isTssActive()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("The Prometheus storage service is not active — see the Status tab for details.").build();
        }
        try {
            return Response.ok(tss.getTagKeys()).build();
        } catch (Exception e) {
            LOG.warn("Tag key suggestion failed", e);
            return Response.serverError().entity("Suggestion failed: " + e.getMessage()).build();
        }
    }

    /** Short-lived per-key cache of tag values, so typeahead keystrokes don't re-fetch the full value list from Prometheus. */
    private final java.util.concurrent.ConcurrentHashMap<String, Object[]> tagValuesCache = new java.util.concurrent.ConcurrentHashMap<>();
    private static final long TAG_VALUES_CACHE_MS = 15000;

    @SuppressWarnings("unchecked")
    private List<String> cachedTagValues(String key) throws Exception {
        Object[] entry = tagValuesCache.get(key);
        if (entry != null && System.currentTimeMillis() - (Long) entry[0] < TAG_VALUES_CACHE_MS) {
            return (List<String>) entry[1];
        }
        List<String> values = tss.getTagValues(key);
        tagValuesCache.put(key, new Object[]{System.currentTimeMillis(), values});
        return values;
    }

    /**
     * Stored values for one tag key (for Metric Explorer autocomplete / the node quick filter).
     * Filtered server-side ({@code contains}, case-insensitive) and capped ({@code limit}), so the
     * response stays small even for keys with tens of thousands of values (e.g. {@code node}).
     * Returns {@code {values: [...], totalMatching: n}}.
     */
    @GET
    @Path("/suggest/values")
    @Produces(MediaType.APPLICATION_JSON)
    public Response suggestValues(@QueryParam("key") String key,
                                  @QueryParam("contains") String contains,
                                  @QueryParam("limit") @DefaultValue("50") int limit) {
        if (!isTssActive()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("The Prometheus storage service is not active — see the Status tab for details.").build();
        }
        if (key == null || key.trim().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("Query parameter 'key' is required").build();
        }
        try {
            List<String> all = cachedTagValues(key.trim());
            String q = contains == null ? "" : contains.trim().toLowerCase();
            List<String> matching = q.isEmpty() ? all : all.stream()
                    .filter(v -> v.toLowerCase().contains(q))
                    .collect(java.util.stream.Collectors.toList());
            int cap = Math.max(1, Math.min(limit, 500));
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("values", matching.size() > cap ? matching.subList(0, cap) : matching);
            out.put("totalMatching", matching.size());
            return Response.ok(out).build();
        } catch (Exception e) {
            LOG.warn("Tag value suggestion failed for key {}", key, e);
            return Response.serverError().entity("Suggestion failed: " + e.getMessage()).build();
        }
    }

    private static TagMatcher.Type parseType(String type) {
        if (type == null || type.trim().isEmpty()) {
            return TagMatcher.Type.EQUALS;
        }
        try {
            return TagMatcher.Type.valueOf(type.trim());
        } catch (IllegalArgumentException e) {
            return TagMatcher.Type.EQUALS;
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String str(Dictionary<String, Object> props, String key, String def) {
        if (props == null) {
            return def;
        }
        Object v = props.get(key);
        return v != null ? v.toString() : def;
    }

    private static long lng(Dictionary<String, Object> props, String key, long def) {
        if (props == null) {
            return def;
        }
        Object v = props.get(key);
        if (v == null) {
            return def;
        }
        try {
            return Long.parseLong(v.toString().trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static String joinPath(String base, String suffix) {
        if (base == null) {
            return suffix;
        }
        try {
            // normalise to avoid double slashes
            URI uri = URI.create(base.endsWith("/") ? base + suffix : base + "/" + suffix);
            return uri.toString();
        } catch (Exception e) {
            return base.endsWith("/") ? base + suffix : base + "/" + suffix;
        }
    }
}
