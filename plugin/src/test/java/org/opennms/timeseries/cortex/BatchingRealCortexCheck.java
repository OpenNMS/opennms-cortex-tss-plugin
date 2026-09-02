/*
 * Licensed to The OpenNMS Group, Inc (TOG) under one or more
 * contributor license agreements.  See the LICENSE.md file
 * distributed with this work for additional information
 * regarding copyright ownership.
 *
 * TOG licenses this file to You under the GNU Affero General
 * Public License Version 3 (the "License") or (at your option)
 * any later version.  You may not use this file except in
 * compliance with the License.  You may obtain a copy of the
 * License at:
 *
 *      https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */
package org.opennms.timeseries.cortex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;

/**
 * Manual e2e check for write batching (dcy/sharded-write-batching) against a REAL Cortex backend.
 *
 * <p>NOT part of the default test run - it does not end in Test/IT/TestCase in a way that would
 * make it collide with a default `mvn test`, but it must still be invoked explicitly with
 * {@code -Dtest=BatchingRealCortexCheck}, and only once the lab is up:
 * <pre>
 *   cd plugin/src/test/resources/org/opennms/timeseries/cortex
 *   docker compose up -d
 *   # wait for http://localhost:9009/ready
 *   cd ../../../../../../../..   (back to plugin/)
 *   mvn -Dtest=BatchingRealCortexCheck test
 *   docker compose -f src/test/resources/org/opennms/timeseries/cortex/docker-compose.yaml down -v
 * </pre>
 *
 * <p>Exists because {@link NMS16271_IT} and {@link CortexTSSIntegrationTest} both use the
 * deprecated {@code DockerComposeContainer}, whose helper image negotiates an old Docker API
 * version that this machine's colima-backed daemon rejects (unrelated to the code under test - the
 * same failure occurs against unmodified {@code main}). Bypassing Testcontainers' compose
 * orchestration entirely and driving an already-running stack matches how OpenNMS's own
 * smoke-test harness (~/github/opennms/smoke-test) does it: no DockerComposeContainer there either.
 *
 * <p>Unlike the existing IT tests, this specifically exercises {@code batchingEnabled=true}: many
 * distinct series, written from multiple concurrent threads in small OpenNMS-shaped groups (a
 * couple of samples each, interleaved across series - the exact case the per-sample Entry/Series
 * duplication fix targets), then read back sample by sample: every series must hold exactly its
 * written (timestamp, value) sequence, in order, with zero loss.
 */
public class BatchingRealCortexCheck {

    @Test
    public void batchedWritesRoundTripThroughARealCortexBackend() throws Exception {
        final CortexTSSConfig config = CortexTSSConfig.builder()
                .batchingEnabled(true)
                .batchShards(4)
                .batchMaxSamples(64)
                .batchLingerMs(200)
                .build();
        final CortexTSS storage = new CortexTSS(config, new KVStoreMock());

        final int seriesCount = 20;
        final int samplesPerSeries = 15;
        final Instant referenceTime = Instant.now().with(ChronoField.MICRO_OF_SECOND, 0L).minusSeconds(samplesPerSeries + 5);
        // The lab's Cortex outlives test runs, and the verification below demands each series hold
        // EXACTLY its written samples - so every run must write fresh series, or it would count the
        // samples earlier runs left in the query window too.
        final String runId = Long.toString(referenceTime.toEpochMilli(), 36);

        final Map<Metric, List<Sample>> bySeries = new LinkedHashMap<>();
        for (int s = 0; s < seriesCount; s++) {
            final Metric metric = ImmutableMetric.builder()
                    .intrinsicTag("resourceId", "e2e/node" + s)
                    .intrinsicTag("name", "batching_e2e_" + runId + "_metric_" + s)
                    .metaTag("mtype", Metric.Mtype.gauge.name())
                    .build();
            final List<Sample> series = new ArrayList<>();
            for (int t = 0; t < samplesPerSeries; t++) {
                series.add(ImmutableSample.builder()
                        .metric(metric)
                        .time(referenceTime.plusSeconds(t))
                        .value(s * 1000.0 + t)
                        .build());
            }
            bySeries.put(metric, series);
        }

        try {
            // OpenNMS never guarantees ordering ACROSS store() calls for the same series even with
            // batching enabled (see "Sample ordering" in the README) - that would make this a test
            // of the documented residual race, not of the fix under test. So each series' samples
            // are handed to store() strictly in order, a few at a time, all from one task; the
            // concurrency that matters here - many distinct series resolved through the new Series
            // cache at once - comes from running many series' tasks across the pool concurrently.
            final ExecutorService pool = Executors.newFixedThreadPool(8);
            final int groupSize = 3;
            for (List<Sample> series : bySeries.values()) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < series.size(); i += groupSize) {
                            storage.store(new ArrayList<>(series.subList(i, Math.min(i + groupSize, series.size()))));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            pool.shutdown();
            assertTrue("all store() calls finished", pool.awaitTermination(30, TimeUnit.SECONDS));

            final int totalSamples = seriesCount * samplesPerSeries;
            Awaitility.await("all samples acknowledged by Cortex")
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> assertEquals(totalSamples,
                            storage.getMetrics().meter("samplesWritten").getCount()));
            assertEquals("no sample may be lost against a healthy backend",
                    0, storage.getMetrics().meter("samplesLost").getCount());

            // Verify against Cortex's query API directly rather than through CortexTSS#getTimeseries:
            // the write path is what changed here, and this repo's own CortexTSSIntegrationTest
            // notes Cortex's query_range semantics for raw (unaggregated) data are quirky enough to
            // need a workaround there - query_range returns step-aligned values, not the raw
            // samples. An instant query over a range-vector selector (metric{...}[2m]) has no step
            // to align to: it returns every raw sample in the window, so the assertion can demand
            // the full sequence - every timestamp, every value, in order, nothing missing, nothing
            // extra, for every series. A bug that dropped or corrupted intermediate samples while
            // still landing each series' last one would pass a last-value check but not this.
            final HttpClient http = HttpClient.newHttpClient();
            for (Map.Entry<Metric, List<Sample>> entry : bySeries.entrySet()) {
                final Metric metric = entry.getKey();
                final List<Sample> expected = entry.getValue();
                final String metricName = metric.getIntrinsicTags().stream()
                        .filter(t -> "name".equals(t.getKey())).findFirst().orElseThrow().getValue();
                final String resourceId = metric.getIntrinsicTags().stream()
                        .filter(t -> "resourceId".equals(t.getKey())).findFirst().orElseThrow().getValue();
                final String query = String.format("%s{resourceId=\"%s\"}[2m]", metricName, resourceId);
                final URI uri = URI.create("http://localhost:9009/prometheus/api/v1/query?query="
                        + URLEncoder.encode(query, StandardCharsets.UTF_8));

                Awaitility.await("series " + metricName + " to report all " + expected.size() + " samples")
                        .atMost(Duration.ofSeconds(30))
                        .pollInterval(Duration.ofMillis(200))
                        .until(() -> querySamples(http, uri).size() >= expected.size());

                final List<double[]> actual = querySamples(http, uri);
                assertEquals("series " + metricName + " must hold exactly its written samples",
                        expected.size(), actual.size());
                for (int i = 0; i < expected.size(); i++) {
                    assertEquals("timestamp of sample " + i + " of " + metricName,
                            expected.get(i).getTime().toEpochMilli() / 1000.0, actual.get(i)[0], 0.0005);
                    assertEquals("value of sample " + i + " of " + metricName,
                            expected.get(i).getValue(), actual.get(i)[1], 0.0001);
                }
            }
        } finally {
            storage.destroy();
        }
    }

    /**
     * The systemic-rejection bound, against the real backend: a batch of samples that are ALL
     * out-of-order draws genuine per-series-worded 400s from Cortex, but the effect is
     * request-wide - every bisection leaf would fail. The cap must conclude systemic, drop the
     * batch in a bounded number of requests, count every sample lost exactly once, and leave the
     * shard alive for the next healthy batch.
     */
    @Test
    public void systemicRejectionFromARealBackendIsBoundedAndSurvivable() throws Exception {
        final CortexTSSConfig config = CortexTSSConfig.builder()
                .batchingEnabled(true)
                .batchShards(1)
                .batchMaxSamples(64)
                .batchLingerMs(200)
                .build();
        final CortexTSS storage = new CortexTSS(config, new KVStoreMock());

        final int seriesCount = 24;
        final Instant baseline = Instant.now().with(ChronoField.MICRO_OF_SECOND, 0L).minusSeconds(10);
        final String runId = Long.toString(baseline.toEpochMilli(), 36);
        final List<Metric> metrics = new ArrayList<>();
        for (int i = 0; i < seriesCount; i++) {
            metrics.add(ImmutableMetric.builder()
                    .intrinsicTag("resourceId", "e2e/systemic" + i)
                    .intrinsicTag("name", "systemic_e2e_" + runId + "_metric_" + i)
                    .metaTag("mtype", Metric.Mtype.gauge.name())
                    .build());
        }

        try {
            // Phase 1: a healthy baseline sample per series, so the backend has a newer timestamp
            // on record for every one of them.
            final List<Sample> fresh = new ArrayList<>();
            for (Metric m : metrics) {
                fresh.add(ImmutableSample.builder().metric(m).time(baseline).value(1.0).build());
            }
            storage.store(fresh);
            Awaitility.await("baseline accepted").atMost(Duration.ofSeconds(30))
                    .until(() -> storage.getMetrics().meter("samplesWritten").getCount() == seriesCount);

            // Phase 2: one OLDER sample per series in one batch. Cortex rejects each as
            // out-of-order (400); since every series in the request is in that state, every
            // bisected half fails identically - the systemic conclusion must fire and account
            // for every sample exactly once, quickly, instead of one request per series.
            final List<Sample> stale = new ArrayList<>();
            for (Metric m : metrics) {
                stale.add(ImmutableSample.builder().metric(m).time(baseline.minusSeconds(300)).value(2.0).build());
            }
            storage.store(stale);
            Awaitility.await("systemic rejection fully accounted").atMost(Duration.ofSeconds(30))
                    .until(() -> storage.getMetrics().meter("samplesLost").getCount() == seriesCount);

            // Phase 3: the shard survives - a healthy batch flows immediately after.
            final List<Sample> recovery = new ArrayList<>();
            for (Metric m : metrics) {
                recovery.add(ImmutableSample.builder().metric(m).time(baseline.plusSeconds(5)).value(3.0).build());
            }
            storage.store(recovery);
            Awaitility.await("recovery batch accepted").atMost(Duration.ofSeconds(30))
                    .until(() -> storage.getMetrics().meter("samplesWritten").getCount() == 2L * seriesCount);
        } finally {
            storage.destroy();
        }
    }

    /**
     * Every raw (timestamp-in-seconds, value) pair the backend holds for the selector, in
     * timestamp order; empty while the series has not appeared yet. Fails the test outright if the
     * selector matches more than one stored series: one label set fanning out into several would
     * mean the write path corrupted labels.
     */
    private static List<double[]> querySamples(final HttpClient http, final URI uri) throws Exception {
        final HttpResponse<String> response = http.send(HttpRequest.newBuilder(uri).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        final JSONArray result = new JSONObject(response.body()).getJSONObject("data").getJSONArray("result");
        if (result.isEmpty()) {
            return List.of();
        }
        assertEquals("one label set must land as exactly one stored series", 1, result.length());
        final JSONArray values = result.getJSONObject(0).getJSONArray("values");
        final List<double[]> samples = new ArrayList<>(values.length());
        for (int i = 0; i < values.length(); i++) {
            final JSONArray pair = values.getJSONArray(i);
            samples.add(new double[]{pair.getDouble(0), pair.getDouble(1)});
        }
        return samples;
    }
}
