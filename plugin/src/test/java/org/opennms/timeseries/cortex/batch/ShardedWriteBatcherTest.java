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
package org.opennms.timeseries.cortex.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.timeseries.cortex.CortexTSS;

import com.codahale.metrics.MetricRegistry;

import prometheus.PrometheusRemote;
import prometheus.PrometheusTypes;

/**
 * Exercises {@link ShardedWriteBatcher} against a recording in-memory sender: batch assembly (size
 * and linger triggers), per-series coalescing and timestamp sorting, per-series ordering across
 * batches, tenant splitting, retry/drop semantics, and drain-on-destroy.
 */
public class ShardedWriteBatcherTest {

    private static final Instant BASE = Instant.parse("2026-08-24T00:00:00Z").truncatedTo(ChronoUnit.SECONDS);

    private final MetricRegistry registry = new MetricRegistry();
    private final RecordingSender sender = new RecordingSender();
    private ShardedWriteBatcher batcher;

    @After
    public void tearDown() {
        if (batcher != null) {
            batcher.destroy();
            batcher = null;
        }
    }

    private ShardedWriteBatcher.Builder builder() {
        return ShardedWriteBatcher.builder()
                .seriesConverter(CortexTSS::toPrometheusTimeSeries)
                .sender(sender)
                .metricRegistry(registry);
    }

    // ------------------------------------------------------------------
    // Batch assembly
    // ------------------------------------------------------------------

    @Test
    public void flushesWhenTheBatchIsFull() {
        batcher = builder().shardCount(1).maxBatchSamples(5).lingerMs(60_000).build();

        Metric metric = gauge("full_batch");
        for (int i = 0; i < 5; i++) {
            assertTrue(batcher.enqueue(sample(metric, BASE.plusSeconds(i), i), null));
        }

        // The linger is a minute; only the size trigger can flush this.
        SentBatch sent = sender.awaitNext();
        assertEquals(1, sent.request.getTimeseriesCount());
        assertEquals(5, sent.request.getTimeseries(0).getSamplesCount());
        assertEquals(5, registry.meter("samplesWritten").getCount());
    }

    @Test
    public void flushesOnLingerWhenTheBatchIsNotFull() {
        batcher = builder().shardCount(1).maxBatchSamples(1000).lingerMs(100).build();

        Metric metric = gauge("linger_batch");
        for (int i = 0; i < 3; i++) {
            assertTrue(batcher.enqueue(sample(metric, BASE.plusSeconds(i), i), null));
        }

        SentBatch sent = sender.awaitNext();
        assertEquals(3, sent.request.getTimeseries(0).getSamplesCount());
    }

    @Test
    public void coalescesSeriesAndSortsSamplesByTimestamp() {
        batcher = builder().shardCount(1).maxBatchSamples(6).lingerMs(60_000).build();

        Metric a = gauge("series_a");
        Metric b = gauge("series_b");
        // Interleaved series, timestamps deliberately out of order per series.
        batcher.enqueue(sample(a, BASE.plusSeconds(2), 1), null);
        batcher.enqueue(sample(b, BASE.plusSeconds(1), 2), null);
        batcher.enqueue(sample(a, BASE.plusSeconds(0), 3), null);
        batcher.enqueue(sample(b, BASE.plusSeconds(3), 4), null);
        batcher.enqueue(sample(a, BASE.plusSeconds(1), 5), null);
        batcher.enqueue(sample(b, BASE.plusSeconds(2), 6), null);

        SentBatch sent = sender.awaitNext();
        assertEquals(2, sent.request.getTimeseriesCount());
        for (PrometheusTypes.TimeSeries ts : sent.request.getTimeseriesList()) {
            assertEquals(3, ts.getSamplesCount());
            assertAscending(ts);
        }
    }

    @Test
    public void splitsTenantsIntoSeparateRequests() {
        batcher = builder().shardCount(1).maxBatchSamples(1000).lingerMs(100).build();

        Metric metric = gauge("multi_tenant");
        batcher.enqueue(sample(metric, BASE, 1), "org-a");
        batcher.enqueue(sample(metric, BASE, 1), "org-b");

        SentBatch first = sender.awaitNext();
        SentBatch second = sender.awaitNext();
        List<String> orgs = List.of(first.organizationId, second.organizationId);
        assertTrue(orgs.contains("org-a"));
        assertTrue(orgs.contains("org-b"));
    }

    /**
     * Meta tags are not part of {@code Metric.getKey()}, but they are part of the emitted label
     * set, so two samples that differ only in a meta tag are two different wire series. Coalescing
     * them would silently store one sample's value under the other's labels.
     */
    @Test
    public void keepsSamplesWithDifferentMetaTagsOnSeparateSeries() {
        batcher = builder().shardCount(1).maxBatchSamples(2).lingerMs(60_000).build();

        batcher.enqueue(sample(metricWithMtype("meta_change", Metric.Mtype.gauge), BASE, 1.0), null);
        batcher.enqueue(sample(metricWithMtype("meta_change", Metric.Mtype.counter), BASE.plusSeconds(1), 2.0), null);

        SentBatch sent = sender.awaitNext();
        assertEquals("a changed meta tag is a different wire series", 2, sent.request.getTimeseriesCount());
        for (PrometheusTypes.TimeSeries ts : sent.request.getTimeseriesList()) {
            assertEquals(1, ts.getSamplesCount());
            double expected = Metric.Mtype.gauge.name().equals(labelValue(ts, "mtype")) ? 1.0 : 2.0;
            assertEquals("each sample must sit under its own label set",
                    expected, ts.getSamples(0).getValue(), 0.0);
        }
    }

    /**
     * Sanitization is lossy: distinct raw metric keys can emit one and the same label set. Those
     * samples are one wire series and must flow through one shard as one TimeSeries entry, or two
     * shards write the same series in parallel, which the remote-write spec forbids.
     */
    @Test
    public void coalescesSeriesWhoseRawKeysSanitizeIdentically() {
        batcher = builder().shardCount(8).maxBatchSamples(2).lingerMs(60_000).build();

        Metric dotted = ImmutableMetric.builder()
                .intrinsicTag("resourceId", "test/collide")
                .intrinsicTag("name", "collide.a")
                .metaTag("mtype", Metric.Mtype.gauge.name())
                .build();
        Metric slashed = ImmutableMetric.builder()
                .intrinsicTag("resourceId", "test/collide")
                .intrinsicTag("name", "collide/a")
                .metaTag("mtype", Metric.Mtype.gauge.name())
                .build();

        batcher.enqueue(sample(dotted, BASE, 1.0), null);
        batcher.enqueue(sample(slashed, BASE.plusSeconds(1), 2.0), null);

        // Same wire identity, so same shard and same TimeSeries entry: one request, one series,
        // both samples in order. On raw-key hashing the two would land apart and this times out
        // or arrives as duplicate label sets.
        SentBatch sent = sender.awaitNext();
        assertEquals(1, sent.request.getTimeseriesCount());
        assertEquals(2, sent.request.getTimeseries(0).getSamplesCount());
        assertAscending(sent.request.getTimeseries(0));
    }

    /**
     * The inverse guarantee: label values may contain any bytes - sanitization only truncates them
     * - so no flattened key encoding is collision-free. These two label sets flatten identically
     * under the {@code \0}/{@code \1} delimiter scheme the series key once used ({@code {v="x",
     * w="y"}} vs {@code {v="x\0w\1y"}}), but they are different wire series: coalescing them would
     * store one series' samples under the other's labels for as long as the series cache kept the
     * colliding entry alive.
     */
    @Test
    public void keepsSeriesDistinctWhenALabelValueEmbedsAnotherSeriesKey() {
        batcher = builder().shardCount(1).maxBatchSamples(2).lingerMs(60_000).build();

        Metric twoTags = ImmutableMetric.builder()
                .intrinsicTag("resourceId", "test/delimiters")
                .intrinsicTag("name", "delimiter_collision")
                .metaTag("mtype", Metric.Mtype.gauge.name())
                .metaTag("v", "x")
                .metaTag("w", "y")
                .build();
        Metric oneTag = ImmutableMetric.builder()
                .intrinsicTag("resourceId", "test/delimiters")
                .intrinsicTag("name", "delimiter_collision")
                .metaTag("mtype", Metric.Mtype.gauge.name())
                .metaTag("v", "x\0w\1y")
                .build();

        batcher.enqueue(sample(twoTags, BASE, 1.0), null);
        batcher.enqueue(sample(oneTag, BASE.plusSeconds(1), 2.0), null);

        SentBatch sent = sender.awaitNext();
        assertEquals("label sets that flatten identically are still two series",
                2, sent.request.getTimeseriesCount());
        for (PrometheusTypes.TimeSeries ts : sent.request.getTimeseriesList()) {
            assertEquals(1, ts.getSamplesCount());
            double expected = "y".equals(labelValue(ts, "w")) ? 1.0 : 2.0;
            assertEquals("each sample must sit under its own label set",
                    expected, ts.getSamples(0).getValue(), 0.0);
        }
    }

    // ------------------------------------------------------------------
    // Ordering
    // ------------------------------------------------------------------

    @Test
    public void keepsPerSeriesOrderAcrossBatches() throws Exception {
        // Block the first send so a second batch builds up behind it; the shard must not deliver
        // the second batch until the first has been acknowledged.
        sender.blockFirstSend();
        batcher = builder().shardCount(1).maxBatchSamples(1).lingerMs(50).build();

        Metric metric = gauge("ordered_series");
        batcher.enqueue(sample(metric, BASE.plusSeconds(1), 1), null);
        sender.awaitSendEntered();
        batcher.enqueue(sample(metric, BASE.plusSeconds(2), 2), null);
        // Nothing else may arrive while the first send is blocked.
        Thread.sleep(200);
        assertEquals(1, sender.sendsStarted());
        sender.releaseBlockedSend();

        SentBatch first = sender.awaitNext();
        SentBatch second = sender.awaitNext();
        assertEquals(BASE.plusSeconds(1).toEpochMilli(), first.request.getTimeseries(0).getSamples(0).getTimestamp());
        assertEquals(BASE.plusSeconds(2).toEpochMilli(), second.request.getTimeseries(0).getSamples(0).getTimestamp());
    }

    // ------------------------------------------------------------------
    // Failure semantics
    // ------------------------------------------------------------------

    @Test
    public void retriesTheSameBatchOnRetryableFailure() {
        sender.failNextSends(1, new RetryableWriteException("simulated 503"));
        batcher = builder().shardCount(1).maxBatchSamples(2).lingerMs(60_000)
                .maxRetries(3).retryBackoffMs(10).build();

        Metric metric = gauge("retried_series");
        batcher.enqueue(sample(metric, BASE, 1), null);
        batcher.enqueue(sample(metric, BASE.plusSeconds(1), 2), null);

        SentBatch sent = sender.awaitNext();
        assertEquals(2, sent.request.getTimeseries(0).getSamplesCount());
        assertEquals(2, sender.sendsStarted());
        assertEquals(1, registry.meter("batch.retries").getCount());
        assertEquals(2, registry.meter("samplesWritten").getCount());
        assertEquals(0, registry.meter("samplesLost").getCount());
    }

    @Test
    public void dropsTheBatchWhenRetriesAreExhaustedAndMovesOn() {
        sender.failNextSends(10, new RetryableWriteException("simulated persistent 503"));
        batcher = builder().shardCount(1).maxBatchSamples(1).lingerMs(60_000)
                .maxRetries(1).retryBackoffMs(10).build();

        Metric metric = gauge("doomed_series");
        batcher.enqueue(sample(metric, BASE, 1), null);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> registry.meter("samplesLost").getCount() == 1);

        // The shard survives and delivers the next batch once the backend recovers.
        sender.failNextSends(0, null);
        batcher.enqueue(sample(metric, BASE.plusSeconds(1), 2), null);
        SentBatch sent = sender.awaitNext();
        assertEquals(BASE.plusSeconds(1).toEpochMilli(), sent.request.getTimeseries(0).getSamples(0).getTimestamp());
    }

    @Test
    public void dropsTheBatchImmediatelyOnFatalFailure() {
        sender.failNextSends(1, new StorageException("simulated 400"));
        batcher = builder().shardCount(1).maxBatchSamples(1).lingerMs(60_000)
                .maxRetries(5).retryBackoffMs(10_000).build();

        Metric metric = gauge("rejected_series");
        batcher.enqueue(sample(metric, BASE, 1), null);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> registry.meter("samplesLost").getCount() == 1);
        // A fatal failure must not be retried: one send, no retry marks.
        assertEquals(1, sender.sendsStarted());
        assertEquals(0, registry.meter("batch.retries").getCount());
    }

    /**
     * A rejection of the whole request - bad credentials, wrong tenant, wrong endpoint - fails
     * every series in it identically. Bisecting it the way a plain {@link StorageException} is
     * bisected would corner nothing: it would just spend up to one request per series confirming a
     * foregone conclusion while this shard sat idle and its queue filled up behind it. It must be
     * dropped in one step instead.
     */
    @Test
    public void dropsTheWholeBatchInOneStepOnANonIsolableFailure() {
        sender.failNextSends(Integer.MAX_VALUE, new NonIsolableWriteException("simulated 401"));
        batcher = builder().shardCount(1).maxBatchSamples(8).lingerMs(60_000).build();

        for (int i = 0; i < 8; i++) {
            batcher.enqueue(sample(gauge("series_" + i), BASE, i), null);
        }

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> registry.meter("samplesLost").getCount() == 8);
        // A bisecting implementation could spend up to 15 requests cornering 8 individually-poison
        // series (n leaves + n-1 internal nodes); treating this as non-isolable costs exactly one.
        assertEquals(1, sender.sendsStarted());
        assertEquals(0, registry.meter("samplesWritten").getCount());
        assertEquals(0, registry.meter("batch.retries").getCount());
    }

    /**
     * A non-retryable rejection of a coalesced request must not take every series in it down: the
     * batch is resent one series at a time, so only what the backend actually rejects is lost.
     */
    @Test
    public void isolatesAPoisonSeriesInsteadOfDroppingTheWholeBatch() {
        sender.poisonSeriesNamed("poison_series");
        batcher = builder().shardCount(1).maxBatchSamples(4).lingerMs(60_000).build();

        Metric poison = gauge("poison_series");
        Metric healthy = gauge("healthy_series");
        batcher.enqueue(sample(poison, BASE, 1.0), null);
        batcher.enqueue(sample(healthy, BASE, 2.0), null);
        batcher.enqueue(sample(poison, BASE.plusSeconds(1), 3.0), null);
        batcher.enqueue(sample(healthy, BASE.plusSeconds(1), 4.0), null);

        SentBatch sent = sender.awaitNext();
        assertEquals(1, sent.request.getTimeseriesCount());
        assertEquals("healthy_series", labelValue(sent.request.getTimeseries(0), "__name__"));
        assertEquals(2, sent.request.getTimeseries(0).getSamplesCount());
        // coalesced batch + the poison half + the healthy half
        assertEquals(3, sender.sendsStarted());
        assertEquals(2, registry.meter("samplesWritten").getCount());
        assertEquals(2, registry.meter("samplesLost").getCount());
    }

    /**
     * Isolation must be bounded in requests, not just in loss: a fatal rejection bisects the
     * request, so one poison series among n is cornered in O(log n) resends, not one POST per
     * series. With 8 series and the poison first, that is: the batch, then fail(P c1 c2 c3),
     * ok(c4..c7), fail(P c1), ok(c2 c3), fail(P) dropped, ok(c1): 7 sends, not 9.
     */
    @Test
    public void bisectionCornersAPoisonSeriesInLogarithmicRequests() {
        sender.poisonSeriesNamed("poison_series");
        batcher = builder().shardCount(1).maxBatchSamples(8).lingerMs(60_000).build();

        batcher.enqueue(sample(gauge("poison_series"), BASE, 0.0), null);
        for (int i = 1; i < 8; i++) {
            batcher.enqueue(sample(gauge("clean_series_" + i), BASE, i), null);
        }

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> registry.meter("samplesWritten").getCount() == 7);
        assertEquals(1, registry.meter("samplesLost").getCount());
        assertEquals(7, sender.sendsStarted());
        int delivered = 0;
        for (SentBatch sent : sender.all()) {
            for (PrometheusTypes.TimeSeries ts : sent.request.getTimeseriesList()) {
                assertTrue("the poison series must not be delivered",
                        labelValue(ts, "__name__").startsWith("clean_series_"));
                delivered++;
            }
        }
        assertEquals(7, delivered);
    }

    /**
     * One retry budget covers a batch and every resend its isolation spawns. Otherwise a backend
     * mixing fatal and retryable failures hands each of up to batchMaxSamples sub-requests a fresh
     * budget, and one batch can hold its shard for hours of sequential backoffs.
     */
    @Test
    public void sharesOneRetryBudgetAcrossIsolationResends() {
        sender.poisonSeriesNamed("poison_series");
        // Every send that is not poisoned fails retryably: h1 consumes the whole budget (1).
        sender.failNextSends(Integer.MAX_VALUE, new RetryableWriteException("simulated flapping 503"));
        batcher = builder().shardCount(1).maxBatchSamples(3).lingerMs(60_000)
                .maxRetries(1).retryBackoffMs(10).build();

        batcher.enqueue(sample(gauge("healthy_1"), BASE, 1.0), null);
        batcher.enqueue(sample(gauge("healthy_2"), BASE, 2.0), null);
        batcher.enqueue(sample(gauge("poison_series"), BASE, 3.0), null);

        // batch(P: fatal) -> bisect: (h1: retryable, retry, retryable, budget spent, dropped),
        // then (h2 P: fatal) -> bisect: (h2: retryable, budget already spent, dropped immediately),
        // (P: fatal, dropped). Per-resend budgets would have retried h2 a second time.
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> registry.meter("samplesLost").getCount() == 3);
        assertEquals("only h1's single retry may consume the budget",
                1, registry.meter("batch.retries").getCount());
        assertEquals(6, sender.sendsStarted());
        assertEquals(0, registry.meter("samplesWritten").getCount());
    }

    /**
     * Nothing the send path throws may kill the shard thread: a dead shard silently strands every
     * series hashed to it until restart while the other shards look healthy.
     */
    @Test
    public void survivesAnUnexpectedRuntimeFailureInTheSendPath() {
        sender.failNextSendsWithRuntime(1, new IllegalStateException("simulated transport bug"));
        batcher = builder().shardCount(1).maxBatchSamples(1).lingerMs(60_000).build();

        Metric metric = gauge("resilient_series");
        batcher.enqueue(sample(metric, BASE, 1.0), null);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> registry.meter("samplesLost").getCount() == 1);

        // The shard is still alive and delivers the next batch.
        batcher.enqueue(sample(metric, BASE.plusSeconds(1), 2.0), null);
        SentBatch sent = sender.awaitNext();
        assertEquals(BASE.plusSeconds(1).toEpochMilli(), sent.request.getTimeseries(0).getSamples(0).getTimestamp());
        assertEquals(1, registry.meter("samplesWritten").getCount());
    }

    /**
     * The timed enqueue variant propagates interruption instead of counting the sample lost, so a
     * caller enqueueing a whole store() batch can stop at the first interrupt rather than burn
     * every remaining sample against an interrupt flag that fails each offer instantly.
     */
    @Test
    public void propagatesInterruptionFromTheTimedEnqueueWithoutCountingTheSampleLost() throws Exception {
        sender.blockFirstSend();
        batcher = builder().shardCount(1).maxBatchSamples(1).lingerMs(10)
                .shardCapacity(1).enqueueTimeoutMs(50).build();

        Metric metric = gauge("interrupted_series");
        // First sample: flushed immediately and stuck in the blocked sender.
        assertTrue(batcher.enqueue(sample(metric, BASE, 1.0), null));
        sender.awaitSendEntered();
        // Second sample: sits in the shard queue (capacity 1), so the next offer must wait.
        assertTrue(batcher.enqueue(sample(metric, BASE.plusSeconds(1), 2.0), null));

        Thread.currentThread().interrupt();
        try {
            batcher.enqueue(sample(metric, BASE.plusSeconds(2), 3.0), null, 5_000);
            fail("expected the timed enqueue to propagate the interruption");
        } catch (InterruptedException expected) {
            // the throw consumed the interrupt flag
        } finally {
            Thread.interrupted(); // never leak the flag into other tests
        }
        assertEquals("the caller still holds the sample, so it must not be counted lost",
                0, registry.meter("samplesLost").getCount());

        sender.releaseBlockedSend();
    }

    @Test
    public void reportsEnqueueFailureWhenAShardStaysFull() {
        sender.blockFirstSend();
        batcher = builder().shardCount(1).maxBatchSamples(1).lingerMs(10)
                .shardCapacity(1).enqueueTimeoutMs(50).build();

        Metric metric = gauge("saturated_series");
        // First sample: flushed immediately and stuck in the blocked sender.
        assertTrue(batcher.enqueue(sample(metric, BASE, 1), null));
        sender.awaitSendEntered();
        // Second sample: sits in the shard queue (capacity 1).
        assertTrue(batcher.enqueue(sample(metric, BASE.plusSeconds(1), 2), null));
        // Third sample: no room, enqueue times out and the sample counts as lost.
        assertFalse(batcher.enqueue(sample(metric, BASE.plusSeconds(2), 3), null));
        assertEquals(1, registry.meter("samplesLost").getCount());

        sender.releaseBlockedSend();
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @Test
    public void destroyDrainsBufferedSamples() {
        batcher = builder().shardCount(2).maxBatchSamples(1000).lingerMs(60_000).build();

        Metric metric = gauge("drained_series");
        for (int i = 0; i < 10; i++) {
            batcher.enqueue(sample(metric, BASE.plusSeconds(i), i), null);
        }
        batcher.destroy();

        int delivered = 0;
        for (SentBatch sent : sender.all()) {
            for (PrometheusTypes.TimeSeries ts : sent.request.getTimeseriesList()) {
                assertAscending(ts);
                delivered += ts.getSamplesCount();
            }
        }
        assertEquals(10, delivered);
        assertEquals(10, registry.meter("samplesWritten").getCount());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static void assertAscending(final PrometheusTypes.TimeSeries ts) {
        for (int i = 1; i < ts.getSamplesCount(); i++) {
            assertTrue("samples must be in timestamp order per series",
                    ts.getSamples(i - 1).getTimestamp() <= ts.getSamples(i).getTimestamp());
        }
    }

    private static Metric gauge(final String name) {
        return metricWithMtype(name, Metric.Mtype.gauge);
    }

    private static Metric metricWithMtype(final String name, final Metric.Mtype mtype) {
        return ImmutableMetric.builder()
                .intrinsicTag("resourceId", "test/" + name)
                .intrinsicTag("name", name)
                .metaTag("mtype", mtype.name())
                .build();
    }

    private static String labelValue(final PrometheusTypes.TimeSeries ts, final String name) {
        return ts.getLabelsList().stream()
                .filter(l -> l.getName().equals(name))
                .map(PrometheusTypes.Label::getValue)
                .findFirst()
                .orElse(null);
    }

    private static Sample sample(final Metric metric, final Instant time, final double value) {
        return ImmutableSample.builder().metric(metric).time(time).value(value).build();
    }

    private static final class SentBatch {
        private final PrometheusRemote.WriteRequest request;
        private final String organizationId;

        SentBatch(final PrometheusRemote.WriteRequest request, final String organizationId) {
            this.request = request;
            this.organizationId = organizationId;
        }
    }

    /**
     * Records every completed send in arrival order. Can block the first send to expose the
     * single-flight invariant, and fail the next N sends with a given exception.
     */
    private static final class RecordingSender implements RemoteWriteSender {
        private final Queue<SentBatch> sent = new ConcurrentLinkedQueue<>();
        private final AtomicInteger started = new AtomicInteger();
        private final AtomicInteger failuresLeft = new AtomicInteger();
        private final AtomicInteger runtimeFailuresLeft = new AtomicInteger();
        private volatile StorageException failure;
        private volatile RuntimeException runtimeFailure;
        private volatile String poisonMetricName;
        private volatile CountDownLatch blockEntered;
        private volatile CountDownLatch blockRelease;

        @Override
        public void send(final PrometheusRemote.WriteRequest writeRequest, final String organizationId)
                throws StorageException {
            started.incrementAndGet();
            final CountDownLatch entered = blockEntered;
            if (entered != null) {
                blockEntered = null;
                entered.countDown();
                try {
                    if (!blockRelease.await(10, TimeUnit.SECONDS)) {
                        throw new StorageException("test sender was never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StorageException("interrupted in test sender");
                }
            }
            if (runtimeFailuresLeft.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw runtimeFailure;
            }
            final String poison = poisonMetricName;
            if (poison != null && writeRequest.getTimeseriesList().stream().anyMatch(
                    ts -> poison.equals(labelValue(ts, "__name__")))) {
                throw new StorageException("simulated 400 for any request carrying " + poison);
            }
            if (failuresLeft.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw failure;
            }
            sent.add(new SentBatch(writeRequest, organizationId));
        }

        void blockFirstSend() {
            blockRelease = new CountDownLatch(1);
            blockEntered = new CountDownLatch(1);
        }

        void awaitSendEntered() {
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> started.get() > 0);
        }

        void releaseBlockedSend() {
            blockRelease.countDown();
        }

        void failNextSends(final int count, final StorageException e) {
            this.failure = e;
            this.failuresLeft.set(count);
        }

        void failNextSendsWithRuntime(final int count, final RuntimeException e) {
            this.runtimeFailure = e;
            this.runtimeFailuresLeft.set(count);
        }

        /** Every request carrying a series with this {@code __name__} fails like a fatal 4xx. */
        void poisonSeriesNamed(final String metricName) {
            this.poisonMetricName = metricName;
        }

        int sendsStarted() {
            return started.get();
        }

        SentBatch awaitNext() {
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !sent.isEmpty());
            return sent.poll();
        }

        List<SentBatch> all() {
            return new ArrayList<>(sent);
        }
    }
}
